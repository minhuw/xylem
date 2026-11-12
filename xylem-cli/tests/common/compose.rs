//! Docker compose fixture shared by the service guards
//!
//! Every Docker-based test stack is started, awaited, and torn down here. Each
//! stack runs as its own compose project under a fresh random name, so
//! concurrent tests never share a server. No stack publishes a host port:
//! tests reach each service at its container's address on the project
//! network, so no `docker-proxy` relays their traffic. An override file pins
//! every container to the CPUs the test process may run on, so a test run
//! under `taskset` keeps its servers on the same CPUs. A guard names its
//! compose file and a readiness probe; this module runs `up -d`, looks up the
//! container addresses, polls the probe, and runs `down -v` on drop for the
//! project it created.

use std::io::Read;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

/// Compose command line
const COMPOSE: &[&str] = &["docker", "compose"];

/// Readiness probes after `up -d` before giving up (30 seconds at `PROBE_INTERVAL`)
const PROBE_ATTEMPTS: u32 = 60;

/// Pause between readiness probes
const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Description of one Docker compose test stack
pub struct ComposeSpec {
    /// Service name used in error messages
    pub name: &'static str,
    /// Compose file path relative to the workspace root
    pub compose_file: &'static str,
    /// Run `build` before `up -d` so the image is rebuilt from its Dockerfile
    pub build_images: bool,
    /// Returns true once every service in the stack answers requests
    pub is_ready: fn(&ComposeStack) -> bool,
}

/// Docker operations the fixture needs, so the lifecycle can be tested offline
trait FixtureRuntime: Send + Sync {
    fn preflight(&self) -> Result<(), Box<dyn std::error::Error>>;
    /// Whether `project` already owns resources of `kind` (container, network, volume)
    fn occupied(&self, kind: &str, project: &str) -> Result<bool, String>;
    /// Names of the services the compose files define
    fn services(
        &self,
        files: &[PathBuf],
        project: &str,
        env: &[(String, String)],
    ) -> Result<Vec<String>, String>;
    fn compose(
        &self,
        files: &[PathBuf],
        project: &str,
        env: &[(String, String)],
        args: &[&str],
    ) -> std::io::Result<bool>;
    /// IPv4 address of `service`'s container on the network `network`
    fn ip(
        &self,
        files: &[PathBuf],
        project: &str,
        env: &[(String, String)],
        service: &str,
        network: &str,
    ) -> Result<Ipv4Addr, String>;
    fn report(&self, message: &str);
}

/// A ready Docker compose project, torn down on drop
pub struct ComposeStack {
    /// The fixture's compose file, then the override that pins its containers
    files: Vec<PathBuf>,
    project: String,
    /// Variables the compose file interpolates
    env: Vec<(String, String)>,
    /// Container address of each service on the project network
    ips: Vec<(String, Ipv4Addr)>,
    runtime: Box<dyn FixtureRuntime>,
    /// Holds the override file, removed after `down`
    _override_dir: tempfile::TempDir,
    /// Set once `up` may have created resources that `down` must remove
    cleanup_armed: bool,
}

impl ComposeStack {
    /// Start the stack described by `spec` in a fresh project and wait for it to be ready
    ///
    /// # Errors
    ///
    /// Returns error if the compose file does not exist, Docker or docker
    /// compose is not available, the fresh project name is already in use,
    /// the images fail to build, the stack fails to start, a container has
    /// no address on the project network, or the stack does not become
    /// ready within 30 seconds.
    pub fn start(spec: &ComposeSpec) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_env(spec, Vec::new())
    }

    /// [`Self::start`] with variables for the compose file to interpolate,
    /// such as a host directory to mount
    pub fn start_with_env(
        spec: &ComposeSpec,
        env: Vec<(String, String)>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().ok_or("No parent directory")?;
        let compose_file = root.join(spec.compose_file);
        if !compose_file.exists() {
            return Err(format!("compose file not found at {:?}", compose_file).into());
        }
        Self::start_with(
            spec,
            compose_file,
            new_project()?,
            env,
            &allowed_cpus()?,
            Box::new(DockerRuntime),
            PROBE_ATTEMPTS,
            PROBE_INTERVAL,
        )
    }

    // The same lifecycle runs against Docker and against the offline fake.
    #[allow(clippy::too_many_arguments)]
    fn start_with(
        spec: &ComposeSpec,
        compose_file: PathBuf,
        project: String,
        env: Vec<(String, String)>,
        cpus: &str,
        runtime: Box<dyn FixtureRuntime>,
        attempts: u32,
        interval: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        runtime.preflight()?;
        ensure_vacant(&*runtime, &project)?;
        let services = runtime.services(std::slice::from_ref(&compose_file), &project, &env)?;
        let override_dir = tempfile::tempdir()?;
        let override_file = override_dir.path().join("cpuset.yml");
        std::fs::write(&override_file, cpuset_override(&services, cpus))?;
        let mut stack = Self {
            files: vec![compose_file, override_file],
            project,
            env,
            ips: Vec::new(),
            runtime,
            _override_dir: override_dir,
            cleanup_armed: false,
        };
        if spec.build_images && !stack.run(&["build"])? {
            return Err(format!("Failed to build {} Docker image", spec.name).into());
        }
        // `up` can create resources even when it fails.
        stack.cleanup_armed = true;
        if !stack.run(&["up", "-d"])? {
            return Err(format!("Failed to start {} with docker compose", spec.name).into());
        }
        // Compose names the network it creates for the project `<project>_default`
        let network = format!("{}_default", stack.project);
        for service in services {
            let ip = stack
                .runtime
                .ip(&stack.files, &stack.project, &stack.env, &service, &network)
                .map_err(|e| format!("{}: {e}", spec.name))?;
            stack.ips.push((service, ip));
        }
        for _ in 0..attempts {
            if (spec.is_ready)(&stack) {
                return Ok(stack);
            }
            thread::sleep(interval);
        }
        Err(format!("{} failed to become ready", spec.name).into())
    }

    fn run(&self, args: &[&str]) -> std::io::Result<bool> {
        self.runtime.compose(&self.files, &self.project, &self.env, args)
    }

    /// The value of the compose file variable `name`, if the stack sets it
    pub fn env(&self, name: &str) -> Option<&str> {
        self.env.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }

    /// `service`'s container `port` at the container's own address on the
    /// project network, which the host reaches without Docker's port
    /// forwarding
    ///
    /// Returns `None` if the compose file defines no service `service`.
    pub fn container_addr(&self, service: &str, port: u16) -> Option<SocketAddr> {
        let (_, ip) = self.ips.iter().find(|(name, _)| name == service)?;
        Some(SocketAddrV4::new(*ip, port).into())
    }
}

impl Drop for ComposeStack {
    fn drop(&mut self) {
        if self.cleanup_armed {
            if let Some(message) =
                cleanup_message(&self.project, "compose teardown", self.run(&["down", "-v"]))
            {
                self.runtime.report(&message);
            }
        }
    }
}

fn cleanup_message(project: &str, action: &str, result: std::io::Result<bool>) -> Option<String> {
    match result {
        Ok(true) => None,
        Ok(false) => Some(format!(
            "ERROR: {action} failed for Compose project {project}; resources may remain"
        )),
        Err(error) => Some(format!(
            "ERROR: {action} could not run for Compose project {project}: {error}; resources may remain"
        )),
    }
}

/// Refuse a project name that already owns resources, which `down -v` would remove
fn ensure_vacant(runtime: &dyn FixtureRuntime, project: &str) -> Result<(), String> {
    for kind in ["container", "network", "volume"] {
        if runtime.occupied(kind, project)? {
            return Err(format!("Compose project {project} already has {kind} resources"));
        }
    }
    Ok(())
}

/// Compose override that gives every service in `services` the cpuset `cpus`
fn cpuset_override(services: &[String], cpus: &str) -> String {
    let mut yaml = String::from("services:\n");
    for service in services {
        yaml += &format!("  \"{service}\":\n    cpuset: \"{cpus}\"\n");
    }
    yaml
}

/// The CPUs this process may run on, as Linux lists them (`1,3,5` or `0-63`)
fn allowed_cpus() -> Result<String, String> {
    let status = std::fs::read_to_string("/proc/self/status")
        .map_err(|e| format!("could not read the allowed CPUs: {e}"))?;
    parse_allowed_cpus(&status).ok_or_else(|| "no Cpus_allowed_list in /proc/self/status".into())
}

fn parse_allowed_cpus(status: &str) -> Option<String> {
    let list = status.lines().find_map(|line| line.strip_prefix("Cpus_allowed_list:"))?.trim();
    (!list.is_empty() && list.bytes().all(|c| c.is_ascii_digit() || c == b',' || c == b'-'))
        .then(|| list.to_string())
}

/// The local Docker daemon, reached through the user's own Docker configuration
struct DockerRuntime;

impl FixtureRuntime for DockerRuntime {
    fn preflight(&self) -> Result<(), Box<dyn std::error::Error>> {
        if !Command::new("docker").arg("--version").output()?.status.success() {
            return Err("Docker is not available".into());
        }
        if !Command::new(COMPOSE[0])
            .args(&COMPOSE[1..])
            .arg("version")
            .output()
            .is_ok_and(|o| o.status.success())
        {
            return Err("docker compose not available".into());
        }
        Ok(())
    }

    fn occupied(&self, kind: &str, project: &str) -> Result<bool, String> {
        let mut command = Command::new("docker");
        command.args([kind, "ls"]);
        if kind == "container" {
            command.arg("--all");
        }
        let output = command
            .args(["--filter", &format!("label=com.docker.compose.project={project}")])
            .args([
                "--format",
                if kind == "volume" {
                    "{{.Name}}"
                } else {
                    "{{.ID}}"
                },
            ])
            .output()
            .map_err(|e| format!("could not check {kind} ownership: {e}"))?;
        if !output.status.success() {
            return Err(format!("could not check {kind} ownership"));
        }
        Ok(!output.stdout.is_empty())
    }

    fn services(
        &self,
        files: &[PathBuf],
        project: &str,
        env: &[(String, String)],
    ) -> Result<Vec<String>, String> {
        let output = compose_command(COMPOSE, files, project, &["config", "--services"])
            .envs(env.iter().map(|(name, value)| (name, value)))
            .output()
            .map_err(|e| format!("could not list the compose services: {e}"))?;
        if !output.status.success() {
            return Err(format!(
                "could not list the compose services: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let services: Vec<String> =
            String::from_utf8_lossy(&output.stdout).lines().map(str::to_string).collect();
        if services.is_empty() {
            return Err("the compose file defines no services".into());
        }
        Ok(services)
    }

    fn compose(
        &self,
        files: &[PathBuf],
        project: &str,
        env: &[(String, String)],
        args: &[&str],
    ) -> std::io::Result<bool> {
        compose_command(COMPOSE, files, project, args)
            .envs(env.iter().map(|(name, value)| (name, value)))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
    }

    fn ip(
        &self,
        files: &[PathBuf],
        project: &str,
        env: &[(String, String)],
        service: &str,
        network: &str,
    ) -> Result<Ipv4Addr, String> {
        let output = compose_command(COMPOSE, files, project, &["ps", "--quiet", service])
            .envs(env.iter().map(|(name, value)| (name, value)))
            .stderr(Stdio::null())
            .output()
            .map_err(|e| format!("could not find the {service} container: {e}"))?;
        let containers = String::from_utf8_lossy(&output.stdout);
        let [container] = containers.split_whitespace().collect::<Vec<_>>()[..] else {
            return Err(format!("{service} does not run in exactly one container: {containers:?}"));
        };
        let output = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{range $name, $net := .NetworkSettings.Networks}}{{println $name $net.IPAddress}}{{end}}",
                container,
            ])
            .stderr(Stdio::null())
            .output()
            .map_err(|e| format!("could not inspect the {service} container: {e}"))?;
        if !output.status.success() {
            return Err(format!("could not inspect the {service} container"));
        }
        parse_network_ip(&String::from_utf8_lossy(&output.stdout), network)
            .ok_or_else(|| format!("{service} has no IPv4 address on network {network}"))
    }

    fn report(&self, message: &str) {
        eprintln!("{message}");
    }
}

/// Fresh compose project name, `xylemtest` and 32 random hex digits
fn new_project() -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let suffix: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("xylemtest{suffix}"))
}

/// `<cmd> -p <project> -f <file>... <args>`
fn compose_command(cmd: &[&str], files: &[PathBuf], project: &str, args: &[&str]) -> Command {
    let mut command = Command::new(cmd[0]);
    command.args(&cmd[1..]).arg("-p").arg(project);
    for file in files {
        command.arg("-f").arg(file);
    }
    command.args(args);
    command
}

/// The IPv4 address on `network` from `<network> <address>` lines, one per
/// network of the container
fn parse_network_ip(output: &str, network: &str) -> Option<Ipv4Addr> {
    // Reject a missing, empty or malformed address rather than guessing.
    let mut addresses = output.lines().filter_map(|line| {
        let (name, address) = line.trim().split_once(' ')?;
        (name == network).then_some(address)
    });
    let address = addresses.next()?.parse().ok()?;
    addresses.next().is_none().then_some(address)
}

/// Whether the server at `address` answers `request` with a reply
/// containing `expected` within its first 1024 bytes
pub fn answers_at(address: SocketAddr, request: &[u8], expected: &[u8]) -> bool {
    use std::io::Write;
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_secs(1)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));
    if stream.write_all(request).is_err() {
        return false;
    }
    let mut buf = [0; 1024];
    let mut used = 0;
    while used < buf.len() {
        match stream.read(&mut buf[used..]) {
            Ok(0) | Err(_) => return false,
            Ok(n) => {
                used += n;
                if buf[..used].windows(expected.len()).any(|part| part == expected) {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_only_one_address_on_the_project_network() {
        let output = "bridge 172.17.0.5\nxylemtest1_default 172.18.0.2\n";
        assert_eq!(
            parse_network_ip(output, "xylemtest1_default"),
            Some(Ipv4Addr::new(172, 18, 0, 2))
        );
        for invalid in [
            "xylemtest1_default \n",
            "xylemtest1_default\n",
            "xylemtest1_default garbage\n",
            "xylemtest1_default fe80::1\n",
            "xylemtest1_default 172.18.0.2\nxylemtest1_default 172.18.0.3\n",
            "xylemtest10_default 172.18.0.2\n",
            "",
        ] {
            assert_eq!(parse_network_ip(invalid, "xylemtest1_default"), None, "{invalid:?}");
        }
    }
    #[test]
    fn parses_the_allowed_cpu_list() {
        let status = "Name:\txylem\nCpus_allowed:\taaaa\nCpus_allowed_list:\t1,3,5,7-9\n";
        assert_eq!(parse_allowed_cpus(status).as_deref(), Some("1,3,5,7-9"));
        assert_eq!(parse_allowed_cpus("Cpus_allowed_list:\t\n"), None);
        assert_eq!(parse_allowed_cpus("Name:\txylem\n"), None);
        assert!(allowed_cpus().is_ok());
    }
    #[test]
    fn override_pins_every_service() {
        assert_eq!(
            cpuset_override(&["redis".into(), "redis-acl".into()], "1,3"),
            "services:\n  \"redis\":\n    cpuset: \"1,3\"\n  \"redis-acl\":\n    cpuset: \"1,3\"\n"
        );
    }
    #[test]
    fn every_operation_has_own_project() {
        let files = [PathBuf::from("fixture.yml"), PathBuf::from("cpuset.yml")];
        for args in [
            &["config", "--services"][..],
            &["build"],
            &["up", "-d"],
            &["ps", "--quiet", "redis"],
            &["down", "-v"],
        ] {
            let command = compose_command(&["docker", "compose"], &files, "xylemtest123", args);
            let actual: Vec<_> =
                command.get_args().map(|s| s.to_string_lossy().into_owned()).collect();
            assert_eq!(
                &actual[..7],
                ["compose", "-p", "xylemtest123", "-f", "fixture.yml", "-f", "cpuset.yml"]
            );
            assert_eq!(&actual[7..], args);
        }
    }
    #[test]
    fn project_names_are_fresh_and_compose_safe() {
        let a = new_project().unwrap();
        let b = new_project().unwrap();
        assert_ne!(a, b);
        assert!(
            a.starts_with("xylemtest")
                && a.bytes().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct FakeState {
        events: Vec<String>,
        lookups: usize,
        collision_at: Option<usize>,
        failure_at: Option<usize>,
        up_fails: bool,
        ip_fails: bool,
        down_fails: bool,
        down_unexecuted: bool,
        /// The override file as `up` saw it
        override_yaml: Option<String>,
    }
    struct FakeRuntime(Arc<Mutex<FakeState>>);
    impl FixtureRuntime for FakeRuntime {
        fn preflight(&self) -> Result<(), Box<dyn std::error::Error>> {
            self.0.lock().unwrap().events.push("preflight".into());
            Ok(())
        }
        fn occupied(&self, kind: &str, project: &str) -> Result<bool, String> {
            let mut s = self.0.lock().unwrap();
            s.lookups += 1;
            let n = s.lookups;
            s.events.push(format!("lookup {project} {kind}"));
            if s.failure_at == Some(n) {
                return Err("lookup failed".into());
            }
            if s.collision_at == Some(n) {
                return Ok(true);
            }
            Ok(false)
        }
        fn services(
            &self,
            files: &[PathBuf],
            project: &str,
            _: &[(String, String)],
        ) -> Result<Vec<String>, String> {
            assert_eq!(files, [PathBuf::from("fixture.yml")]);
            self.0.lock().unwrap().events.push(format!("services {project}"));
            Ok(vec!["redis".into(), "nginx".into()])
        }
        fn compose(
            &self,
            files: &[PathBuf],
            project: &str,
            _: &[(String, String)],
            args: &[&str],
        ) -> std::io::Result<bool> {
            let mut s = self.0.lock().unwrap();
            assert_eq!(files.len(), 2);
            assert_eq!(files[0], PathBuf::from("fixture.yml"));
            s.events.push(format!("compose {project} {}", args.join(" ")));
            match args[0] {
                "build" => Ok(true),
                "up" => {
                    s.override_yaml = Some(std::fs::read_to_string(&files[1]).unwrap());
                    Ok(!s.up_fails)
                }
                "down" if s.down_unexecuted => Err(std::io::Error::other("fake execution failure")),
                "down" => Ok(!s.down_fails),
                _ => panic!("unexpected command"),
            }
        }
        fn ip(
            &self,
            _: &[PathBuf],
            project: &str,
            _: &[(String, String)],
            service: &str,
            network: &str,
        ) -> Result<Ipv4Addr, String> {
            let mut s = self.0.lock().unwrap();
            s.events.push(format!("ip {project} {service} {network}"));
            if s.ip_fails {
                return Err("no address".into());
            }
            Ok(Ipv4Addr::new(10, 0, 0, if service == "redis" { 2 } else { 3 }))
        }
        fn report(&self, message: &str) {
            self.0.lock().unwrap().events.push(format!("report {message}"));
        }
    }
    fn test_start(
        state: Arc<Mutex<FakeState>>,
        build: bool,
        ready: fn(&ComposeStack) -> bool,
    ) -> Result<ComposeStack, Box<dyn std::error::Error>> {
        let spec = ComposeSpec {
            name: "fake",
            compose_file: "unused",
            build_images: build,
            is_ready: ready,
        };
        ComposeStack::start_with(
            &spec,
            PathBuf::from("fixture.yml"),
            "xylemtestfresh".into(),
            Vec::new(),
            "1,3",
            Box::new(FakeRuntime(state)),
            2,
            Duration::ZERO,
        )
    }
    fn ready(_: &ComposeStack) -> bool {
        true
    }
    fn not_ready(_: &ComposeStack) -> bool {
        false
    }
    fn panic_ready(_: &ComposeStack) -> bool {
        panic!("readiness panic")
    }
    fn commands(state: &FakeState) -> Vec<String> {
        state.events.iter().filter(|s| s.starts_with("compose ")).cloned().collect()
    }

    #[test]
    fn lifecycle_ready_stack_pins_and_addresses_every_service() {
        let state = Arc::new(Mutex::new(FakeState::default()));
        let stack = test_start(state.clone(), true, |stack| {
            stack.container_addr("redis", 6379) == Some("10.0.0.2:6379".parse().unwrap())
                && stack.container_addr("nginx", 80) == Some("10.0.0.3:80".parse().unwrap())
                && stack.container_addr("memcached", 11211).is_none()
        })
        .unwrap();
        drop(stack);
        let state = state.lock().unwrap();
        assert_eq!(
            state.override_yaml.as_deref(),
            Some(cpuset_override(&["redis".into(), "nginx".into()], "1,3").as_str())
        );
        let steps: Vec<_> = state
            .events
            .iter()
            .filter(|e| !e.starts_with("lookup ") && *e != "preflight")
            .cloned()
            .collect();
        assert_eq!(
            steps,
            [
                "services xylemtestfresh",
                "compose xylemtestfresh build",
                "compose xylemtestfresh up -d",
                "ip xylemtestfresh redis xylemtestfresh_default",
                "ip xylemtestfresh nginx xylemtestfresh_default",
                "compose xylemtestfresh down -v",
            ]
        );
    }
    #[test]
    fn lifecycle_collision_or_lookup_failure_never_starts_or_cleans() {
        for n in 1..=3 {
            for fail in [false, true] {
                let state = Arc::new(Mutex::new(FakeState {
                    collision_at: (!fail).then_some(n),
                    failure_at: fail.then_some(n),
                    ..Default::default()
                }));
                assert!(test_start(state.clone(), true, ready).is_err());
                assert!(commands(&state.lock().unwrap()).is_empty());
                assert!(!state.lock().unwrap().events.iter().any(|e| e.starts_with("report ")));
            }
        }
    }
    #[test]
    fn lifecycle_failed_up_address_and_readiness_cleanup_only_owned_project() {
        for (up_fails, ip_fails) in [(false, false), (true, false), (false, true)] {
            let state =
                Arc::new(Mutex::new(FakeState { up_fails, ip_fails, ..Default::default() }));
            assert!(test_start(state.clone(), false, not_ready).is_err());
            assert_eq!(
                commands(&state.lock().unwrap()),
                ["compose xylemtestfresh up -d", "compose xylemtestfresh down -v"]
            );
        }
    }
    #[test]
    fn lifecycle_teardown_failure_is_reported_during_unwind() {
        for unexecuted in [false, true] {
            let state = Arc::new(Mutex::new(FakeState {
                down_fails: true,
                down_unexecuted: unexecuted,
                ..Default::default()
            }));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = test_start(state.clone(), false, panic_ready);
            }));
            assert!(result.is_err());
            let state = state.lock().unwrap();
            assert_eq!(
                commands(&state),
                ["compose xylemtestfresh up -d", "compose xylemtestfresh down -v"]
            );
            let expected = if unexecuted {
                "compose teardown could not run"
            } else {
                "compose teardown failed"
            };
            assert!(state
                .events
                .iter()
                .any(|e| e.contains(expected) && e.contains("xylemtestfresh")));
        }
    }
}
