//! Docker compose fixture shared by the service guards
//!
//! Every Docker-based test stack is started, awaited, and torn down here. Each
//! stack runs as its own compose project under a fresh random name and
//! publishes its ports on host ports Docker picks, so concurrent tests never
//! share a server or clash on a port. A guard names its compose file and a
//! readiness probe; this module runs `up -d`, polls the probe, and runs
//! `down -v` on drop for the project it created.

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
    fn compose(
        &self,
        file: &Path,
        project: &str,
        env: &[(String, String)],
        args: &[&str],
    ) -> std::io::Result<bool>;
    fn port(
        &self,
        file: &Path,
        project: &str,
        env: &[(String, String)],
        service: &str,
        port: u16,
        protocol: &str,
    ) -> Option<u16>;
    fn report(&self, message: &str);
}

/// A ready Docker compose project, torn down on drop
pub struct ComposeStack {
    compose_file: PathBuf,
    project: String,
    /// Variables the compose file interpolates
    env: Vec<(String, String)>,
    runtime: Box<dyn FixtureRuntime>,
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
    /// the images fail to build, the stack fails to start, or it does not
    /// become ready within 30 seconds.
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
            Box::new(DockerRuntime),
            PROBE_ATTEMPTS,
            PROBE_INTERVAL,
        )
    }

    // The same lifecycle runs against Docker and against the offline fake.
    fn start_with(
        spec: &ComposeSpec,
        compose_file: PathBuf,
        project: String,
        env: Vec<(String, String)>,
        runtime: Box<dyn FixtureRuntime>,
        attempts: u32,
        interval: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        runtime.preflight()?;
        let mut stack = Self {
            compose_file,
            project,
            env,
            runtime,
            cleanup_armed: false,
        };
        ensure_vacant(&*stack.runtime, &stack.project)?;
        if spec.build_images && !stack.run(&["build"])? {
            return Err(format!("Failed to build {} Docker image", spec.name).into());
        }
        // `up` can create resources even when it fails.
        stack.cleanup_armed = true;
        if !stack.run(&["up", "-d"])? {
            return Err(format!("Failed to start {} with docker compose", spec.name).into());
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
        self.runtime.compose(&self.compose_file, &self.project, &self.env, args)
    }

    /// The value of the compose file variable `name`, if the stack sets it
    pub fn env(&self, name: &str) -> Option<&str> {
        self.env.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }

    /// Host port Docker published on 127.0.0.1 for `service`'s container `port`
    ///
    /// Returns `None` if the service is not running or is not published on
    /// exactly one loopback address.
    pub fn published_port(&self, service: &str, port: u16, protocol: &str) -> Option<u16> {
        self.runtime
            .port(&self.compose_file, &self.project, &self.env, service, port, protocol)
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

    fn compose(
        &self,
        file: &Path,
        project: &str,
        env: &[(String, String)],
        args: &[&str],
    ) -> std::io::Result<bool> {
        compose_command(COMPOSE, file, project, args)
            .envs(env.iter().map(|(name, value)| (name, value)))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
    }

    fn port(
        &self,
        file: &Path,
        project: &str,
        env: &[(String, String)],
        service: &str,
        port: u16,
        protocol: &str,
    ) -> Option<u16> {
        let target = port.to_string();
        let output = compose_command(
            COMPOSE,
            file,
            project,
            &["port", "--protocol", protocol, service, &target],
        )
        .envs(env.iter().map(|(name, value)| (name, value)))
        .stderr(Stdio::null())
        .output()
        .ok()?;
        if !output.status.success() {
            return None;
        }
        parse_published_port(std::str::from_utf8(&output.stdout).ok()?)
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

/// `<cmd> -p <project> -f <file> <args>`
fn compose_command(cmd: &[&str], file: &Path, project: &str, args: &[&str]) -> Command {
    let mut command = Command::new(cmd[0]);
    command.args(&cmd[1..]).arg("-p").arg(project).arg("-f").arg(file).args(args);
    command
}

fn parse_published_port(output: &str) -> Option<u16> {
    // Reject non-loopback, multiple bindings and malformed output rather than guessing.
    let address: SocketAddr = output.trim().parse().ok()?;
    match address {
        SocketAddr::V4(addr) if *addr.ip() == Ipv4Addr::LOCALHOST && addr.port() != 0 => {
            Some(addr.port())
        }
        _ => None,
    }
}

/// `127.0.0.1:<port>`
pub fn addr(port: u16) -> SocketAddr {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port).into()
}

/// Whether the server on `127.0.0.1:<port>` answers `request` with a reply
/// containing `expected` within its first 1024 bytes
pub fn answers(port: u16, request: &[u8], expected: &[u8]) -> bool {
    use std::io::Write;
    let Ok(mut stream) = TcpStream::connect_timeout(&addr(port), Duration::from_secs(1)) else {
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
    fn parses_only_one_loopback_binding() {
        assert_eq!(parse_published_port("127.0.0.1:32768\n"), Some(32768));
        for invalid in [
            "0.0.0.0:1234",
            "127.0.0.1:0",
            "127.0.0.1:12\n127.0.0.1:13",
            "[::1]:1234",
            "garbage",
        ] {
            assert_eq!(parse_published_port(invalid), None);
        }
    }
    #[test]
    fn every_operation_has_own_project() {
        for args in [
            &["build"][..],
            &["up", "-d"],
            &["port", "--protocol", "tcp", "redis", "6379"],
            &["down", "-v"],
        ] {
            let command = compose_command(
                &["docker", "compose"],
                Path::new("fixture.yml"),
                "xylemtest123",
                args,
            );
            let actual: Vec<_> =
                command.get_args().map(|s| s.to_string_lossy().into_owned()).collect();
            assert_eq!(&actual[..5], ["compose", "-p", "xylemtest123", "-f", "fixture.yml"]);
            assert_eq!(&actual[5..], args);
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
        down_fails: bool,
        down_unexecuted: bool,
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
        fn compose(
            &self,
            _: &Path,
            project: &str,
            _: &[(String, String)],
            args: &[&str],
        ) -> std::io::Result<bool> {
            let mut s = self.0.lock().unwrap();
            s.events.push(format!("compose {project} {}", args.join(" ")));
            match args[0] {
                "build" => Ok(true),
                "up" => Ok(!s.up_fails),
                "down" if s.down_unexecuted => Err(std::io::Error::other("fake execution failure")),
                "down" => Ok(!s.down_fails),
                _ => panic!("unexpected command"),
            }
        }
        fn port(
            &self,
            _: &Path,
            _: &str,
            _: &[(String, String)],
            _: &str,
            _: u16,
            _: &str,
        ) -> Option<u16> {
            panic!("unexpected port lookup")
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
    fn lifecycle_failed_up_and_readiness_cleanup_only_owned_project() {
        for up_fails in [false, true] {
            let state = Arc::new(Mutex::new(FakeState { up_fails, ..Default::default() }));
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
