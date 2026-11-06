//! Docker compose fixture shared by the service guards
//!
//! Every Docker-based test stack is started, awaited, and torn down here. A guard
//! names its compose file and a readiness probe; this module detects
//! docker-compose versus docker compose, runs `up -d`, polls the probe, and runs
//! `down -v` on drop, but only for a stack it started itself.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

// Global lock to prevent concurrent container start within one test process
static COMPOSE_LOCK: Mutex<()> = Mutex::new(());

/// Pause after `up -d` before the first readiness probe
const STARTUP_DELAY: Duration = Duration::from_secs(2);

/// Pause between readiness probes
const PROBE_INTERVAL: Duration = Duration::from_millis(500);

/// Readiness probes after `up -d` before giving up (30 seconds at `PROBE_INTERVAL`)
const PROBE_ATTEMPTS: u32 = 60;

/// Compose command line, either `docker-compose` or `docker compose`
type ComposeCommand = &'static [&'static str];

/// Description of one Docker compose test stack
pub struct ComposeSpec {
    /// Service name used in progress and error messages
    pub name: &'static str,
    /// Compose file path relative to the workspace root
    pub compose_file: &'static str,
    /// Run `build` before `up -d` so the image is rebuilt from its Dockerfile
    pub build_images: bool,
    /// Returns true once every service in the stack answers requests
    pub is_ready: fn() -> bool,
}

/// A ready Docker compose stack, torn down on drop if this fixture started it
pub struct ComposeStack {
    compose_file: PathBuf,
    /// Command that started the stack, or `None` if the stack was already
    /// running and must be left up on drop
    started_with: Option<ComposeCommand>,
}

impl ComposeStack {
    /// Start the stack described by `spec` and wait for it to be ready
    ///
    /// This will:
    /// 1. Reuse the stack if its readiness probe already passes
    /// 2. Check if Docker and docker-compose are available
    /// 3. Build the images if `spec.build_images` is set (no timeout)
    /// 4. Start the stack via docker-compose up -d
    /// 5. Wait for the readiness probe to pass
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - The compose file does not exist
    /// - Docker or docker-compose is not available
    /// - The images fail to build
    /// - The stack fails to start
    /// - The stack doesn't become ready within 30 seconds
    pub fn start(spec: &ComposeSpec) -> Result<Self, Box<dyn std::error::Error>> {
        // Acquire lock to prevent concurrent start attempts
        let _lock = COMPOSE_LOCK.lock().unwrap();

        // Get path to docker-compose.yml first
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let workspace_root = Path::new(manifest_dir).parent().ok_or("No parent directory")?;
        let compose_file = workspace_root.join(spec.compose_file);

        if !compose_file.exists() {
            return Err(format!("docker-compose.yml not found at {:?}", compose_file).into());
        }

        // Check if the stack is already running
        if (spec.is_ready)() {
            // Someone else started it, so return a guard that won't stop it
            eprintln!("{} already running and ready", spec.name);
            return Ok(Self { compose_file, started_with: None });
        }

        // Check if Docker is available
        let docker_check = Command::new("docker").arg("--version").output()?;
        if !docker_check.status.success() {
            return Err("Docker is not available".into());
        }

        let compose_cmd = detect_compose_command()
            .ok_or("docker-compose not available (tried 'docker-compose' and 'docker compose')")?;

        if spec.build_images {
            // Build separately so the build can take as long as it needs
            eprintln!(
                "Building {} Docker image (this may take a few minutes on first run)...",
                spec.name
            );
            let build_status = run_compose(compose_cmd, &compose_file, &["build"])?;
            if !build_status.success() {
                return Err(format!("Failed to build {} Docker image", spec.name).into());
            }
            eprintln!("{} Docker image built successfully", spec.name);
        }

        let up_status = run_compose(compose_cmd, &compose_file, &["up", "-d"])?;
        if !up_status.success() {
            return Err(format!("Failed to start {} with docker-compose", spec.name).into());
        }

        thread::sleep(STARTUP_DELAY);

        // Verify the stack is ready
        for attempt in 0..PROBE_ATTEMPTS {
            if (spec.is_ready)() {
                eprintln!("{} ready after {} attempts", spec.name, attempt + 1);
                return Ok(Self {
                    compose_file,
                    started_with: Some(compose_cmd),
                });
            }
            thread::sleep(PROBE_INTERVAL);
        }

        Err(format!("{} failed to become ready within 30 seconds", spec.name).into())
    }
}

impl Drop for ComposeStack {
    fn drop(&mut self) {
        // Only the fixture that started the stack tears it down
        if let Some(compose_cmd) = self.started_with {
            // Stop and remove containers using docker-compose down -v
            let _ = run_compose(compose_cmd, &self.compose_file, &["down", "-v"]);
        }
    }
}

/// Find the compose command, trying docker-compose before docker compose
fn detect_compose_command() -> Option<ComposeCommand> {
    let standalone = Command::new("docker-compose")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if standalone {
        return Some(&["docker-compose"]);
    }

    let plugin = Command::new("docker")
        .args(["compose", "version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if plugin {
        return Some(&["docker", "compose"]);
    }

    None
}

/// Run `<compose_cmd> -f <compose_file> <args>` with its output discarded
fn run_compose(
    compose_cmd: ComposeCommand,
    compose_file: &Path,
    args: &[&str],
) -> std::io::Result<ExitStatus> {
    Command::new(compose_cmd[0])
        .args(&compose_cmd[1..])
        .arg("-f")
        .arg(compose_file)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
}
