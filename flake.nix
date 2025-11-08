{
  description = "Xylem - A simple, high-performance latency measurement tool for single-machine deployments";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Rust toolchain with specific components
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
        };

        # Build inputs for the project
        buildInputs = with pkgs; [
          openssl
          pkg-config
        ] ++ lib.optionals stdenv.isDarwin [
          darwin.apple_sdk.frameworks.Security
          darwin.apple_sdk.frameworks.SystemConfiguration
        ];

        nativeBuildInputs = with pkgs; [
          pkg-config
        ];

      in
      {
        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = buildInputs ++ [
            rustToolchain

            # Development tools
            pkgs.cargo-watch
            pkgs.cargo-edit
            pkgs.cargo-audit
            pkgs.cargo-flamegraph
            pkgs.cargo-deny
            pkgs.cargo-outdated
            pkgs.bacon

            # Pre-commit and formatting
            pkgs.pre-commit
            pkgs.git

            # Testing and benchmarking
            pkgs.hyperfine
            pkgs.redis # For integration tests
            pkgs.memcached # For integration tests
            pkgs.nginx # For HTTP integration tests

            # Profiling
            pkgs.linuxPackages.perf # For flamegraph on Linux

            # Additional utilities
            pkgs.tokei # Code statistics
            pkgs.just # Command runner
          ];

          inherit nativeBuildInputs;

          # Environment variables
          RUST_BACKTRACE = "1";
          RUST_LOG = "info";

          # Shell hook for setup
          shellHook = ''
            echo "🌳 Xylem development environment"
            echo "Rust version: $(rustc --version)"
            echo "Cargo version: $(cargo --version)"
            echo ""
            echo "Available commands:"
            echo "  cargo build                    - Build the project"
            echo "  cargo test                     - Run tests"
            echo "  cargo clippy                   - Run linter"
            echo "  cargo fmt                      - Format code"
            echo "  cargo flamegraph --profile profiling -- <args>  - Profile with flamegraph"
            echo "  pre-commit install             - Install git hooks"
            echo "  bacon                          - Watch and build"
            echo ""
            echo "See PROFILING.md for profiling guide"
            echo ""

            # Setup pre-commit hooks if not already installed
            if [ ! -f .git/hooks/pre-commit ]; then
              echo "Installing pre-commit hooks..."
              pre-commit install
            fi
          '';
        };

        # Package definition
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "xylem";
          version = "0.1.0";

          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          inherit buildInputs nativeBuildInputs;

          meta = with pkgs.lib; {
            description = "A simple, high-performance latency measurement tool for single-machine deployments";
            homepage = "https://github.com/minhuw/xylem";
            license = licenses.mit;
            maintainers = [ ];
            platforms = platforms.unix;
          };
        };
      }
    );
}
