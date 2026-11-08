{
  description = "Xylem - A simple, high-performance latency measurement tool for single-machine deployments";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    # The DPDK SDK of the `ntcp-dpdk` feature, from the ntcp commit that
    # xylem-transport/Cargo.toml pins; built from this flake's nixpkgs
    ntcp = {
      url = "github:minhuw/ntcp/46f8390913582a84ac746a21cfc5a24d391c9e2c";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, ntcp }:
    # nixpkgs unstable no longer supports x86_64-darwin
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ] (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Rust toolchain with specific components
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" "llvm-tools-preview" ];
        };

        # Build inputs for the project; on macOS the stdenv's default SDK
        # provides the system frameworks
        buildInputs = with pkgs; [
          openssl
          pkg-config
        ];

        nativeBuildInputs = with pkgs; [
          pkg-config
          protobuf # Required for evalsync's prost-build
        ];

        # ntcp's static DPDK (with the mlx5 PMDs) and the libraries that
        # `pkg-config --static libdpdk` names, for the `ntcp-dpdk` feature.
        # ntcp builds it for x86_64-linux only, so `--all-features` builds
        # need that system.
        dpdkInputs = pkgs.lib.optionals (system == "x86_64-linux") [
          ntcp.packages.${system}.dpdk-mlx5
          pkgs.rdma-core
          pkgs.elfutils
          pkgs.intel-ipsec-mb
          pkgs.jansson
          pkgs.libnl
          pkgs.libpcap
          pkgs.numactl
          pkgs.openssl
          pkgs.zlib
          pkgs.zstd
        ];

        # The package is built with upstream Rust, whose prebuilt standard library has line tables
        # (nixpkgs' rustc builds it without), so profilers can resolve std code to source lines.
        rustVersion = "1.99.1";
        rustPackageToolchain = pkgs.rust-bin.stable.${rustVersion}.minimal;
        rustPlatform = pkgs.makeRustPlatform {
          cargo = rustPackageToolchain;
          rustc = rustPackageToolchain;
        };

        # Sources that standard library was compiled from: library/ and the crates its Cargo.lock
        # takes from vendor/, except the Windows-only ones, which no Unix build compiles. Its DWARF
        # paths are /rustc/<commit>/library/... and /rust/deps/<crate>-<version>/...
        rustSrcTop = "rustc-${rustVersion}-src";
        rustSrcTarball = pkgs.fetchurl {
          url = "https://static.rust-lang.org/dist/${rustSrcTop}.tar.xz";
          sha256 = "6451598a99e77f07571960f6b21d577399d3a30f33e80cbd2e3a4aae6535d265";
        };
        rustStdSrc = pkgs.runCommand "rust-std-src-${rustVersion}" { } ''
          tar -tJf ${rustSrcTarball} > members
          mkdir -p $out
          tar -xJf ${rustSrcTarball} -C $out ${rustSrcTop}/library
          # Registry packages of std's lockfile, as cargo vendor --versioned-dirs names them, without
          # windows-sys, windows-targets and the windows_<target> import libraries
          awk -F '"' '/^name = /{n=$2} /^version = /{v=$2} /^source = "registry/{print n "-" v}' \
            $out/${rustSrcTop}/library/Cargo.lock | grep -vE '^windows[-_]' | sort -u > crates
          grep -oE '^${rustSrcTop}/vendor/[^/]+/' members | sort -u > vendored
          grep -F -f <(sed 's|.*|${rustSrcTop}/vendor/&/|' crates) -x vendored > needed
          echo "$(wc -l < needed) of $(wc -l < crates) non-Windows crates of library/Cargo.lock are vendored"
          tar -xJf ${rustSrcTarball} -C $out $(sed 's|/$||' needed)
        '';

      in
      {
        # Development shell
        devShells.default = pkgs.mkShell ({
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

            # Testing tools
            pkgs.cargo-nextest
            # Its tests fail in the nixpkgs sandbox, where nixpkgs' rustc writes
            # newer raw profiles than the sandbox's llvm-profdata reads. Here it
            # uses the toolchain's own llvm-tools-preview, which match rustc.
            (pkgs.cargo-llvm-cov.overrideAttrs { doCheck = false; })

            # Pre-commit and formatting
            pkgs.pre-commit
            pkgs.git

            # Testing and benchmarking
            pkgs.hyperfine

            # Additional utilities
            pkgs.tokei # Code statistics
            pkgs.just # Command runner
          ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
            # Profiling, for flamegraph
            pkgs.perf
          ] ++ dpdkInputs ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
            # The `ntcp-af-xdp` feature links libbpf
            pkgs.libbpf
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
            echo "  just test                      - Run all tests (nextest)"
            echo "  just test-unit                 - Run unit tests only (fast)"
            echo "  just test-integration          - Run integration tests (requires Docker)"
            echo "  cargo build                    - Build the project"
            echo "  cargo clippy                   - Run linter"
            echo "  cargo fmt                      - Format code"
            echo "  cargo llvm-cov                 - Generate code coverage"
            echo "  cargo flamegraph --profile profiling -- <args>  - Profile with flamegraph"
            echo "  pre-commit install             - Install git hooks"
            echo "  bacon                          - Watch and build"
            echo ""
            echo "⚠️  Integration tests require Docker to be installed and running"
            echo ""

            # Setup pre-commit hooks if not already installed
            if [ ! -f .git/hooks/pre-commit ]; then
              echo "Installing pre-commit hooks..."
              pre-commit install
            fi
          '';
        } // pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
          # The `ntcp-af-xdp` feature compiles its XDP program for the BPF
          # target, with a clang that adds no host-target flags (not
          # cc-wrapper's) and the Linux UAPI headers
          NTCP_BPF_CLANG = "${pkgs.llvmPackages.clang-unwrapped}/bin/clang";
          NTCP_BPF_CFLAGS = "-isystem ${pkgs.linuxHeaders}/include";
        });

        # Package definition
        packages.default = rustPlatform.buildRustPackage {
          pname = "xylem";
          version = "0.1.0";

          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
            # One hash covers every crate of a git repository at its pinned rev
            outputHashes = {
              "ntcp-native-0.0.1" = "sha256-SzVa6N/SWdnXrhPcXfXH/bVoqdO72aQP03hIGv3OXqU=";
            };
          };

          # Enable evalsync feature for coordinated benchmarking
          buildFeatures = [ "xylem-cli/evalsync" ];

          inherit buildInputs nativeBuildInputs;

          # Skip tests that require external services (nginx) not available in sandbox
          doCheck = false;

          # Move DWARF into a separate `debug` output for profile symbolization. separateDebugInfo
          # asks for it through NIX_RUSTFLAGS, which only nixpkgs' wrapped rustc reads, so the level
          # is set in the cargo profile (the cargo build hook already turns its stripping off).
          separateDebugInfo = true;
          CARGO_PROFILE_RELEASE_DEBUG = "limited";

          # Rhea's bench packaging contract: debug/share/rhea/sources/<name> is the source tree the build
          # had at <build dir>/<name>, which DWARF paths refer to; profilers then need nothing else.
          # Upstream remaps the standard library to /rustc/<commit> and its crates to /rust/deps.
          postFixup = ''
            sources=$debug/share/rhea/sources
            mkdir -p $sources
            commit=$(rustc -vV | sed -n 's/^commit-hash: //p')
            [ -n "$commit" ] || { echo "rustc -vV gives no commit-hash" >&2; exit 1; }
            ln -s ${rustStdSrc}/${rustSrcTop} $sources/$commit
            ln -s ${rustStdSrc}/${rustSrcTop}/vendor $sources/deps
            ln -s $cargoDeps $sources/$(stripHash $cargoDeps)
            ln -s $src $sources/$(stripHash $src)
          '';

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
