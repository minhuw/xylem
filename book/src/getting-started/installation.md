# Installation

## Prerequisites

Xylem is written in Rust and requires:

- **Rust**: Version 1.70 or later
- **Cargo**: Rust's package manager (comes with Rust)

## Installing Rust

If you don't have Rust installed, you can install it using [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After installation, ensure your Rust installation is up to date:

```bash
rustup update
```

## Building from Source

1. Clone the repository:

```bash
git clone https://github.com/minhuw/xylem.git
cd xylem
```

2. Build the project in release mode:

```bash
cargo build --release
```

3. The compiled binary will be located at `target/release/xylem`

4. (Optional) Install the binary to your system:

```bash
cargo install --path xylem-cli
```

## Verifying Installation

Verify that Xylem is correctly installed:

```bash
xylem --version
```

You should see output showing the version number.

## Next Steps

- Follow the [Quick Start guide](./quick-start.md) to run your first benchmark
- Read about [Basic Usage](./basic-usage.md) to learn core concepts
