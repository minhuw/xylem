# Changelog

Release history and notable changes for Xylem.

## [Unreleased]

### Added
- mdBook documentation structure
- Comprehensive user guide
- Architecture documentation
- Examples and tutorials

## [0.1.0] - 2024-XX-XX

### Added
- Initial release of Xylem
- Support for Redis protocol
- Support for HTTP protocol
- Support for Memcached protocol
- TCP transport implementation
- UDP transport implementation
- Unix domain socket transport
- TLS transport support
- Configurable workload patterns
- Multiple statistics algorithms:
  - DDSketch
  - T-Digest
  - HDR Histogram
- JSON configuration support
- CLI interface with clap
- Multiple output formats (text, JSON, CSV)
- Connection pooling
- Pipelining support (Redis)
- Comprehensive test suite
- CI/CD with GitHub Actions
- Code coverage tracking

### Infrastructure
- Multi-crate workspace structure
- Modular architecture
- Event-driven I/O with mio
- Zero-copy optimizations

## Release Schedule

Xylem follows semantic versioning (SemVer):

- **Major version** (X.0.0): Breaking changes
- **Minor version** (0.X.0): New features, backwards compatible
- **Patch version** (0.0.X): Bug fixes

## Upcoming Features

See the [GitHub Issues](https://github.com/minhuw/xylem/issues) for planned features and roadmap.

## Contributing

See [Contributing Guide](../contributing/setup.md) for how to contribute.

## See Also

- [GitHub Releases](https://github.com/minhuw/xylem/releases)
- [Migration Guide](../guide/migration.md) (for breaking changes)
