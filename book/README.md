# Xylem Documentation

This directory contains the mdBook documentation for Xylem.

## Building the Documentation

### Prerequisites

Install mdBook:

```bash
cargo install mdbook
```

### Build

Build the documentation:

```bash
cd book
mdbook build
```

The generated HTML will be in `book/book/`.

### Serve Locally

Serve the documentation locally with live reload:

```bash
cd book
mdbook serve
```

Then open http://localhost:3000 in your browser.

### Watch for Changes

The `serve` command automatically rebuilds when source files change.

## Documentation Structure

- `src/` - Markdown source files
- `book.toml` - mdBook configuration
- `book/` - Generated HTML (git-ignored)

## Contributing

When adding new pages:

1. Create the markdown file in `src/`
2. Add it to `src/SUMMARY.md`
3. Build and verify: `mdbook build`

## Publishing

The documentation can be published to GitHub Pages or any static hosting service.

### GitHub Pages

Add to `.github/workflows/docs.yml`:

```yaml
name: Deploy Docs

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Setup mdBook
        uses: peaceiris/actions-mdbook@v1
        with:
          mdbook-version: 'latest'
      - name: Build
        run: cd book && mdbook build
      - name: Deploy
        uses: peaceiris/actions-gh-pages@v3
        with:
          github_token: ${{ secrets.GITHUB_TOKEN }}
          publish_dir: ./book/book
```

## See Also

- [mdBook Documentation](https://rust-lang.github.io/mdBook/)
- [Xylem Repository](https://github.com/minhuw/xylem)
