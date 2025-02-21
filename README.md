# DB Mover

DB Mover is a data moving tool for different databases, aimed at providing the fastest experience.

## Development

### Build

To build executable run:
```
cargo build --release
```

### Tests

Run tests:
```bash
POSTGRES_URI="postgres://username@localhost/postgres" cargo test
```

`POSTGRES_URI` - URI for database, which will be used in tests. However, for each test run new database will be created and dropped. 

### Benchmark's

Generate data for benchmarks:
```bash
cargo run -p gen-bench-data -- --help
```

Run benchmark:
```bash
cargo bench
```

### Pre-commit hooks

Install pre-commit hooks
```bash
pre-commit install
```
