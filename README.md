# DB Mover

DB Mover is a data moving tool for different databases, aimed at providing the fastest experience.

## Install

```bash
cargo install db-mover
```

## Usage

```bash
db-mover --input "sqlite://input.db" --output "postgres://postgres@localhost/postgres"
```

Use help for options descriptions
```bash
db-mover --help
```

## Features

- [x] PostgreSQL support
- [x] SQLite support
- [x] MySQL / MariaDB support
- [ ] Microsoft SQL Server support
- [ ] Oracle support
- [x] schema compatibility check
- [ ] schema generation

## Types conversion

DB Mover ensures schema compatibility by comparing the table schemas from the reader and writer databases. Each supported database type is mapped to a unified DB Mover type. A table is considered compatible if the type mapping from the reader database to the DB Mover type matches the type mapping from the writer database to the same DB Mover type.

### Supported types

- `String` - UTF-8–encoded string
- `Bytes` - sequence of bytes
- `I64` - 64-bit signed integer
- `I32` - 32-bit signed integer
- `I16` - 16-bit signed integer
- `F64` - 64-bit floating-point type
- `F32` - 32-bit floating-point type
- `Decimal` - decimal number
- `Bool` - logical value that can be either true or false
- `Timestamptz` - timestamp with time zone
- `Timestamp` - timestamp without time zone
- `Date` - date type
- `Time` - time type
- `Uuid` - 128-bit Universally Unique Identifier (UUID)
- `Json` - JSON type

### PostgreSQL

| PostgreSQL                  | DB Mover      |
|-----------------------------|---------------|
| varchar, text, bpchar       | String        |
| bytea                       | Bytes         |
| bigint, bigserial           | I64           |
| integer, serial             | I32, I64      |
| smallint, smallserial       | I16, I32, I64 |
| double precision            | F64           |
| real                        | F32, F64      |
| decimal, numeric            | Decimal       |
| boolean                     | Bool          |
| timestamp with time zone    | Timestamptz   |
| timestamp without time zone | Timestamp     |
| date                        | Date          |
| time without time zone      | Time          |
| UUID                        | Uuid          |
| JSON, JSONB                 | Json          |

### MySQL/MariaDB

| Type name (case insensitive)                            | DB Mover      | Comment                                                                            |
|---------------------------------------------------------|---------------|------------------------------------------------------------------------------------|
| char, varchar, tinytext, text, mediumtext, longtext     | String        |                                                                                    |
| binary, varbinary, tinyblob, blob, mediumblob, longblob | Bytes         |                                                                                    |
| bigint                                                  | I64           |                                                                                    |
| integer                                                 | I32, I64      |                                                                                    |
| smallint                                                | I16, I32, I64 |                                                                                    |
| double, double precision, real                          | F64           |                                                                                    |
| float                                                   | F32, F64      |                                                                                    |
| decimal, numeric                                        | Decimal       |                                                                                    |
| boolean, bool, tinyint(1)                               | Bool          |                                                                                    |
| timestamp                                               | Timestamptz   |                                                                                    |
| datetime                                                | Timestamp     |                                                                                    |
| date                                                    | Date          |                                                                                    |
| time                                                    | Time          |                                                                                    |
| binary(16)                                              | Uuid          | Can be disabled with flag `--no-mysql-binary-16-as-uuid`                           |
| json, longtext                                          | Json          | In MariaDB, a longtext column with a `JSON_VALID()` constraint is treated as JSON. |

### SQLite

SQLite uses [dynamic typing](https://www.sqlite.org/datatype3.html), so type mapping is based on the declared column type names. If an actual column value does not match the expected DB Mover type inferred from the column name, an error will be raised.

| SQLite type name (case insensitive)                               | DB Mover      | Comment                                                                           |
|-------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------|
| character, varchar*, nvarchar*, char*, nchar*, clob, text, bpchar | String        | `*` - means zero or more chars in type name                                       |
| bytea, blob                                                       | Bytes         |                                                                                   |
| bigint, bigserial                                                 | I64           |                                                                                   |
| integer, serial                                                   | I32, I64      |                                                                                   |
| tinyint, smallint, smallserial                                    | I16, I32, I64 |                                                                                   |
| double, double precision                                          | F64           |                                                                                   |
| real, float                                                       | F32, F64      |                                                                                   |
| boolean, bool                                                     | Bool          |                                                                                   |
| timestamptz                                                       | Timestamptz   | RFC3339 ("YYYY-MM-DD HH:MM:SS.SSS+-HH:MM")                                        |
| timestamp, datetime                                               | Timestamp     | ISO 8601 "YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS"                          |
| date                                                              | Date          | "YYYY-MM-DD"                                                                      |
| time                                                              | Time          | ISO 8601 time without timezone "HH:MM"/"HH:MM:SS"/"HH:MM:SS.SSS"                  |
| uuid                                                              | Uuid          | 4 bytes blob                                                                      |
| json, jsonb                                                       | Json          | [Rules](https://docs.rs/rusqlite/latest/src/rusqlite/types/serde_json.rs.html#31) |

## Development

### Build

To build executable run:
```
cargo build --release
```

### Tests
Docker required. After end of tests some containers might be still alive, remove them manually.

Run tests:
```bash
cargo test
```

Run benchmarks:
```bash
cargo bench
```

### Pre-commit hooks

Install pre-commit hooks
```bash
pre-commit install
```
