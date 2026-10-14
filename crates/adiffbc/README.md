# adiffbc

Differential testing for arbitrary [ADBC](https://arrow.apache.org/adbc/) drivers.

`adiffbc` connects to two or more ADBC databases. It runs the same queries for each, and compares candidate results with the oracle.

```
  adiffbc run --oracle ... --candidate ... --queries ...
       |
       v
  +---------+ Create tables & populate  +----------------------+
  | adiffbc | ------------------------> | oracle  (ADBC driver)|--+
  +---------+          |                +----------------------+  |    compare
                       +----------->    | candidate (driver 1) |--+--> findings
                                        +----------------------+  |
                                                ....              |
                                        +----------------------+  |
                                        | candidate (driver N) |--+
                                        +----------------------+
```

## Commands

| Command                                | Description                                                  |
|----------------------------------------|--------------------------------------------------------------|
| `adiffbc run --oracle U --candidate U` | Run queries. Exit code 1 means findings, 2 means error.      |
| `adiffbc probe U...`                   | Load (and install) drivers, connect, and print driver info.  |

Use `--format json` for machine-readable output. See `adiffbc run --help`.

## Variants

A variant is one database connection, written as `[NAME=]DRIVER:URI` or
`[NAME=]profile://PROFILE`. Without `NAME=`, the name is the driver or
profile name. Names must be unique, so name variants that share a driver.

| Variant                                | Name         | Driver               | `uri` option sent to the driver |
|----------------------------------------|--------------|----------------------|---------------------------------|
| `sqlite::memory:`                      | `sqlite`     | `sqlite`             | `:memory:`                      |
| `duckdb:data.duckdb`                   | `duckdb`     | `duckdb`             | `data.duckdb`                   |
| `flightsql:grpc://localhost:50051`     | `flightsql`  | `flightsql`          | `grpc://localhost:50051`        |
| `pg=postgresql://user@host:5432/db`    | `pg`         | `postgresql`         | the whole URI                   |
| `datafusion`                           | `datafusion` | `datafusion`         | none                            |
| `old=sqlite@1.11.0::memory:`           | `old`        | `sqlite` 1.11.0      | `:memory:`                      |
| `./libmy_driver.so:file.db`            | `my_driver`  | that library         | `file.db`                       |
| `profile://staging`                    | `staging`    | from the profile     | none                            |
| `pg=profile://staging`                 | `pg`         | from the profile     | none                            |

`DRIVER://...` sends the whole URI. `DRIVER:REST` sends only `REST`. For
example, `flightsql://host:port` sends `flightsql://host:port`, which the
Flight SQL driver does not accept. Use `flightsql:grpc://host:port`.

### Options
Driver options in `adiffbc` are the ADBC option key-values. 

- `-o NAME.KEY=VALUE`: an ADBC database option for variant `NAME`.
- `-c NAME.KEY=VALUE`: an ADBC connection option for variant `NAME`.

`NAME` can be `*` for all variants. Later values win. `-o` values override
the values from a URI or a profile. `adiffbc` does not check the keys. The
driver checks them, and most drivers reject unknown keys.

```sh
adiffbc run \
  --oracle    serial=duckdb::memory:   -o serial.threads=1 \
  --candidate parallel=duckdb::memory: -o parallel.threads=8 \
  -c '*.adbc.connection.autocommit=true' \
  --setup fixtures.sql --queries 'queries/*.sql'
```

Here `serial` and `parallel` both get `adbc.connection.autocommit=true`.

### Profiles

`adiffbc` supports [ADBC connection profiles](https://arrow.apache.org/adbc/current/format/connection_profiles.html)
(`profile://NAME`). A profile keeps a driver and its options in a TOML file,
and can read secrets from environment variables. Use a profile for secrets,
because command lines can appear in `ps` output and shell history.
`adiffbc` stops with an error if a profile uses an environment variable
that is not set.

## Queries

- `--queries FILE...` (`-q`): query files, one query per file. A quoted glob
  such as `'queries/*.sql'` is expanded in sorted order.
- `-e SQL`: an inline query. Inline queries run after the query files.

## Setup SQL

`--setup FILE` runs on every variant. `--setup NAME=FILE` runs on variant
`NAME` only. Use this when the variants need different SQL to create the
same data. Scripts run in the given order and are split at top-level `;`.

## Drivers

`adiffbc` finds a driver by name in this order:

1. `$ADBC_DRIVER_PATH`
2. the install directory (`--install-dir`, default: the user cache
   directory, for example `~/Library/Caches/adiffbc/drivers`)
3. `$VIRTUAL_ENV/etc/adbc/drivers` and `$CONDA_PREFIX/etc/adbc/drivers`
4. the ADBC user and system directories

If it does not find the driver, it runs `dbc install DRIVER` into the install
directory. [dbc](https://docs.columnar.tech/dbc/) checks driver signatures
and writes a driver manifest. `adiffbc` runs `dbc`, `uvx dbc`, or
`pipx run dbc`, in that order. Use `--dbc "COMMAND"` to set the command, and
`--no-install` to never install.

A pinned driver (`sqlite@1.11.0`, or a range such as `'duckdb@>=1.4,<2'`)
always goes through `dbc install`, which does nothing if that version is
already installed. Each pinned version has its own directory, so two
versions of one driver can be compared in one run:

```sh
adiffbc run --oracle old=sqlite@1.11.0::memory: \
            --candidate new=sqlite@1.12.0::memory: --queries 'queries/*.sql'
```

## Current limits

- No per-query timeout. ADBC calls are blocking.
- No artifacts are written. Findings go to stdout only.
- Queries come from `--queries` and `-e` only. There is no random generator yet.
- A raw library path uses the default entrypoint name. For a driver with a
  different entrypoint (for example DuckDB), use its manifest or a dbc install.
