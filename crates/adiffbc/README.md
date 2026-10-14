# adiffbc

Differential testing for arbitrary [ADBC](https://arrow.apache.org/adbc/) drivers.

`adiffbc` loads two or more ADBC drivers through `adbc_driver_manager`. It runs
the same setup SQL and the same query corpus on each driver, then compares each
candidate result with the oracle result. The comparison uses the
`StrictComparator` from `crates/core`.

```
  campaign.yaml
       |
       v
  +---------+   setup SQL + corpus   +----------------------+
  | adiffbc | ---------------------> | oracle  (ADBC driver)|--+
  +---------+          |             +----------------------+  |  compare
                       +-----------> | candidate (driver N) |--+--> findings
                                     +----------------------+
```

## Commands

| Command                     | Description                                              |
|-----------------------------|----------------------------------------------------------|
| `adiffbc validate <config>` | Parse the config. Does not load drivers.                  |
| `adiffbc probe <config>`    | Load each driver, connect, and print the driver info.    |
| `adiffbc run <config>`      | Run the campaign. Exit code 1 means findings, 2 means error. |

Use `--format json` for machine-readable output.

## Configuration

See `examples/adbc-sqlite-duckdb/campaign.yaml`.

- `drivers.<name>.driver`: a driver or manifest name that the ADBC driver
  manager can find, or a path to a shared library or `.toml` manifest. A path
  is relative to the config file. `${env:NAME}` is supported.
- `drivers.<name>.entrypoint`: the driver init symbol, if it is not the
  default (for example, DuckDB uses `duckdb_adbc_init`).
- `drivers.<name>.adbc_version`: `1.0.0` or `1.1.0` (default).
- `variants.<name>`: one driver connection. Set `oracle: true` on exactly one
  variant. `uri`, `options` (database options), and `connection_options` are
  sent to the driver.
- `setup`: SQL scripts that run on every variant before the queries. Scripts
  are split at top-level `;`.
- `campaign.schema`: `relaxed` (default) ignores top-level nullability and
  metadata. `strict` compares schemas exactly.

## Current limits

- No per-query timeout. ADBC calls are blocking.
- No artifacts are written. Findings go to stdout only.
- Only the `corpus` generator is supported.
