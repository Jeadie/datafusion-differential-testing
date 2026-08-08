# DataFusion Differential Testing
A library and declarative, config-driven tool to perform differential correctness testing for DataFusion `SessionContext` and `TableProviders`. 

## Example
A single testing campaign defines:
 - One or more table schemas and arrow data
 - A source of SQL queries (e.g. a file of SQL queries, random fuzz, AST mutations, etc)
 - Two or more variants defining a mapping of table -> [`TableProvider`].

```yaml
# One or more table schemas and arrow data
dataset:
  paths:
    - fixtures/data.sql

# A source of SQL queries.
generators:
  - type: corpus
    paths:
      - queries/*.sql

# >=2 variants defining a mapping of table -> [`TableProvider`].
variants:
  oracle:
    oracle: true
    tables:
      "*": memtable
  duckdb_nonfederated:
    tables:
      "*":
        provider: duck_shared

# Configuration to instantiate [`TableProvider`] via `datafusion-contrib/datafusion-table-providers`. 
providers:
  duck_shared:
    type: duckdb
    params:
      duckdb_open: .datafusion-differential-test/duck-shared.db
```

Then 
```bash
datafusion-differential-test run examples/four-variant/campaign.yaml
```

The runnable example under `examples/four-variant` compares an in-memory oracle
against a federated and a non-federated variant of the same DuckDB and SQLite
providers. Running it surfaces a real difference:

```
Completed 3 queries with 2 findings. Artifacts: examples/four-variant/artifacts/...
  - [duckdb_nonfederated] CandidateRejected: SELECT count(1) AS n FROM orders;
  - [sqlite_nonfederated] CandidateRejected: SELECT count(1) AS n FROM orders;
```

We can see details in the findings artifact
```json
{
  "id": "finding-000000",
  "query_id": "query-000001",
  "candidate": "duckdb_nonfederated",
  "kind": "candidate_rejected",
  "message": "candidate rejected the query: Internal error: Physical input schema should be the same as the one converted from logical input schema. Differences: \n\t- Different number of fields: (physical) 3 vs (logical) 0.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues",
  "sql": "SELECT count(1) AS n FROM orders;\n"
}
```
A datafusion bug (in `datafusion-table-providers`):
```bash
Internal error: Physical input schema should be the same as the one converted from logical input schema.Differences:
  - Different number of fields: (physical) 3 vs (logical) 0.
This issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues
```

## Supported Datasets

Supports per-table fixtures: SQL, arrow IPC, parquet, csv or oracle providers (see below).
```yaml
dataset:
  - name: orders # arrow IPC inferred
    path: fixtures/orders.arrow
  - name: customers
    path: fixtures/customers.csv
    options:
      has_header: "true"
      delimiter: ","
  - name: events
    path: fixtures/events.parquet
  - name: lineitem
    variant: truth # Use `lineitem` table from `truth` variant. Expected to exist.
```

As well as multi-table declarations:

```yaml
# Use DDL/DML from SQL files.
dataset:
  paths:
    - fixtures/data.sql

# Use all tables from the `truth` variant. Will write to remaining `lifecycle=managed` variants.
dataset:
  variant: truth

variants:
  truth:
    oracle: true
    tables:
      "*": reuse_sqlite_file

providers:
  reuse_sqlite_file:
    type: sqlite
    lifecycle: existing # Dont insert/delete data
    params:
      sqlite_open: retained.sqlite
```

## Supported Generators
Currently, only a hardcoded list of SQL queries exist. SQL is applied in order of `paths`.
```yaml
generators:
  - type: corpus
    paths:
      - queries/*.sql
```

## Support Providers

Currently three tables are supported. Sqlite and duckdb from `datafusion-table-providers`, and the Datafusion default `MemTable`.

```yaml
providers:
  a_file_that_is_duckdb:
    type: duckdb
    params:
      duckdb_open: /path/to/my/duck.db # will create/overwrite .
  also_sqlite:
    type: sqlite
    lifecycle: existing
    params:
      sqlite_open: i_have_the_data.sqlite
  in_memory_duckdb:
    type: duckdb
```

`MemTable` does not need a provider to be used in `variants`.

## Federation support

Variants can also be configured to support `datafusion-federation` (see repo: https://github.com/datafusion-contrib/datafusion-federation). 

```yaml
variants:
  duckdb_federated:
    tables:
      # All tables in one duckdb file. Full federation pushdown.
      "*":
        provider: a_file_that_is_duckdb
    session:
      extensions:
        - type: federation

  duckdb_federated_unshared:
    tables:
      # Since customer and orders use different duckdb files, they wont federate together.
      customer:
        provider: duckdb_file_one
      orders:
        provider: duckdb_file_two
    session:
      extensions:
        - type: federation
```
