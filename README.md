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
  duckdb_shared_federated:
    tables:
      "*":
        provider: duck_shared
    session:
      extensions:
        - type: federation

# Configuration to instantiate [`TableProvider`] via `datafusion-contrib/datafusion-table-providers`. 
providers:
  duck_shared:
    type: duckdb
    params:
      duckdb_open: .datafusion-differential-test/duck-shared.db
```

Then 
```bash
datafusion-differential-test run example/campaign.yaml
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
