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
  mode: fixtures
  fixtures:
    - type: sql
      path: fixtures/data.sql

# A source of SQL queries.
generators:
  - type: corpus
    paths:
      - queries/*.sql

# >=2 variants defining a mapping of table -> [`TableProvider`].
variants:
  oracle:
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
