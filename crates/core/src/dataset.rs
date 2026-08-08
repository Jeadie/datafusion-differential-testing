use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use datafusion::{
    prelude::{CsvReadOptions, ParquetReadOptions, SessionContext},
    sql::parser::DFParser,
};

use crate::{
    compare::encode_rows,
    error::{Error, Result},
    DatasetId, FixtureSource, TableId,
};

#[derive(Debug, Clone)]
pub struct DatasetSnapshot {
    pub id: DatasetId,
    pub tables: BTreeMap<String, TableSnapshot>,
}

#[derive(Debug, Clone)]
pub struct TableSnapshot {
    pub id: TableId,
    pub schema: SchemaRef,
    pub ipc_path: PathBuf,
    pub row_count: u64,
    pub content_hash: String,
}

impl TableSnapshot {
    pub fn read_batches(&self) -> Result<Vec<RecordBatch>> {
        read_ipc(&self.ipc_path)
    }
}

pub struct SnapshotBuilder {
    output_directory: PathBuf,
}

impl SnapshotBuilder {
    #[must_use]
    pub fn new(output_directory: impl Into<PathBuf>) -> Self {
        Self {
            output_directory: output_directory.into(),
        }
    }

    pub async fn build(&self, fixtures: &[FixtureSource]) -> Result<DatasetSnapshot> {
        let (schemas, batches) = self.collect_fixtures(fixtures).await?;
        if schemas.is_empty() {
            return Err(Error::Dataset {
                table: "*".to_owned(),
                message: "fixtures produced no tables".to_owned(),
            });
        }
        self.build_from_batches(schemas, batches)
    }

    /// Register fixtures into a session and collect every resulting table's schema
    /// and batches. Unlike [`build`], this does not write IPC and tolerates an empty
    /// fixture set so callers can merge variant-sourced tables before building.
    pub async fn collect_fixtures(
        &self,
        fixtures: &[FixtureSource],
    ) -> Result<(
        BTreeMap<String, SchemaRef>,
        BTreeMap<String, Vec<RecordBatch>>,
    )> {
        fs::create_dir_all(&self.output_directory).map_err(|source| Error::Artifact {
            path: self.output_directory.clone(),
            source,
        })?;
        if fixtures.is_empty() {
            return Ok((BTreeMap::new(), BTreeMap::new()));
        }
        let context = SessionContext::new();
        for fixture in fixtures {
            match fixture {
                FixtureSource::Sql { path } => {
                    let sql = fs::read_to_string(path).map_err(|source| Error::Artifact {
                        path: path.clone(),
                        source,
                    })?;
                    execute_fixture_sql(&context, &sql).await?;
                }
                FixtureSource::SqlInline { sql } => execute_fixture_sql(&context, sql).await?,
                FixtureSource::ArrowIpc { name, path } => {
                    register_ipc(&context, name, path)?;
                }
                FixtureSource::Parquet { name, path } => {
                    context
                        .register_parquet(
                            name,
                            path.to_string_lossy(),
                            ParquetReadOptions::default(),
                        )
                        .await?;
                }
                FixtureSource::Csv {
                    name,
                    path,
                    options,
                } => {
                    let options = csv_options(options)?;
                    context
                        .register_csv(name, path.to_string_lossy(), options)
                        .await?;
                }
            }
        }

        let catalog = context
            .catalog("datafusion")
            .ok_or_else(|| Error::Dataset {
                table: "*".to_owned(),
                message: "default DataFusion catalog is unavailable".to_owned(),
            })?;
        let schema = catalog.schema("public").ok_or_else(|| Error::Dataset {
            table: "*".to_owned(),
            message: "default DataFusion schema is unavailable".to_owned(),
        })?;
        let mut table_names = schema.table_names();
        table_names.sort();

        let mut collected = BTreeMap::new();
        let mut schemas = BTreeMap::new();
        for name in table_names {
            let provider = schema.table(&name).await?.ok_or_else(|| Error::Dataset {
                table: name.clone(),
                message: "table disappeared during snapshot creation".to_owned(),
            })?;
            let escaped = name.replace('"', "\"\"");
            let batches = context
                .sql(&format!("SELECT * FROM \"{escaped}\""))
                .await?
                .collect()
                .await?;
            let schema = batches
                .first()
                .map(RecordBatch::schema)
                .or_else(|| Some(provider.schema()))
                .ok_or_else(|| Error::Dataset {
                    table: name.clone(),
                    message: "cannot determine table schema".to_owned(),
                })?;
            schemas.insert(name.clone(), schema);
            collected.insert(name, batches);
        }
        Ok((schemas, collected))
    }

    pub fn build_from_batches(
        &self,
        schemas: BTreeMap<String, SchemaRef>,
        batches: BTreeMap<String, Vec<RecordBatch>>,
    ) -> Result<DatasetSnapshot> {
        fs::create_dir_all(&self.output_directory).map_err(|source| Error::Artifact {
            path: self.output_directory.clone(),
            source,
        })?;
        let mut tables = BTreeMap::new();
        for (index, (name, schema)) in schemas.into_iter().enumerate() {
            let batches = batches.get(&name).ok_or_else(|| Error::Dataset {
                table: name.clone(),
                message: "snapshot batches are unavailable".to_owned(),
            })?;
            let id = TableId::new(format!("table-{index:06}"))?;
            let ipc_path = self.output_directory.join(format!("{id}.arrow"));
            write_ipc(&ipc_path, schema.as_ref(), batches)?;
            let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
            let content_hash = logical_hash(schema.as_ref(), batches)?;
            tables.insert(
                name,
                TableSnapshot {
                    id,
                    schema: schema.clone(),
                    ipc_path,
                    row_count,
                    content_hash,
                },
            );
        }
        Ok(DatasetSnapshot {
            id: DatasetId::new("dataset-000001")?,
            tables,
        })
    }
}

fn register_ipc(context: &SessionContext, name: &str, path: &Path) -> Result<()> {
    let batches = read_ipc(path)?;
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| Error::Dataset {
            table: name.to_owned(),
            message: "Arrow IPC fixture contains no record batch".to_owned(),
        })?;
    let table = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
    context.register_table(name, Arc::new(table))?;
    Ok(())
}

fn csv_options(options: &BTreeMap<String, String>) -> Result<CsvReadOptions<'static>> {
    let mut parsed = CsvReadOptions::new();
    for (key, value) in options {
        match key.as_str() {
            "has_header" => {
                parsed = parsed.has_header(value.parse().map_err(|_| Error::Configuration {
                    path: "dataset[].options.has_header".to_owned(),
                    message: "expected `true` or `false`".to_owned(),
                })?);
            }
            "delimiter" => {
                let bytes = value.as_bytes();
                if bytes.len() != 1 {
                    return Err(Error::Configuration {
                        path: "dataset[].options.delimiter".to_owned(),
                        message: "expected one ASCII byte".to_owned(),
                    });
                }
                parsed = parsed.delimiter(bytes[0]);
            }
            "schema_infer_max_records" => {
                parsed.schema_infer_max_records =
                    value.parse().map_err(|_| Error::Configuration {
                        path: "dataset[].options.schema_infer_max_records".to_owned(),
                        message: "expected a positive integer".to_owned(),
                    })?;
            }
            _ => {
                return Err(Error::Configuration {
                    path: format!("dataset[].options.{key}"),
                    message: "unsupported CSV option".to_owned(),
                });
            }
        }
    }
    Ok(parsed)
}

async fn execute_fixture_sql(context: &SessionContext, sql: &str) -> Result<()> {
    let statements = DFParser::parse_sql(sql)?;
    for statement in statements {
        context.sql(&statement.to_string()).await?.collect().await?;
    }
    Ok(())
}

pub fn write_ipc(path: &Path, schema: &Schema, batches: &[RecordBatch]) -> Result<()> {
    let temporary = path.with_extension("arrow.tmp");
    let file = File::create(&temporary).map_err(|source| Error::Artifact {
        path: temporary.clone(),
        source,
    })?;
    let mut writer = FileWriter::try_new(file, schema)?;
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    fs::rename(&temporary, path).map_err(|source| Error::Artifact {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

pub fn read_ipc(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).map_err(|source| Error::Artifact {
        path: path.to_path_buf(),
        source,
    })?;
    FileReader::try_new(file, None)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Error::from)
}

pub fn logical_hash(schema: &Schema, batches: &[RecordBatch]) -> Result<String> {
    let mut rows = encode_rows(batches).map_err(|message| Error::Dataset {
        table: "*".to_owned(),
        message,
    })?;
    rows.sort();
    let mut hasher = blake3::Hasher::new();
    hasher.update(format!("{schema:?}").as_bytes());
    for row in rows {
        hasher.update(&(row.len() as u64).to_le_bytes());
        hasher.update(&row);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builds_csv_and_parquet_fixtures() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let csv_path = temporary.path().join("people.csv");
        fs::write(&csv_path, "id,name\n1,Ada\n2,Grace\n").expect("write CSV fixture");

        let parquet_path = temporary.path().join("events.parquet");
        let source = SessionContext::new();
        source
            .sql("CREATE TABLE events AS VALUES (10, 'open'), (20, 'closed')")
            .await
            .expect("create source table")
            .collect()
            .await
            .expect("execute source table creation");
        source
            .sql(&format!(
                "COPY events TO '{}' STORED AS PARQUET",
                parquet_path.display()
            ))
            .await
            .expect("plan Parquet export")
            .collect()
            .await
            .expect("write Parquet fixture");

        let snapshot = SnapshotBuilder::new(temporary.path().join("snapshot"))
            .build(&[
                FixtureSource::Csv {
                    name: "people".to_owned(),
                    path: csv_path,
                    options: [("has_header".to_owned(), "true".to_owned())].into(),
                },
                FixtureSource::Parquet {
                    name: "events".to_owned(),
                    path: parquet_path,
                },
            ])
            .await
            .expect("build fixture snapshot");

        assert_eq!(snapshot.tables["people"].row_count, 2);
        assert_eq!(snapshot.tables["events"].row_count, 2);
    }
}
