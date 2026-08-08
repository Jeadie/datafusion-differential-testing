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
use datafusion::{prelude::SessionContext, sql::parser::DFParser};

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
        fs::create_dir_all(&self.output_directory).map_err(|source| Error::Artifact {
            path: self.output_directory.clone(),
            source,
        })?;
        let context = SessionContext::new();
        let mut ipc_sources = Vec::new();
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
                    ipc_sources.push((name.clone(), path.clone()));
                }
            }
        }

        for (name, path) in ipc_sources {
            let batches = read_ipc(&path)?;
            let schema =
                batches
                    .first()
                    .map(RecordBatch::schema)
                    .ok_or_else(|| Error::Dataset {
                        table: name.clone(),
                        message: "Arrow IPC fixture contains no record batch".to_owned(),
                    })?;
            let table = datafusion::datasource::MemTable::try_new(schema, vec![batches])?;
            context.register_table(&name, Arc::new(table))?;
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
        if table_names.is_empty() {
            return Err(Error::Dataset {
                table: "*".to_owned(),
                message: "fixtures produced no tables".to_owned(),
            });
        }

        let mut tables = BTreeMap::new();
        for (index, name) in table_names.into_iter().enumerate() {
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
            let id = TableId::new(format!("table-{index:06}"))?;
            let ipc_path = self.output_directory.join(format!("{id}.arrow"));
            write_ipc(&ipc_path, schema.as_ref(), &batches)?;
            let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
            let content_hash = logical_hash(schema.as_ref(), &batches)?;
            tables.insert(
                name,
                TableSnapshot {
                    id,
                    schema,
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
