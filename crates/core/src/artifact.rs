use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use crate::{
    error::{Error, Result},
    DatasetManifest, DatasetSnapshot, Finding, RunManifest, TableManifest,
};

pub trait ArtifactStore: Send + Sync {
    fn root(&self) -> &Path;
    fn initialize(&self, manifest: &RunManifest) -> Result<()>;
    fn write_dataset_manifest(&self, snapshot: &DatasetSnapshot) -> Result<()>;
    fn write_finding(&self, finding: &Finding) -> Result<PathBuf>;
    fn write_summary(&self, summary: &serde_json::Value) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct FileArtifactStore {
    root: PathBuf,
}

impl FileArtifactStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl ArtifactStore for FileArtifactStore {
    fn root(&self) -> &Path {
        &self.root
    }

    fn initialize(&self, manifest: &RunManifest) -> Result<()> {
        if let Some(parent) = self.root.parent() {
            fs::create_dir_all(parent).map_err(|source| Error::Artifact {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        create_exclusive(&self.root)?;
        fs::create_dir(self.root.join("findings")).map_err(|source| Error::Artifact {
            path: self.root.join("findings"),
            source,
        })?;
        atomic_json(&self.root.join("run.json"), manifest)
    }

    fn write_dataset_manifest(&self, snapshot: &DatasetSnapshot) -> Result<()> {
        let tables = snapshot
            .tables
            .iter()
            .map(|(name, table)| {
                (
                    name.clone(),
                    TableManifest {
                        row_count: table.row_count,
                        content_hash: table.content_hash.clone(),
                        ipc_path: table.ipc_path.clone(),
                    },
                )
            })
            .collect();
        atomic_json(
            &self.root.join("dataset-manifest.json"),
            &DatasetManifest {
                id: snapshot.id.clone(),
                tables,
            },
        )
    }

    fn write_finding(&self, finding: &Finding) -> Result<PathBuf> {
        let path = self
            .root
            .join("findings")
            .join(format!("{}.json", finding.id));
        atomic_json(&path, finding)?;
        Ok(path)
    }

    fn write_summary(&self, summary: &serde_json::Value) -> Result<()> {
        atomic_json(&self.root.join("summary.json"), summary)
    }
}

/// An [`ArtifactStore`] that discards everything. Useful for library use where
/// findings are consumed from the returned [`RunReport`](crate::RunReport)
/// rather than from disk.
#[derive(Debug, Clone)]
pub struct NullArtifactStore {
    root: PathBuf,
}

impl NullArtifactStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl ArtifactStore for NullArtifactStore {
    fn root(&self) -> &Path {
        &self.root
    }

    fn initialize(&self, _manifest: &RunManifest) -> Result<()> {
        Ok(())
    }

    fn write_dataset_manifest(&self, _snapshot: &DatasetSnapshot) -> Result<()> {
        Ok(())
    }

    fn write_finding(&self, finding: &Finding) -> Result<PathBuf> {
        Ok(self.root.join("findings").join(format!("{}.json", finding.id)))
    }

    fn write_summary(&self, _summary: &serde_json::Value) -> Result<()> {
        Ok(())
    }
}

fn create_exclusive(path: &Path) -> Result<()> {
    fs::create_dir(path).map_err(|source| Error::Artifact {
        path: path.to_path_buf(),
        source,
    })
}

fn atomic_json<T: serde::Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    let temporary = path.with_extension("json.tmp");
    let mut file = File::create(&temporary).map_err(|source| Error::Artifact {
        path: temporary.clone(),
        source,
    })?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n").map_err(|source| Error::Artifact {
        path: temporary.clone(),
        source,
    })?;
    file.sync_all().map_err(|source| Error::Artifact {
        path: temporary.clone(),
        source,
    })?;
    fs::rename(&temporary, path).map_err(|source| Error::Artifact {
        path: path.to_path_buf(),
        source,
    })
}
