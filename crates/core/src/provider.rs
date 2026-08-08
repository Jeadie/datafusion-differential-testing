use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{catalog::TableProvider, physical_plan::SendableRecordBatchStream};

use crate::{
    error::{Error, Result},
    model::{ProviderLifecycle, RemoteTableRef, ResolvedParams, UnresolvedParams},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderIdentity {
    pub name: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredTable {
    pub remote: RemoteTableRef,
    pub logical_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableSelection {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoadReport {
    pub rows_loaded: u64,
}

#[async_trait]
pub trait ProviderFactory: Send + Sync {
    fn kind(&self) -> &'static str;

    fn validate(&self, params: &UnresolvedParams) -> Result<()>;

    async fn connect(
        &self,
        name: &str,
        params: &ResolvedParams,
        lifecycle: &ProviderLifecycle,
    ) -> Result<Arc<dyn ProviderInstance>>;
}

#[async_trait]
pub trait ProviderInstance: Send + Sync {
    fn identity(&self) -> ProviderIdentity;

    fn managed(&self) -> Option<&dyn ManagedProvider>;

    async fn table_exists(&self, table: &RemoteTableRef) -> Result<bool>;

    async fn discover(&self, selection: &TableSelection) -> Result<Vec<DiscoveredTable>>;

    async fn table_provider(&self, table: &RemoteTableRef) -> Result<Arc<dyn TableProvider>>;
}

#[async_trait]
pub trait ManagedProvider: Send + Sync {
    fn validate_schema(&self, schema: &arrow::datatypes::Schema) -> Result<()>;

    async fn table_exists(&self, table: &RemoteTableRef) -> Result<bool>;

    async fn create_table(
        &self,
        table: &RemoteTableRef,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<()>;

    async fn load(
        &self,
        table: &RemoteTableRef,
        batches: SendableRecordBatchStream,
    ) -> Result<LoadReport>;

    async fn drop_table(&self, table: &RemoteTableRef) -> Result<()>;
}

#[derive(Default)]
pub struct ProviderRegistry {
    factories: BTreeMap<String, Arc<dyn ProviderFactory>>,
}

impl ProviderRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, factory: Arc<dyn ProviderFactory>) -> Result<()> {
        let kind = factory.kind().to_owned();
        if self.factories.insert(kind.clone(), factory).is_some() {
            return Err(Error::Configuration {
                path: format!("providers.{kind}.type"),
                message: "provider factory is already registered".to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn get(&self, kind: &str) -> Option<&Arc<dyn ProviderFactory>> {
        self.factories.get(kind)
    }

    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.factories.keys().map(String::as_str)
    }
}
