use std::{collections::BTreeMap, sync::Arc};

use datafusion::execution::session_state::SessionStateBuilder;

use crate::{error::Error, model::ResolvedParams, Result};

pub trait SessionExtension: Send + Sync {
    fn kind(&self) -> &'static str;

    fn validate(&self, params: &ResolvedParams) -> Result<()>;

    fn apply(
        &self,
        builder: SessionStateBuilder,
        params: &ResolvedParams,
    ) -> Result<SessionStateBuilder>;
}

#[derive(Default)]
pub struct SessionExtensionRegistry {
    extensions: BTreeMap<String, Arc<dyn SessionExtension>>,
}

impl SessionExtensionRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, extension: Arc<dyn SessionExtension>) -> Result<()> {
        let kind = extension.kind().to_owned();
        if self.extensions.insert(kind.clone(), extension).is_some() {
            return Err(Error::Configuration {
                path: format!("extensions.{kind}"),
                message: "session extension is already registered".to_owned(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn get(&self, kind: &str) -> Option<&Arc<dyn SessionExtension>> {
        self.extensions.get(kind)
    }

    pub fn kinds(&self) -> impl Iterator<Item = &str> {
        self.extensions.keys().map(String::as_str)
    }
}
