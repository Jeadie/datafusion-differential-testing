//! Federation session extension for DataFusion differential testing.

use std::sync::Arc;

use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_differential_testing::{ResolvedParams, Result, SessionExtension};
use datafusion_federation::{default_analyzer_rules, FederatedQueryPlanner};

#[derive(Debug, Default)]
pub struct FederationExtension;

impl SessionExtension for FederationExtension {
    fn kind(&self) -> &'static str {
        "federation"
    }

    fn validate(&self, params: &ResolvedParams) -> Result<()> {
        if params.is_empty() {
            Ok(())
        } else {
            Err(datafusion_differential_testing::Error::Configuration {
                path: "session.extensions.federation.params".to_owned(),
                message: "the federation extension accepts no parameters".to_owned(),
            })
        }
    }

    fn apply(
        &self,
        builder: SessionStateBuilder,
        _params: &ResolvedParams,
    ) -> Result<SessionStateBuilder> {
        Ok(builder
            .with_analyzer_rules(default_analyzer_rules())
            .with_query_planner(Arc::new(FederatedQueryPlanner::new())))
    }
}

#[must_use]
pub const fn compiled_extension_kinds() -> &'static [&'static str] {
    &["federation"]
}
