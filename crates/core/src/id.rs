use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use crate::error::Error;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, Error> {
                let value = value.into();
                validate_id(stringify!($name), &value)?;
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }
    };
}

fn validate_id(kind: &'static str, value: &str) -> Result<(), Error> {
    let valid = !value.is_empty()
        && value.len() <= 128
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'));
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidId {
            kind,
            value: value.to_owned(),
        })
    }
}

id_type!(CampaignId);
id_type!(RunId);
id_type!(DatasetId);
id_type!(TableId);
id_type!(BatchId);
id_type!(QueryId);
id_type!(VariantId);
id_type!(FindingId);
id_type!(ProviderId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_reject_path_components() {
        assert!(CampaignId::new("campaign-1").is_ok());
        assert!(CampaignId::new("../campaign").is_err());
        assert!(CampaignId::new("").is_err());
    }
}
