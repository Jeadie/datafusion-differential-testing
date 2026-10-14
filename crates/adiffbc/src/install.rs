//! Installs missing ADBC drivers with Columnar's `dbc` CLI
//! (<https://docs.columnar.tech/dbc/>).

use std::{env, path::PathBuf, process::Command};

use serde::Deserialize;

use crate::spec::{Installer, VersionConstraint};

/// Tried in order when `--dbc` is not given.
const CANDIDATE_COMMANDS: [&[&str]; 3] = [&["dbc"], &["uvx", "dbc"], &["pipx", "run", "dbc"]];

/// One line of `dbc install --json` output.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", content = "payload")]
enum DbcEvent {
    #[serde(rename = "install.status")]
    Status(InstallStatus),
    #[serde(rename = "error")]
    Error(DbcError),
}

#[derive(Debug, Deserialize)]
pub struct InstallStatus {
    /// For example `installed` or `already installed`.
    pub status: String,
    pub driver: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
struct DbcError {
    message: String,
}

impl Installer {
    /// Run `dbc install` into [`Installer::directory`], or into a subdirectory
    /// per pinned version. Returns the path of the driver manifest it wrote.
    pub fn install(
        &self,
        name: &str,
        version: Option<&VersionConstraint>,
    ) -> Result<(PathBuf, InstallStatus), String> {
        let (package, directory) = match version {
            Some(version) => (
                version.package(name),
                self.directory.join(version.directory_name(name)),
            ),
            None => (name.to_owned(), self.directory.clone()),
        };
        let command = match &self.command {
            Some(command) => command.clone(),
            None => find_dbc().ok_or_else(|| {
                format!(
                    "driver `{package}` is not installed and `dbc` was not found on PATH. \
                     Install dbc (https://docs.columnar.tech/dbc/getting_started/installation/), \
                     pass `--dbc COMMAND`, or install the driver yourself"
                )
            })?,
        };
        let (program, arguments) = command
            .split_first()
            .ok_or("install.command must not be empty")?;
        std::fs::create_dir_all(&directory)
            .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
        eprintln!(
            "installing ADBC driver `{package}` into {} with `{}`",
            directory.display(),
            command.join(" ")
        );
        // dbc installs into `ADBC_DRIVER_PATH` when it is set and `--level` is not.
        let output = Command::new(program)
            .args(arguments)
            .args(["install", "--json", package.as_str()])
            .env("ADBC_DRIVER_PATH", &directory)
            .output()
            .map_err(|error| format!("cannot run `{}`: {error}", command.join(" ")))?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let event = stdout
            .lines()
            .rev()
            .find_map(|line| serde_json::from_str::<DbcEvent>(line).ok());
        match event {
            Some(DbcEvent::Status(status)) if output.status.success() => {
                eprintln!("{} {}: {}", status.driver, status.version, status.status);
                // dbc names the manifest after the driver it resolved.
                let manifest = directory.join(format!("{}.toml", status.driver));
                Ok((manifest, status))
            }
            Some(DbcEvent::Error(error)) => {
                Err(format!("`dbc install {package}` failed: {}", error.message))
            }
            _ => Err(format!(
                "`dbc install {package}` failed ({}): {}{}",
                output.status,
                stdout.trim(),
                String::from_utf8_lossy(&output.stderr).trim()
            )),
        }
    }
}

fn find_dbc() -> Option<Vec<String>> {
    CANDIDATE_COMMANDS
        .iter()
        .find(|command| on_path(command[0]))
        .map(|command| command.iter().map(|part| (*part).to_owned()).collect())
}

fn on_path(program: &str) -> bool {
    env::var_os("PATH").is_some_and(|paths| {
        env::split_paths(&paths).any(|directory| directory.join(program).is_file())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dbc_json_events() {
        let installed = r#"{"schema_version":1,"kind":"install.status","payload":{"status":"installed","driver":"sqlite","version":"1.12.0","location":"/x","checksum":"ab"}}"#;
        let Ok(DbcEvent::Status(status)) = serde_json::from_str(installed) else {
            panic!("expected an install status");
        };
        assert_eq!(
            (status.status.as_str(), status.version.as_str()),
            ("installed", "1.12.0")
        );
        let failed = r#"{"schema_version":1,"kind":"error","payload":{"code":"install_failed","message":"not found"}}"#;
        assert!(matches!(
            serde_json::from_str(failed),
            Ok(DbcEvent::Error(DbcError { message })) if message == "not found"
        ));
    }
}
