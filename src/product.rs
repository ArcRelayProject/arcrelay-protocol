use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{ProtocolError, Result};

pub const PRODUCT_MANIFEST_SCHEMA_VERSION: u16 = 1;
pub const PRODUCT_INSTALLATION_SCHEMA_VERSION: u16 = 1;
const INSTALLATION_FILE: &str = "product-installation.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityId {
    Mouse,
    Transfer,
    Clipboard,
    Monitor,
    Privacy,
    Automate,
    Print,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationPolicy {
    Always,
    OnDemand,
    OnInterest,
    Scheduled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProductManifest {
    pub schema_version: u16,
    pub product_id: String,
    pub product_code: String,
    pub display_name: String,
    pub data_namespace: String,
    pub included_modules: Vec<CapabilityId>,
    pub enabled_ui_routes: Vec<String>,
    pub default_activation_policy: BTreeMap<CapabilityId, ActivationPolicy>,
}

impl ProductManifest {
    #[must_use]
    pub fn suite() -> Self {
        built_in_manifest(include_str!("../manifests/arcrelay-suite.json"))
    }

    #[must_use]
    pub fn transfer() -> Self {
        built_in_manifest(include_str!("../manifests/arcrelay-transfer.json"))
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != PRODUCT_MANIFEST_SCHEMA_VERSION {
            return Err(product_error("unsupported product manifest schema"));
        }
        if !valid_product_id(&self.product_id) {
            return Err(product_error("invalid product id"));
        }
        if !valid_short_identifier(&self.product_code, 8) {
            return Err(product_error("invalid product code"));
        }
        if self.display_name.trim().is_empty()
            || self.display_name.chars().count() > 64
            || self.display_name.chars().any(char::is_control)
        {
            return Err(product_error("invalid product display name"));
        }
        if !valid_short_identifier(&self.data_namespace, 64) {
            return Err(product_error("invalid data namespace"));
        }
        if self.included_modules.is_empty() || self.included_modules.len() > 32 {
            return Err(product_error("product must contain 1 to 32 modules"));
        }
        let modules = self
            .included_modules
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if modules.len() != self.included_modules.len() {
            return Err(product_error("product modules must be unique"));
        }
        if self
            .default_activation_policy
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            != modules
        {
            return Err(product_error(
                "every product module must have exactly one activation policy",
            ));
        }
        if self.enabled_ui_routes.len() > 32
            || self
                .enabled_ui_routes
                .iter()
                .any(|route| !valid_short_identifier(route, 48))
            || self.enabled_ui_routes.iter().collect::<BTreeSet<_>>().len()
                != self.enabled_ui_routes.len()
        {
            return Err(product_error("invalid or duplicate product UI route"));
        }
        Ok(())
    }

    #[must_use]
    pub fn supports(&self, capability: CapabilityId) -> bool {
        self.included_modules.contains(&capability)
    }

    #[must_use]
    pub fn product_id_for_code(product_code: &str) -> Option<&'static str> {
        match product_code {
            "suite" => Some("com.arcrelay.suite"),
            "xfer" => Some("com.arcrelay.transfer"),
            _ => None,
        }
    }

    #[must_use]
    pub fn for_product_id(product_id: &str) -> Option<Self> {
        match product_id {
            "com.arcrelay.suite" => Some(Self::suite()),
            "com.arcrelay.transfer" => Some(Self::transfer()),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProductInstallation {
    pub schema_version: u16,
    pub product_id: String,
    pub installation_id: String,
    pub created_at_ms: i64,
}

impl ProductInstallation {
    pub fn load_or_create(directory: &Path, manifest: &ProductManifest) -> Result<Self> {
        manifest.validate()?;
        std::fs::create_dir_all(directory)?;
        harden_directory(directory)?;
        let path = directory.join(INSTALLATION_FILE);
        if path.exists() {
            return load_installation(&path, manifest);
        }

        let installation = Self {
            schema_version: PRODUCT_INSTALLATION_SCHEMA_VERSION,
            product_id: manifest.product_id.clone(),
            installation_id: uuid::Uuid::new_v4().to_string(),
            created_at_ms: now_ms(),
        };
        let bytes = serde_json::to_vec_pretty(&installation)?;
        match create_private_file(&path) {
            Ok(mut file) => {
                file.write_all(&bytes)?;
                file.sync_all()?;
                harden_private_file(&path)?;
                Ok(installation)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                load_installation(&path, manifest)
            }
            Err(error) => Err(error.into()),
        }
    }
}

fn load_installation(path: &Path, manifest: &ProductManifest) -> Result<ProductInstallation> {
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(product_error(
            "product installation identity cannot be a symlink",
        ));
    }
    let installation: ProductInstallation = serde_json::from_slice(&std::fs::read(path)?)?;
    if installation.schema_version != PRODUCT_INSTALLATION_SCHEMA_VERSION
        || installation.product_id != manifest.product_id
        || !is_uuid_v4(&installation.installation_id)
        || installation.created_at_ms <= 0
    {
        return Err(product_error(
            "product installation identity does not match this product",
        ));
    }
    harden_private_file(path)?;
    Ok(installation)
}

fn create_private_file(path: &PathBuf) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

fn harden_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn harden_private_file(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn valid_product_id(value: &str) -> bool {
    value.len() <= 64
        && value.split('.').count() >= 3
        && value.split('.').all(|segment| {
            !segment.is_empty()
                && segment.len() <= 32
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
                && segment
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && segment
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
        })
}

fn valid_short_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn is_uuid_v4(value: &str) -> bool {
    uuid::Uuid::parse_str(value)
        .ok()
        .is_some_and(|uuid| uuid.get_version() == Some(uuid::Version::Random))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn product_error(message: impl Into<String>) -> ProtocolError {
    ProtocolError::Other(format!("product context error: {}", message.into()))
}

fn built_in_manifest(json: &str) -> ProductManifest {
    let manifest: ProductManifest =
        serde_json::from_str(json).expect("built-in product manifest must be valid JSON");
    manifest
        .validate()
        .expect("built-in product manifest must satisfy the runtime contract");
    manifest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_invalid(mutator: impl FnOnce(&mut ProductManifest)) {
        let mut manifest = ProductManifest::transfer();
        mutator(&mut manifest);
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn product_code_and_id_lookups_are_bidirectional_for_built_ins() {
        for (code, id) in [
            ("suite", "com.arcrelay.suite"),
            ("xfer", "com.arcrelay.transfer"),
        ] {
            assert_eq!(ProductManifest::product_id_for_code(code), Some(id));
            assert_eq!(
                ProductManifest::for_product_id(id).unwrap().product_code,
                code
            );
        }
        assert_eq!(ProductManifest::product_id_for_code("unknown"), None);
        assert_eq!(
            ProductManifest::for_product_id("com.arcrelay.unknown"),
            None
        );
    }

    #[test]
    fn manifest_validation_rejects_every_invalid_field_shape() {
        assert_invalid(|manifest| manifest.schema_version += 1);
        assert_invalid(|manifest| manifest.product_id = "only.two".into());
        assert_invalid(|manifest| manifest.product_id = "com.-arcrelay.transfer".into());
        assert_invalid(|manifest| manifest.product_code = "Too_Long!".into());
        assert_invalid(|manifest| manifest.display_name = " \n".into());
        assert_invalid(|manifest| manifest.display_name = "x".repeat(65));
        assert_invalid(|manifest| manifest.display_name = "bad\0name".into());
        assert_invalid(|manifest| manifest.data_namespace = "-namespace".into());
        assert_invalid(|manifest| manifest.included_modules.clear());
        assert_invalid(|manifest| manifest.included_modules = vec![CapabilityId::Transfer; 33]);
        assert_invalid(|manifest| manifest.included_modules.push(CapabilityId::Transfer));
        assert_invalid(|manifest| {
            manifest.default_activation_policy.clear();
        });
        assert_invalid(|manifest| manifest.enabled_ui_routes = vec!["transfer".into(); 2]);
        assert_invalid(|manifest| manifest.enabled_ui_routes = vec!["Bad Route".into()]);
        assert_invalid(|manifest| {
            manifest.enabled_ui_routes = (0..33).map(|i| format!("r{i}")).collect()
        });
    }

    #[test]
    fn loading_rejects_malformed_or_tampered_installation_files() {
        let manifest = ProductManifest::transfer();
        for contents in [
            "not json".to_string(),
            serde_json::json!({
                "schemaVersion": PRODUCT_INSTALLATION_SCHEMA_VERSION + 1,
                "productId": manifest.product_id,
                "installationId": uuid::Uuid::new_v4().to_string(),
                "createdAtMs": 1
            })
            .to_string(),
            serde_json::json!({
                "schemaVersion": PRODUCT_INSTALLATION_SCHEMA_VERSION,
                "productId": manifest.product_id,
                "installationId": uuid::Uuid::nil().to_string(),
                "createdAtMs": 1
            })
            .to_string(),
            serde_json::json!({
                "schemaVersion": PRODUCT_INSTALLATION_SCHEMA_VERSION,
                "productId": manifest.product_id,
                "installationId": uuid::Uuid::new_v4().to_string(),
                "createdAtMs": 0
            })
            .to_string(),
        ] {
            let directory = tempfile::tempdir().unwrap();
            std::fs::write(directory.path().join(INSTALLATION_FILE), contents).unwrap();
            assert!(ProductInstallation::load_or_create(directory.path(), &manifest).is_err());
        }
    }

    #[test]
    fn installation_creation_rejects_an_invalid_manifest_without_writing() {
        let directory = tempfile::tempdir().unwrap();
        let mut manifest = ProductManifest::transfer();
        manifest.product_code.clear();

        assert!(ProductInstallation::load_or_create(directory.path(), &manifest).is_err());
        assert!(!directory.path().join(INSTALLATION_FILE).exists());
    }
}
