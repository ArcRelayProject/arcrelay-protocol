#[cfg(feature = "host")]
pub mod clipboard_replication;
pub mod error;
pub mod message;
pub mod product;
#[cfg(feature = "host")]
pub mod proto_msg;
pub mod remote_files;
#[cfg(feature = "host")]
pub mod server;

#[cfg(test)]
mod product_contract_tests {
    use super::product::{CapabilityId, ProductInstallation, ProductManifest};

    #[test]
    fn built_in_suite_and_transfer_manifests_have_stable_interoperability_ids() {
        let suite = ProductManifest::suite();
        let transfer = ProductManifest::transfer();

        suite.validate().unwrap();
        transfer.validate().unwrap();
        assert_eq!(suite.product_id, "com.arcrelay.suite");
        assert_eq!(transfer.product_id, "com.arcrelay.transfer");
        assert_eq!(transfer.product_code, "xfer");
        assert!(suite.supports(CapabilityId::Transfer));
        assert!(transfer.supports(CapabilityId::Transfer));
        assert!(ProductManifest::for_product_id("com.arcrelay.unknown").is_none());
    }

    #[test]
    fn manifest_rejects_duplicate_modules_and_unsafe_identifiers() {
        let mut manifest = ProductManifest::transfer();
        manifest.included_modules.push(CapabilityId::Transfer);
        assert!(manifest.validate().is_err());

        let mut manifest = ProductManifest::transfer();
        manifest.product_id = "ArcRelay Transfer/../../".into();
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn installation_id_is_stable_and_cannot_cross_product_boundaries() {
        let directory = tempfile::tempdir().unwrap();
        let first =
            ProductInstallation::load_or_create(directory.path(), &ProductManifest::transfer())
                .unwrap();
        let second =
            ProductInstallation::load_or_create(directory.path(), &ProductManifest::transfer())
                .unwrap();

        assert_eq!(first, second);
        assert!(uuid::Uuid::parse_str(&first.installation_id).is_ok());
        assert!(
            ProductInstallation::load_or_create(directory.path(), &ProductManifest::suite(),)
                .is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn installation_identity_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        ProductInstallation::load_or_create(directory.path(), &ProductManifest::transfer())
            .unwrap();

        let directory_mode = std::fs::metadata(directory.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let identity_mode = std::fs::metadata(directory.path().join("product-installation.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(directory_mode, 0o700);
        assert_eq!(identity_mode, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn installation_identity_rejects_symlink_redirection() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let installation_directory = root.path().join("installation");
        std::fs::create_dir_all(&installation_directory).unwrap();
        let target = root.path().join("outside.json");
        std::fs::write(&target, b"do-not-touch").unwrap();
        symlink(
            &target,
            installation_directory.join("product-installation.json"),
        )
        .unwrap();

        assert!(ProductInstallation::load_or_create(
            &installation_directory,
            &ProductManifest::transfer(),
        )
        .is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"do-not-touch");
    }
}
