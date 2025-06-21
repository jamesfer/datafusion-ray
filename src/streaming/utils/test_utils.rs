use std::ffi::OsStr;
use std::sync::Arc;
use local_ip_address::local_ip;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use crate::streaming::runtime::Runtime;
use crate::streaming::state::checkpoint_storage::FileSystemStateStorage;
use crate::streaming::state::file_system::TempdirFileSystemStorage;
use object_store::local::LocalFileSystem;

pub fn make_temp_dir(prefix: impl AsRef<OsStr>) -> Result<tempfile::TempDir, DataFusionError> {
    tempfile::Builder::new()
        .prefix(&prefix)
        .tempdir()
        .map_err(|e| internal_datafusion_err!("Failed to create temporary directory: {}", e))
}

pub async fn make_test_runtime() -> Result<Arc<Runtime>, DataFusionError> {
    let local_ip_address = local_ip()
        .map_err(|e| internal_datafusion_err!("Failed to get local IP address: {}", e))?;
    let local_file_system = TempdirFileSystemStorage::from_tempdir(
        make_temp_dir("test_runtime_local_fs")?
    );
    let remote_file_system_tempdir = make_temp_dir("test_runtime_remote_fs")?;
    let remote_object_store = Arc::new(
        LocalFileSystem::new_with_prefix(remote_file_system_tempdir.path())
            .map_err(|e| internal_datafusion_err!("Failed to create LocalFileSystem: {}", e))?
    );
    let remote_state_storage = Arc::new(FileSystemStateStorage::new(
        remote_object_store,
    ));

    let runtime = Runtime::start(
        local_ip_address,
        Arc::new(local_file_system),
        remote_state_storage,
    ).await?;
    Ok(Arc::new(runtime))
}
