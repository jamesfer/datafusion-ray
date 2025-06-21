use async_trait::async_trait;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use futures_util::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use tokio_stream::wrappers::ReadDirStream;

#[derive(Debug, Clone)]
pub struct FileSystemEntry {
    pub name: OsString,
    pub directory: bool,
}

#[async_trait]
pub trait FileSystemStorage {
    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<(), DataFusionError>;
    async fn read_file(&self, path: &Path) -> Result<Vec<u8>, DataFusionError>;
    async fn list_files(&self, path: &Path) -> Result<Vec<FileSystemEntry>, DataFusionError>;
    async fn copy(&self, src: &Path, dst: &Path) -> Result<(), DataFusionError>;
    async fn remove_dir_all(&self, path: &Path) -> Result<(), DataFusionError>;
    async fn mkdir_all(&self, path: &Path) -> Result<(), DataFusionError>;
    fn get_physical_path(&self, path: &Path) -> Result<PathBuf, DataFusionError>;
}

pub struct PrefixedLocalFileSystemStorage {
    dir: OsString,
}

impl PrefixedLocalFileSystemStorage {
    pub fn new(dir: impl Into<OsString>) -> Self {
        Self { dir: dir.into() }
    }

    fn prefixed_path(&self, path: impl AsRef<Path>) -> PathBuf {
        Path::new(&self.dir).join(&path)
    }
}

#[async_trait]
impl FileSystemStorage for PrefixedLocalFileSystemStorage {
    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<(), DataFusionError> {
        tokio::fs::write(self.prefixed_path(path), contents).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to write file: {}", e)))
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>, DataFusionError> {
        tokio::fs::read(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to read file: {}", e)))
    }

    async fn list_files(&self, path: &Path) -> Result<Vec<FileSystemEntry>, DataFusionError> {
        let stream = ReadDirStream::new(tokio::fs::read_dir(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to read dir: {}", e)))?);

        let stream = stream
            .map_err(|err| internal_datafusion_err!("Error when reading directory: {}", err))
            .flat_map(|result| futures::stream::once(async {
                match result {
                    Err(err) => Err(err),
                    Ok(entry) => {
                        Ok(FileSystemEntry {
                            name: entry.file_name(),
                            directory: entry.file_type().await.map(|ft| ft.is_dir()).unwrap_or_else(|_| false),
                        })
                    }
                }
            }))
            .try_collect::<Vec<_>>().await?;

        Ok(stream)
    }

    async fn copy(&self, src: &Path, dst: &Path) -> Result<(), DataFusionError> {
        copy(Path::new(&self.dir), src, dst).await
    }

    async fn remove_dir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        tokio::fs::remove_dir_all(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to remove directory: {}", e)))
    }

    async fn mkdir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        tokio::fs::create_dir_all(self.prefixed_path(path)).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to create directory: {}", e)))
    }

    fn get_physical_path(&self, path: &Path) -> Result<PathBuf, DataFusionError> {
        Ok(self.prefixed_path(path))
    }
}

async fn copy(root_dir: &Path, src: &Path, dst: &Path) -> Result<(), DataFusionError> {
    // Copy individual files
    let absolute_src_path = Path::new(&root_dir).join(&src);
    let absolute_dst_path = Path::new(&root_dir).join(&dst);
    if !absolute_src_path.is_dir() {
        tokio::fs::copy(absolute_src_path, absolute_dst_path).await
            .map_err(|e| DataFusionError::Execution(format!("Failed to copy file: {}", e)))?;
        return Ok(());
    }

    // Copy directories recursively
    tokio::fs::create_dir_all(absolute_dst_path).await
        .map_err(|e| DataFusionError::Execution(format!("Failed to create directory: {}", e)))?;
    let mut entries = tokio::fs::read_dir(absolute_src_path).await
        .map_err(|e| DataFusionError::Execution(format!("Failed to read directory: {}", e)))?;
    while let Some(entry) = entries.next_entry().await
        .map_err(|e| DataFusionError::Execution(format!("Failed to read directory entry: {}", e)))? {
        let file_name = entry.file_name();
        let src_path = Path::new(src).join(&file_name);
        let dst_path = Path::new(dst).join(&file_name);
        Box::pin(copy(&root_dir, &src_path, &dst_path)).await?;
    }
    Ok(())
}

pub struct TempdirFileSystemStorage {
    dir: tempfile::TempDir,
    inner: PrefixedLocalFileSystemStorage,
}

impl TempdirFileSystemStorage {
    pub fn from_tempdir(dir: tempfile::TempDir) -> Self {
        Self {
            inner: PrefixedLocalFileSystemStorage::new(dir.path()),
            dir,
        }
    }
}

#[async_trait]
impl FileSystemStorage for TempdirFileSystemStorage {
    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<(), DataFusionError> {
        self.inner.write_file(path, contents).await
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>, DataFusionError> {
        self.inner.read_file(path).await
    }

    async fn list_files(&self, path: &Path) -> Result<Vec<FileSystemEntry>, DataFusionError> {
        self.inner.list_files(path).await
    }

    async fn copy(&self, src: &Path, dst: &Path) -> Result<(), DataFusionError> {
        self.inner.copy(src, dst).await
    }

    async fn remove_dir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        self.inner.remove_dir_all(path).await
    }

    async fn mkdir_all(&self, path: &Path) -> Result<(), DataFusionError> {
        self.inner.mkdir_all(path).await
    }

    fn get_physical_path(&self, path: &Path) -> Result<PathBuf, DataFusionError> {
        self.inner.get_physical_path(path)
    }
}
