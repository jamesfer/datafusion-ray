use std::borrow::Borrow;
use bytes::Bytes;
use object_store::{path::Path, Error as ObjectStoreError, PutMode, UpdateVersion};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use crate::streaming::state::remote_checkpoint_storage::ObjectStoreRef;

pub async fn read_file(storage: &ObjectStoreRef, path: &Path) -> Result<Option<Bytes>, DataFusionError> {
    match storage.get(&path).await {
        Ok(result) => {
            let content = result.bytes().await
                .map_err(|e| internal_datafusion_err!("Failed to get file bytes: {}", e))?;
            Ok(Some(content))
        }
        Err(ObjectStoreError::NotFound { .. }) => Ok(None),
        Err(err) => Err(DataFusionError::Execution(format!("Failed to read file: {}", err)))
    }
}

/// Atomically update a file using optimistic concurrency control with ETags.
///
/// This method:
/// 1. Reads the current file and its ETag
/// 2. Calls the provided callback with the content
/// 3. Conditionally writes the modified content back only if the ETag matches
/// 4. Loops on ETag conflicts until successful or non-conflict error occurs
/// 5. Returns Ok(()) on success, or Err on non-conflict failures
pub async fn atomic_update_file<F>(
    storage: &ObjectStoreRef,
    path: &Path,
    mut callback: F,
) -> Result<(), DataFusionError>
where
    F: FnMut(Option<Bytes>) -> Result<Bytes, DataFusionError>,
{
    loop {
        // Read current state with ETag
        let current_file = match storage.get(path).await {
            Ok(result) => {
                let version = result.meta.version.clone();
                let e_tag = match &result.meta.e_tag {
                    None => {
                        return Err(DataFusionError::Internal("ETag is missing".to_string()));
                    }
                    Some(e_tag) => e_tag.clone()
                };

                let content = result.bytes().await
                    .map_err(|e| internal_datafusion_err!("Failed to get file bytes: {}", e))?;
                Some((content, e_tag, version))
            }
            Err(ObjectStoreError::NotFound { .. }) => {
                // File doesn't exist, use empty content with no ETag
                None
            }
            Err(err) => {
                return Err(DataFusionError::Execution(format!("Failed to read file: {}", err)));
            }
        };

        let (updated_content, original_file_meta) = match current_file {
            Some((content, e_tag, version)) => {
                // File existed
                let new_content = callback(Some(content))?;
                (new_content, Some((e_tag, version)))
            }
            None => {
                // File didn't exist, pass None to callback
                let new_content = callback(None)?;
                (new_content, None)
            }
        };

        // Conditionally write back using ETag
        let put_result = match (updated_content, original_file_meta) {
            (updated_content, Some((e_tag, version))) => {
                // Original file existed, use conditional update
                storage.put_opts(
                    path,
                    updated_content.into(),
                    PutMode::Update(UpdateVersion {
                        e_tag: Some(e_tag),
                        version,
                    }).into(),
                ).await
            }
            (updated_content, None) => {
                // File doesn't exist, use conditional create
                storage.put_opts(path, updated_content.into(), PutMode::Create.into())
                    .await
            }
        };

        match put_result {
            Ok(_) => {
                return Ok(()); // Update succeeded
            }
            Err(object_store::Error::Precondition { .. }) => {
                // ETag mismatch or file already exists, loop to retry
                continue;
            }
            Err(e) => {
                // Other error, return immediately
                return Err(DataFusionError::Internal(format!("Failed to write JSON: {}", e)));
            }
        }
    }
}

pub struct LazyFileObject<T> {
    storage: ObjectStoreRef,
    pub path: Path,
}

impl <T> LazyFileObject<T> {
    pub fn new(storage: ObjectStoreRef, path: Path) -> Self {
        Self { storage, path }
    }

    pub async fn read_from_json(&self) -> Result<Option<T>, DataFusionError>
    where T: serde::Deserialize
    {
        match read_file(&self.storage, &self.path).await? {
            None => {
                Ok(None)
            }
            Some(content) => {
                serde_json::from_slice(content.as_ref())
                    .map(Some)
                    .map_err(|e| {
                        internal_datafusion_err!("Failed to parse JSON: {}", e)
                    })
            }
        }
    }

    // Update the file optimistically
    pub async fn atomic_update<F>(&self, mut callback: F) -> Result<(), DataFusionError>
    where
        F: FnMut(Option<T>) -> Result<T, DataFusionError>,
        T: serde::Deserialize + serde::Serialize,
    {
        atomic_update_file(
            &self.storage,
            &self.path,
            |current_content| {
                let current = current_content
                    .map(|bytes| serde_json::from_slice(bytes.as_ref()).map_err(|e| {
                        DataFusionError::Internal(format!("Failed to parse JSON: {}", e))
                    }))
                    .transpose()?;
                let updated = callback(current)?;

                // Serialize the updated content
                let json_content = serde_json::to_vec_pretty(&updated).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to serialize JSON: {}", e))
                })?;

                Ok(Bytes::from(json_content))
            },
        ).await
    }
}
