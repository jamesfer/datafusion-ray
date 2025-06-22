use datafusion::common::{internal_datafusion_err, DataFusionError, Result};
use futures_util::TryFutureExt;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::file_system::FileSystemStorage;
use crate::streaming::state::operator_latest_json::OperatorLatestJson;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

pub struct FileSystemStateStorage {
    storage: ObjectStoreRef,
    operator_latest_json: OperatorLatestJson,
}

impl FileSystemStateStorage {
    pub fn new(storage: ObjectStoreRef) -> Self {
        let operator_latest_json = OperatorLatestJson::new(storage.clone());
        Self { storage, operator_latest_json }
    }

    pub async fn start_checkpoint_part(&self, operator_id: &str) -> Result<String> {
        // TODO one day this will need to add a pending entry to the metadata to prevent files
        //  being deleted while the checkpoint is in progress.
        let checkpoint_part_id = format!("checkpoint-part_{}", Uuid::new_v4().to_string());
        Ok(checkpoint_part_id)
    }

    // Returns an object store instance scoped to the checkpoint directory for the given operator and checkpoint part.
    pub fn get_scoped_file_system(&self, operator_id: &str, checkpoint_part_id: &str) -> ObjectStoreRef {
        let checkpoint_path = self.get_checkpoint_path(operator_id, checkpoint_part_id);
        Arc::new(PrefixStore::new(self.storage.clone(), checkpoint_path))
    }

    pub async fn complete_checkpoint(
        &self,
        operator_id: &str,
        checkpoint_id: &str,
        checkpoint_part_id: String,
        partition_range: PartitionRange,
    ) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::Internal(format!("Failed to get current time: {}", e)))?
            .as_millis() as u64;

        self.operator_latest_json.atomic_update_latest_json(operator_id, |mut latest| {
            latest.add_completed_checkpoint(checkpoint_id, checkpoint_part_id, timestamp, partition_range);
            Ok(latest)
        }).await?;

        Ok(())
    }

    pub async fn get_state_checkpoint_parts(
        &self,
        state_id: &str,
        partition_range: &PartitionRange,
        checkpoint_id: &str,
    ) -> Result<Vec<(String, PartitionRange)>> {
        let latest = self.operator_latest_json.read_latest_json(state_id).await?;
        latest.find_checkpoint_parts(checkpoint_id, partition_range)
            .ok_or(internal_datafusion_err!(
                "No viable checkpoints found for operator {} in range {:?}",
                state_id,
                partition_range
            ))
    }

    pub async fn get_latest_state_checkpoint_parts(
        &self,
        state_id: &str,
        partition_range: &PartitionRange,
    ) -> Result<(String, Vec<(String, PartitionRange)>)> {
        let latest = self.operator_latest_json.read_latest_json(state_id).await?;
        latest.completed_checkpoints.into_iter()
            // Search in reverse order to find the most recent checkpoint
            .rev()
            .find_map(|checkpoint_entry|
                checkpoint_entry.find_checkpoint_parts(partition_range)
                    .map(|parts| (checkpoint_entry.id, parts))
            )
            .ok_or(internal_datafusion_err!(
                "No viable checkpoints found for operator {} in range {:?}",
                state_id,
                partition_range
            ))
    }

    fn get_checkpoint_path(&self, state_id: &str, checkpoint_id: &str) -> Path {
        Path::from(format!(
            "operators/{}/checkpoints/{}",
            state_id, checkpoint_id
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_start_checkpoint() -> Result<()> {
        let (_temp_dir, checkpoint_storage) = create_temp_checkpoint_store()?;

        let _checkpoint_id = checkpoint_storage.start_checkpoint_part("operator1").await?;

        // This test just confirms that no error is thrown
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_checkpoint() -> Result<()> {
        let (_temp_dir, checkpoint_storage) = create_temp_checkpoint_store()?;
        let operator_id = "operator1";
        let checkpoint_id = "chk1";

        let checkpoint_part_id = checkpoint_storage.start_checkpoint_part(operator_id).await?;
        let partition_range = PartitionRange::new(0, 1, 10);
        
        // For the checkpoint directory, we'll use a mock path since we're testing the JSON metadata
        checkpoint_storage.complete_checkpoint(operator_id, checkpoint_id, checkpoint_part_id, partition_range).await?;

        let latest = checkpoint_storage.operator_latest_json.read_latest_json(operator_id).await?;
        
        assert_eq!(latest.completed_checkpoints.len(), 1);
        assert_eq!(latest.completed_checkpoints[0].partitions.len(), 1);
        assert_eq!(latest.completed_checkpoints[0].partitions[0].partition_range.start(), 0);
        assert_eq!(latest.completed_checkpoints[0].partitions[0].partition_range.end(), 1);
        
        Ok(())
    }

    fn create_temp_checkpoint_store() -> Result<(TempDir, FileSystemStateStorage), DataFusionError> {
        let temp_dir = TempDir::new()?;
        let local_fs = LocalFileSystem::new_with_prefix(temp_dir.path())
            .map_err(|e| DataFusionError::Internal(format!("Failed to create LocalFileSystem: {}", e)))?;
        let storage = Arc::new(local_fs) as ObjectStoreRef;
        let checkpoint_storage = FileSystemStateStorage::new(storage);
        Ok((temp_dir, checkpoint_storage))
    }
}
