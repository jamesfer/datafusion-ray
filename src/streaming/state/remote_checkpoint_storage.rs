use datafusion::common::{internal_datafusion_err, DataFusionError, Result};
use futures_util::TryFutureExt;
use object_store::prefix::PrefixStore;
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::file_structure_constants::{get_pipeline_latest_json_path, get_remote_state_dir_path, make_checkpoint_part_id};
use crate::streaming::state::file_system::FileSystemStorage;
use crate::streaming::state::latest_checkpoint_model_2::{CheckpointPart, LatestCheckpoints, OperatorFlowGraph, ParentState};
use crate::streaming::state::object_file_utils::LazyFileObject;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

pub struct RemoteCheckpointStorage {
    storage: ObjectStoreRef,
    latest_json: LazyFileObject<LatestCheckpoints>,
}

impl RemoteCheckpointStorage {
    pub fn new(storage: ObjectStoreRef) -> Self {
        let latest_json = LazyFileObject::new(
            storage.clone(),
            get_pipeline_latest_json_path(),
        );
        Self { storage, latest_json }
    }

    pub async fn start_checkpoint_part(&self, operator_id: &str) -> Result<String> {
        // TODO one day this will need to add a pending entry to the metadata to prevent files
        //  being deleted while the checkpoint is in progress.
        Ok(make_checkpoint_part_id())
    }

    // Returns an object store instance scoped to the directory where state files can be stored.
    // It is up to the particular state backend to organise the directory structure to support state
    // from different checkpoints.
    pub fn get_state_file_system(&self, operator_id: &str, state_id: &str) -> ObjectStoreRef {
        let checkpoint_path = get_remote_state_dir_path(operator_id, state_id);
        Arc::new(PrefixStore::new(self.storage.clone(), checkpoint_path))
    }

    pub async fn complete_checkpoint(
        &self,
        operator_id: String,
        checkpoint_id: String,
        partition_range: PartitionRange,
        parents: Vec<(usize, ParentState)>,
        states_refs: HashMap<String, String>,
    ) -> Result<()> {
        let completed_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DataFusionError::Internal(format!("Failed to get current time: {}", e)))?
            .as_millis() as u64;
        let part = CheckpointPart {
            checkpoint_id,
            operator_id,
            partition_range,
            completed_timestamp,
            parents,
            states_refs,
        };

        self.latest_json.atomic_update(|latest| {
            let mut latest = latest.unwrap_or_default();
            latest.add_checkpoint_part(part.clone());
            Ok(latest)
        }).await
    }

    pub async fn get_operator_checkpoint_parts(
        &self,
        operator_id: &str,
        partition_range: &PartitionRange,
        checkpoint_id: &str,
    ) -> Result<Option<Vec<(&CheckpointPart, PartitionRange)>>> {
        let latest = match self.latest_json.read_from_json().await? {
            None => return Ok(None),
            Some(latest) => latest,
        };
        Ok(latest.get_checkpoint(checkpoint_id, operator_id, partition_range))
    }

    pub async fn get_latest_completed_checkpoint(&self, operator_flow_graph: OperatorFlowGraph) -> Result<Option<&str>> {
        let latest = match self.latest_json.read_from_json().await? {
            None => return Ok(None),
            Some(latest) => latest,
        };
        latest.latest_complete_checkpoint(operator_flow_graph)
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

    fn create_temp_checkpoint_store() -> Result<(TempDir, RemoteCheckpointStorage), DataFusionError> {
        let temp_dir = TempDir::new()?;
        let local_fs = LocalFileSystem::new_with_prefix(temp_dir.path())
            .map_err(|e| DataFusionError::Internal(format!("Failed to create LocalFileSystem: {}", e)))?;
        let storage = Arc::new(local_fs) as ObjectStoreRef;
        let checkpoint_storage = RemoteCheckpointStorage::new(storage);
        Ok((temp_dir, checkpoint_storage))
    }
}
