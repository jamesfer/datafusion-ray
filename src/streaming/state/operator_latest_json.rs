use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::checkpoint_storage::ObjectStoreRef;
use crate::streaming::state::object_file_utils::{atomic_update_file, read_file};
use bytes::Bytes;
use datafusion::common::DataFusionError;
use object_store::path::Path;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub partition_range: PartitionRange,
    pub timestamp: u64,
    pub checkpoint_part_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointEntry {
    pub id: String,
    pub partitions: Vec<PartitionInfo>,
}

impl CheckpointEntry {
    // Returns a vec of (checkpoint_part_id, partition_range) tuples
    pub fn find_checkpoint_parts(&self, partition_range: &PartitionRange) -> Option<Vec<(String, PartitionRange)>> {
        // Of all the available checkpoint parts, find which ones we need to cover the partition range
        let partition_ranges = self.partitions
            .iter()
            .map(|partition_info| partition_info.partition_range.clone())
            .collect::<Vec<_>>();
        let intersecting_partitions = partition_range.find_covering_partitions(&partition_ranges)?;

        let partition_infos = intersecting_partitions.into_iter()
            .map(|overlap| (
                self.partitions[overlap.index].checkpoint_part_id.clone(),
                overlap.selected_partition_range,
            ))
            .collect::<Vec<_>>();
        Some(partition_infos)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestCheckpoints {
    pub completed_checkpoints: Vec<CheckpointEntry>,
}

impl LatestCheckpoints {
    // Returns a vec of (checkpoint_part_id, partition_range) tuples
    pub fn find_checkpoint_parts(&self, checkpoint_id: &str, partition_range: &PartitionRange) -> Option<Vec<(String, PartitionRange)>> {
        // Find the checkpoint matching the given id
        let checkpoint_details = self.completed_checkpoints.iter()
            .find(|checkpoint_entry| &checkpoint_entry.id == checkpoint_id)?;
        checkpoint_details.find_checkpoint_parts(partition_range)
    }

    pub fn add_completed_checkpoint(
        &mut self,
        checkpoint_id: &str,
        checkpoint_part_id: String,
        timestamp: u64,
        partition_range: PartitionRange,
    ) {
        let info = PartitionInfo {
            partition_range,
            timestamp,
            checkpoint_part_id,
        };

        let existing_checkpoint = self
            .completed_checkpoints
            .iter_mut()
            .find(|entry| entry.id == checkpoint_id);
        if let Some(existing_entry) = existing_checkpoint {
            existing_entry.partitions.push(info);
        } else {
            let new_entry = CheckpointEntry {
                id: checkpoint_id.to_string(),
                partitions: vec![info],
            };
            self.completed_checkpoints.push(new_entry);
        }
    }
}

pub struct OperatorLatestJson {
    storage: ObjectStoreRef,
}

impl OperatorLatestJson {
    pub fn new(storage: ObjectStoreRef) -> Self {
        Self { storage }
    }

    pub async fn read_latest_json(&self, state_id: &str) -> datafusion::common::Result<LatestCheckpoints> {
        let path = self.get_latest_json_path(state_id);
        match read_file(&self.storage, &path).await? {
            None => {
                Ok(LatestCheckpoints {
                    completed_checkpoints: Vec::new(),
                })
            }
            Some(content) => {
                serde_json::from_slice(content.as_ref()).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to parse JSON: {}", e))
                })
            }
        }
    }

    /// Atomically update the latest.json file using optimistic concurrency control with ETags.
    pub async fn atomic_update_latest_json<F>(
        &self,
        state_id: &str,
        mut callback: F,
    ) -> datafusion::common::Result<()>
    where
        F: FnMut(LatestCheckpoints) -> Result<LatestCheckpoints, DataFusionError>,
    {
        let latest_path = self.get_latest_json_path(state_id);
        atomic_update_file(
            &self.storage,
            &latest_path,
            |current_content| {
                let current_latest = current_content
                    .map(|bytes| serde_json::from_slice(bytes.as_ref()).map_err(|e| {
                        DataFusionError::Internal(format!("Failed to parse JSON: {}", e))
                    }))
                    .transpose()?
                    .unwrap_or(LatestCheckpoints {
                        completed_checkpoints: Vec::new(),
                    });
                let updated_latest = callback(current_latest)?;

                // Serialize the updated content
                let json_content = serde_json::to_vec_pretty(&updated_latest).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to serialize JSON: {}", e))
                })?;

                Ok(Some(Bytes::from(json_content)))
            },
        ).await
    }

    fn get_latest_json_path(&self, state_id: &str) -> Path {
        Path::from(format!("operators/{}/latest.json", state_id))
    }
}
