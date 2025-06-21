use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use futures_util::TryFutureExt;
use object_store::{ObjectStore, UpdateVersion};
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::PutMode;
use datafusion::common::{internal_datafusion_err, DataFusionError, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::file_system::FileSystemStorage;

pub type ObjectStoreRef = Arc<dyn ObjectStore>;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatestCheckpoints {
    pub completed_checkpoints: Vec<CheckpointEntry>,
}

pub struct FileSystemStateStorage {
    storage: ObjectStoreRef,
}

impl FileSystemStateStorage {
    pub fn new(storage: ObjectStoreRef) -> Self {
        Self { storage }
    }

    pub async fn start_checkpoint_part(&self, operator_id: &str) -> Result<String> {
        // TODO use operator_id
        let checkpoint_part_id = Uuid::new_v4().to_string();
        Ok(checkpoint_part_id)
    }

    pub fn get_checkpoint_file_system(&self, operator_id: &str, checkpoint_part_id: &str) -> ObjectStoreRef {
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

        let partition_info = PartitionInfo {
            partition_range,
            timestamp,
            checkpoint_part_id,
        };

        self.atomic_update_latest_json(operator_id, |mut latest| {
            if let Some(existing_entry) = latest
                .completed_checkpoints
                .iter_mut()
                .find(|entry| entry.id == checkpoint_id)
            {
                existing_entry.partitions.push(partition_info);
            } else {
                let new_entry = CheckpointEntry {
                    id: checkpoint_id.to_string(),
                    partitions: vec![partition_info],
                };
                latest.completed_checkpoints.push(new_entry);
            }

            Ok(latest)
        }).await?;

        Ok(())
    }

    pub async fn get_operator_checkpoints(
        &self,
        operator_id: &str,
        partition_range: &PartitionRange,
        checkpoint_id: &str,
    ) -> Result<Vec<(String, PartitionRange)>> {
        let latest = self.read_latest_json(operator_id).await?;

        println!("Read latest file: {:?}", latest);

        Ok(latest
            .completed_checkpoints
            .into_iter()
            // Only consider checkpoints that match the given id
            .filter(|checkpoint_entry| &checkpoint_entry.id == checkpoint_id)
            .find_map(|checkpoint_entry| {
                let partition_ranges = checkpoint_entry.partitions
                    .iter()
                    .map(|partition_info| partition_info.partition_range.clone())
                    .collect::<Vec<_>>();
                let intersecting_partitions = Self::get_non_overlapping_partitions(partition_range, &partition_ranges)?;
                let partition_infos = intersecting_partitions.into_iter()
                    .map(|overlap| (
                        checkpoint_entry.partitions[overlap.index].checkpoint_part_id.clone(),
                        overlap.selected_partition_range,
                    ))
                    .collect::<Vec<_>>();
                Some(partition_infos)
            })
            .ok_or(internal_datafusion_err!(
                "No viable checkpoints found for operator {} in range {:?}",
                operator_id,
                partition_range
            ))?)
    }

    pub async fn get_latest_operator_checkpoints(
        &self,
        operator_id: &str,
        partition_range: &PartitionRange,
    ) -> Result<(String, Vec<(String, PartitionRange)>)> {
        let latest = self.read_latest_json(operator_id).await?;
        Ok(latest
            .completed_checkpoints
            .into_iter()
            .find_map(|checkpoint_entry| {
                let partition_ranges = checkpoint_entry.partitions
                    .iter()
                    .map(|partition_info| partition_info.partition_range.clone())
                    .collect::<Vec<_>>();
                let intersecting_partitions = Self::get_non_overlapping_partitions(partition_range, &partition_ranges)?;
                let partition_infos = intersecting_partitions.into_iter()
                    .map(|overlap| (
                        checkpoint_entry.partitions[overlap.index].checkpoint_part_id.clone(),
                        overlap.selected_partition_range,
                    ))
                    .collect::<Vec<_>>();
                Some((checkpoint_entry.id, partition_infos))
            })
            .ok_or(internal_datafusion_err!(
                "No viable checkpoints found for operator {} in range {:?}",
                operator_id,
                partition_range
            ))?)
    }

    fn get_checkpoint_path(&self, operator_id: &str, checkpoint_id: &str) -> Path {
        Path::from(format!(
            "operators/{}/checkpoints/{}",
            operator_id, checkpoint_id
        ))
    }

    fn get_latest_json_path(&self, operator_id: &str) -> Path {
        Path::from(format!("operators/{}/latest.json", operator_id))
    }

    async fn read_latest_json(&self, operator_id: &str) -> Result<LatestCheckpoints> {
        let path = self.get_latest_json_path(operator_id);
        match self.storage.get(&path).await {
            Ok(result) => {
                let content = result.bytes().await
                    .map_err(|e| internal_datafusion_err!("Failed to get file bytes: {}", e))?;
                serde_json::from_slice(content.as_ref()).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to parse JSON: {}", e))
                })
            }
            Err(_) => {
                Ok(LatestCheckpoints {
                    completed_checkpoints: Vec::new(),
                })
            }
        }
    }

    /// Atomically update the latest.json file using optimistic concurrency control with ETags.
    ///
    /// This method:
    /// 1. Reads the current latest.json file and its ETag
    /// 2. Calls the provided callback with the parsed LatestCheckpoints
    /// 3. Conditionally writes the modified LatestCheckpoints back only if the ETag matches
    /// 4. Loops on ETag conflicts until successful or non-conflict error occurs
    /// 5. Returns Ok(()) on success, or Err on non-conflict failures
    pub async fn atomic_update_latest_json<F>(
        &self,
        operator_id: &str,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(LatestCheckpoints) -> Result<LatestCheckpoints>,
    {
        let latest_path = self.get_latest_json_path(operator_id);

        loop {
            // Read current state with ETag
            let (current_latest, e_tag, version) = match self.storage.get(&latest_path).await {
                Ok(result) => {
                    let e_tag = result.meta.e_tag.clone();
                    let version = result.meta.version.clone();
                    let content = result.bytes().await
                        .map_err(|e| internal_datafusion_err!("Failed to get file bytes: {}", e))?;
                    let latest = serde_json::from_slice(content.as_ref()).map_err(|e| {
                        DataFusionError::Internal(format!("Failed to parse JSON: {}", e))
                    })?;
                    (latest, e_tag, version)
                }
                Err(_) => {
                    // File doesn't exist, use empty state with no ETag
                    (LatestCheckpoints {
                        completed_checkpoints: Vec::new(),
                    }, None, None)
                }
            };

            // Apply the callback to get the updated state
            let updated_latest = callback(current_latest)?;

            // Serialize the updated content
            let json_content = serde_json::to_string_pretty(&updated_latest).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize JSON: {}", e))
            })?;

            // Conditionally write back using ETag
            let put_result = match e_tag {
                Some(_) => {
                    // File exists, use conditional update
                    self.storage.put_opts(
                        &latest_path,
                        json_content.into(),
                        PutMode::Update(UpdateVersion {
                            e_tag,
                            version,
                        }).into(),
                    ).await
                }
                None => {
                    // File doesn't exist, use conditional create
                    self.storage.put_opts(&latest_path, json_content.into(), PutMode::Create.into())
                        .await
                }
            };

            match put_result {
                Ok(_) => {
                    println!("Atomically updated latest json to {:?}, {:?}", latest_path, updated_latest);
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

    fn get_non_overlapping_partitions(
        search_range: &PartitionRange,
        available_partitions: &[PartitionRange],
    ) -> Option<Vec<OverlappingPartition>> {
        // Easy implementation, not quite optimal, but good enough for now.
        
        // All partition ranges will be converted to the same partition count for comparison
        let target_partitions = available_partitions.iter()
            .map(|partition_range| partition_range.partitions())
            .chain([search_range.partitions()])
            .max()?;
        let search_start = search_range.start() * target_partitions / search_range.partitions();
        let search_end = search_range.end() * target_partitions / search_range.partitions();
        
        // Normalize all available partitions to the target partition count
        // (original index, normalised start, normalised end)
        let mut normalized_partitions: Vec<(usize, usize, usize)> = available_partitions
            .iter()
            .enumerate()
            .map(|(index, partition_range)| {
                let normalized_start = partition_range.start() * target_partitions / partition_range.partitions();
                let normalized_end = partition_range.end() * target_partitions / partition_range.partitions();
                (index, normalized_start, normalized_end)
            })
            .filter(|(_index, start, end)| {
                // Check if the partition overlaps with the search range
                *start < search_end && *end > search_start
            })
            .collect();
        
        // Sort by start position
        normalized_partitions.sort_by_key(|(start, _, _)| *start);

        let mut current_search_start = search_start;
        let mut found_partitions = Vec::new();
        for (index, start, end) in normalized_partitions {
            if end > current_search_start {
                // If the partition starts after the current search start, we have a gap
                if start > current_search_start {
                    // No overlapping partitions found
                    return None;
                }

                // Update the current search start to the end of this partition
                let overlap_end = search_end.min(end);
                found_partitions.push(OverlappingPartition {
                    index,
                    selected_partition_range: PartitionRange::new(
                        current_search_start,
                        overlap_end,
                        target_partitions,
                    ),
                });

                current_search_start = overlap_end;
                if current_search_start >= search_end {
                    // We have finished searching
                    return Some(found_partitions);
                }
            }
        }

        // If we reach here, it means we didn't cover the entire search range
        assert!(current_search_start < search_end, "Search algorithm finished without returning the success case");
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
struct OverlappingPartition {
    index: usize,
    selected_partition_range: PartitionRange,
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

        let latest = checkpoint_storage.read_latest_json(operator_id).await?;
        
        assert_eq!(latest.completed_checkpoints.len(), 1);
        assert_eq!(latest.completed_checkpoints[0].partitions.len(), 1);
        assert_eq!(latest.completed_checkpoints[0].partitions[0].partition_range.start(), 0);
        assert_eq!(latest.completed_checkpoints[0].partitions[0].partition_range.end(), 1);
        
        Ok(())
    }

    #[test]
    fn test_get_non_overlapping_partitions() {
        // Test case 1: Perfect coverage with non-overlapping partitions
        let search_range = PartitionRange::new(0, 4, 4); // Covers partitions 0,1,2,3 out of 4
        let available_partitions = vec![
            PartitionRange::new(0, 2, 4), // Covers 0,1
            PartitionRange::new(2, 4, 4), // Covers 2,3
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 2, 4),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::new(2, 4, 4),
            },
        ]));

        // Test case 2: No coverage possible due to gap
        let search_range = PartitionRange::new(0, 4, 4);
        let available_partitions = vec![
            PartitionRange::new(0, 1, 4), // Covers 0 only
            PartitionRange::new(3, 4, 4), // Covers 3 only (gap at 1,2)
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert!(result.is_none());
        
        // Test case 3: Different partition counts (normalised comparison)
        let search_range = PartitionRange::new(0, 1, 2); // Half of 2 partitions = equivalent to 0-5 of 10
        let available_partitions = vec![
            PartitionRange::new(0, 5, 10), // First half in 10-partition system
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 5, 10),
            },
        ]));

        // Test case 4: Search range has fewer base partitions, and available partitions don't
        // divide evenly
        let search_range = PartitionRange::new(0, 1, 2); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::new(0, 3, 8), // 8 partitions total, covers first half
            PartitionRange::new(3, 4, 8), // 8 partitions total, covers second half
        ];
        
        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(0, 3, 8),
            },
            OverlappingPartition {
                index: 1,
                selected_partition_range: PartitionRange::new(3, 4, 8),
            },
        ]));

        // Test case 5: Ranges that extend outside search range and overlap with each other
        let search_range = PartitionRange::new(2, 6, 8); // 2 partitions total
        let available_partitions = vec![
            PartitionRange::new(3, 8, 16),
            PartitionRange::new(0, 3, 16), // Should be excluded
            PartitionRange::new(6, 16, 16),
            PartitionRange::new(7, 16, 16),
        ];

        let result = FileSystemStateStorage::get_non_overlapping_partitions(&search_range, &available_partitions);
        assert_eq!(result, Some(vec![
            OverlappingPartition {
                index: 0,
                selected_partition_range: PartitionRange::new(4, 8, 16),
            },
            OverlappingPartition {
                index: 2,
                selected_partition_range: PartitionRange::new(8, 12, 16),
            },
        ]))
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
