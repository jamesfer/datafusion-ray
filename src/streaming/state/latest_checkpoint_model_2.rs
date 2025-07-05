use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use crate::streaming::partitioning::PartitionRange;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParentState {
    NotStarted,
    SamePoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointPart {
    pub checkpoint_id: String,
    pub operator_id: String,
    pub partition_range: PartitionRange,
    pub completed_timestamp: u64,
    pub parents: Vec<(usize, ParentState)>,
    // Map from state name to the exact reference in the storage for the checkpoint of that state
    pub states_refs: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointEntry {
    pub id: String,
    pub started_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LatestCheckpoints {
    // Keyed by checkpoint id, then operator id
    pub checkpoint_parts: HashMap<String, HashMap<String, Vec<CheckpointPart>>>,
    pub checkpoints: Vec<CheckpointEntry>
}

impl LatestCheckpoints {
    pub fn add_checkpoint_part(&mut self, part: CheckpointPart) {
        // Ensure checkpoint entry exists
        if !self.checkpoints.iter().any(|c| c.id == part.checkpoint_id) {
            self.checkpoints.push(CheckpointEntry {
                id: part.checkpoint_id.clone(),
                started_timestamp: part.completed_timestamp,
            });
        }

        // Add part to checkpoint_parts storage
        self.checkpoint_parts
            .entry(part.checkpoint_id.clone())
            .or_default()
            .entry(part.operator_id.clone())
            .or_default()
            .push(part);
    }

    pub fn get_checkpoint(
        &self,
        checkpoint_id: &str,
        operator_id: &str,
        partition_range: &PartitionRange,
    ) -> Option<Vec<(&CheckpointPart, PartitionRange)>> {
        let checkpoint_parts = self.checkpoint_parts.get(checkpoint_id)?.get(operator_id)?;
        let ranges = checkpoint_parts.iter()
            .map(|part| part.partition_range.clone())
            .collect::<Vec<_>>();
        let overlapping_ranges = partition_range.find_covering_partitions(&ranges)?;
        overlapping_ranges.into_iter()
            .map(|overlap| (&checkpoint_parts[overlap.index], overlap.selected_partition_range))
            .collect()
    }

    pub fn latest_complete_checkpoint(&self, operator_flow_graph: OperatorFlowGraph) -> Result<Option<&str>, DataFusionError> {
        for checkpoint in self.checkpoints.iter().rev() {
            let checkpoint_parts = match self.checkpoint_parts.get(&checkpoint.id) {
                None => continue,
                Some(parts) => parts,
            };

            if Self::check_checkpoint(&operator_flow_graph, checkpoint_parts)? {
                return Ok(Some(&checkpoint.id));
            }
        }

        Ok(None)
    }

    fn check_checkpoint(
        operator_flow_graph: &OperatorFlowGraph,
        checkpoint_parts: &HashMap<String, Vec<CheckpointPart>>
    ) -> Result<bool, DataFusionError> {
        // For each operator, check that for each partition, all parents have the same checkpoint id
        // and all children, following the partitioning strategy listed.
        let mut requirements_graph: HashMap<String, Vec<_>> = HashMap::new();

        // For each operator in the flow graph, convert the existing checkpoint parts into
        // requirements
        for (_, node) in &operator_flow_graph.nodes {
            let requirements = match checkpoint_parts.get(&node.operator_id) {
                Some(parts) => {
                    parts.iter().map(|part| CompletedCheckpointRequirement {
                        partition_range: part.partition_range.clone(),
                        requirement: RequirementEnum::SamePoint,
                    }).collect()
                },
                None => {
                    // No checkpoints were found for this operator, so it is all in the not
                    // started state.
                    vec![
                        CompletedCheckpointRequirement {
                            partition_range: PartitionRange::full(),
                            requirement: RequirementEnum::NotStarted,
                        }
                    ]
                },
            };
            requirements_graph.insert(node.operator_id.clone(), requirements);
        }

        // Iterate again through the operators, to ensure that each operator's parents and
        // children are consistent with each other
        for (_, node) in &operator_flow_graph.nodes {
            let requirements = requirements_graph.get(&node.operator_id)
                .ok_or(internal_datafusion_err!("No requirements found for operator {}", node.operator_id))?;
            for parent_link in &node.parents {
                let parent_requirements = requirements_graph.get(&parent_link.parent_id)
                    .ok_or(internal_datafusion_err!("No requirements found for parent operator {}", parent_link.parent_id))?;

                if !Self::verify_parent_and_child_are_consistent(
                    requirements,
                    parent_requirements,
                    &parent_link.partitioning,
                ) {
                    return Ok(false);
                }
            }

            // // Now find all children of this node
            // let children = operator_flow_graph.nodes
            //     .iter()
            //     .flat_map(|(child_id, child_node)| {
            //         child_node.parents.iter()
            //             .filter(|parent_link| parent_link.parent_id == node.operator_id)
            //             .map(move |parent_link| (child_id, parent_link))
            //     });
            // for (child_id, child_link) in children {
            //     let child_requirements = requirements_graph.get(child_id)
            //         .ok_or(internal_datafusion_err!("No requirements found for child operator {}", child_id))?;
            //     if !Self::verify_parent_and_child_are_consistent(
            //         child_requirements,
            //         requirements,
            //         &child_link.partitioning,
            //     ) {
            //         return Ok(false);
            //     }
            // }
        }

        Ok(true)
    }

    fn verify_parent_and_child_are_consistent(
        child_requirements: &[CompletedCheckpointRequirement],
        parent_requirements: &[CompletedCheckpointRequirement],
        partitioning_dependency: &OperatorFlowGraphPartitioning,
    ) -> bool {
        // match partitioning_dependency {
        //     OperatorFlowGraphPartitioning::Shuffle => {
        //         // When the partitioning dependency is shuffle, there needs to be at least one child if
        //         // there is at least one parent. The case where there are no parents and some
        //         // children is checked below.
        //         if parent_requirements.len() > 0 && child_requirements.is_empty() {
        //             return false;
        //         }
        //     },
        //     OperatorFlowGraphPartitioning::EquivalentHash => {
        //         // When the partitioning dependency is equivalent hash, each parent needs to be
        //         // covered by the child partition ranges
        //         let all_parents_covered = parent_requirements.iter().all(|parent_requirement| {
        //             parent_requirement.partition_range.find_covering_partitions(
        //                 &child_requirements.iter()
        //                     .map(|r| r.partition_range.clone())
        //                     .collect::<Vec<_>>()
        //             ).is_some()
        //         });
        //
        //         if !all_parents_covered {
        //             return false;
        //         }
        //     },
        // };

        child_requirements.iter().all(|child_requirement| {
            // Get the set of parent requirements that need to be satisfied based on the
            // way data is partitioned between the two operators
            let necessary_parent_partition_range = match partitioning_dependency {
                OperatorFlowGraphPartitioning::Shuffle => {
                    // For shuffle, we need to ensure that all parents have the same
                    // partition range
                    PartitionRange::full()
                },
                OperatorFlowGraphPartitioning::EquivalentHash => {
                    // For equivalent hash, we need to find the covering partitions
                    // that match the requirement's partition range
                    child_requirement.partition_range.clone()
                },
            };

            match child_requirement.requirement {
                RequirementEnum::NotStarted => {
                    // The parent must not have been started yet. If there are any
                    // overlapping checkpoints, we can return false
                    let count = parent_requirements.iter()
                        .filter(|parent_requirement| {
                            !necessary_parent_partition_range.intersection(&parent_requirement.partition_range).is_empty()
                        })
                        .count();

                    count == 0
                },
                RequirementEnum::SamePoint => {
                    // The parent checkpoints must cover the whole partition range, and they must
                    // all be at the same checkpoint
                    let parent_partition_ranges = parent_requirements.iter()
                        .map(|r| r.partition_range.clone())
                        .collect::<Vec<_>>();
                    let overlapping_parent_requirements = necessary_parent_partition_range
                        .find_covering_partitions(&parent_partition_ranges);
                    match overlapping_parent_requirements {
                        None => false,
                        Some(overlaps) => overlaps.iter()
                            .all(|overlap| parent_requirements[overlap.index].requirement == RequirementEnum::SamePoint)
                    }
                }
            }
        })
    }
}

#[derive(PartialEq)]
pub enum RequirementEnum {
    NotStarted,
    SamePoint,
}

pub struct CompletedCheckpointRequirement {
    partition_range: PartitionRange,
    requirement: RequirementEnum,
}

pub enum OperatorFlowGraphPartitioning {
    Shuffle,
    EquivalentHash,
}

pub struct OperatorFlowGraphParent {
    parent_id: String,
    ordinal: usize,
    partitioning: OperatorFlowGraphPartitioning,
}

pub struct OperatorFlowGraphNode {
    operator_id: String,
    parents: Vec<OperatorFlowGraphParent>,
}

pub struct OperatorFlowGraph {
    nodes: HashMap<String, OperatorFlowGraphNode>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latest_complete_checkpoint_can_find_complete_checkpoint() {
        let mut checkpoints = LatestCheckpoints::default();
        
        // Create two checkpoint parts for different operators
        let part1 = CheckpointPart {
            checkpoint_id: "checkpoint_1".to_string(),
            operator_id: "operator_1".to_string(),
            partition_range: PartitionRange::full(),
            completed_timestamp: 1000,
            parents: vec![],
            states_refs: HashMap::new(),
        };
        
        let part2 = CheckpointPart {
            checkpoint_id: "checkpoint_1".to_string(),
            operator_id: "operator_2".to_string(),
            partition_range: PartitionRange::full(),
            completed_timestamp: 1001,
            parents: vec![(0, ParentState::SamePoint)],
            states_refs: HashMap::new(),
        };

        // Add parts to checkpoints
        checkpoints.add_checkpoint_part(part1);
        checkpoints.add_checkpoint_part(part2);

        // Create operator flow graph with shuffle partitioning
        let mut nodes = HashMap::new();
        
        nodes.insert("operator_1".to_string(), OperatorFlowGraphNode {
            operator_id: "operator_1".to_string(),
            parents: vec![],
        });
        
        nodes.insert("operator_2".to_string(), OperatorFlowGraphNode {
            operator_id: "operator_2".to_string(),
            parents: vec![OperatorFlowGraphParent {
                parent_id: "operator_1".to_string(),
                ordinal: 0,
                partitioning: OperatorFlowGraphPartitioning::Shuffle,
            }],
        });

        let flow_graph = OperatorFlowGraph { nodes };

        // Test that latest_complete_checkpoint finds the checkpoint
        let result = checkpoints.latest_complete_checkpoint(flow_graph).unwrap();
        assert_eq!(result, Some("checkpoint_1"));
    }

    #[test]
    fn test_latest_complete_checkpoint_returns_none_when_parent_missing() {
        let mut checkpoints = LatestCheckpoints::default();
        
        // Create checkpoint part only for child operator
        let part2 = CheckpointPart {
            checkpoint_id: "checkpoint_1".to_string(),
            operator_id: "operator_2".to_string(),
            partition_range: PartitionRange::full(),
            completed_timestamp: 1001,
            parents: vec![(0, ParentState::SamePoint)],
            states_refs: HashMap::new(),
        };

        // Add only the child part
        checkpoints.add_checkpoint_part(part2);

        // Create operator flow graph with shuffle partitioning
        let mut nodes = HashMap::new();
        
        nodes.insert("operator_1".to_string(), OperatorFlowGraphNode {
            operator_id: "operator_1".to_string(),
            parents: vec![],
        });
        
        nodes.insert("operator_2".to_string(), OperatorFlowGraphNode {
            operator_id: "operator_2".to_string(),
            parents: vec![OperatorFlowGraphParent {
                parent_id: "operator_1".to_string(),
                ordinal: 0,
                partitioning: OperatorFlowGraphPartitioning::Shuffle,
            }],
        });

        let flow_graph = OperatorFlowGraph { nodes };

        // Test that latest_complete_checkpoint returns None
        let result = checkpoints.latest_complete_checkpoint(flow_graph).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_latest_complete_checkpoint_returns_none_when_child_missing() {
        let mut checkpoints = LatestCheckpoints::default();
        
        // Create checkpoint part only for parent operator
        let part1 = CheckpointPart {
            checkpoint_id: "checkpoint_1".to_string(),
            operator_id: "operator_1".to_string(),
            partition_range: PartitionRange::full(),
            completed_timestamp: 1000,
            parents: vec![],
            states_refs: HashMap::new(),
        };

        // Add only the parent part
        checkpoints.add_checkpoint_part(part1);

        // Create operator flow graph with shuffle partitioning
        let mut nodes = HashMap::new();
        
        nodes.insert("operator_1".to_string(), OperatorFlowGraphNode {
            operator_id: "operator_1".to_string(),
            parents: vec![],
        });
        
        nodes.insert("operator_2".to_string(), OperatorFlowGraphNode {
            operator_id: "operator_2".to_string(),
            parents: vec![OperatorFlowGraphParent {
                parent_id: "operator_1".to_string(),
                ordinal: 0,
                partitioning: OperatorFlowGraphPartitioning::Shuffle,
            }],
        });

        let flow_graph = OperatorFlowGraph { nodes };

        // Test that latest_complete_checkpoint returns None
        let result = checkpoints.latest_complete_checkpoint(flow_graph).unwrap();
        assert_eq!(result, None);
    }
}
