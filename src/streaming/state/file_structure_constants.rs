use std::path::PathBuf;
use object_store::path::Path;

// Remote structure
// ---------------------

pub fn make_checkpoint_part_id() -> String {
    format!("checkpoint_part_{}", uuid::Uuid::new_v4())
}

pub fn get_remote_checkpoint_part_dir_path(state_id: &str, checkpoint_part_id: &str) -> Path {
    Path::from(format!(
        "operators/{}/checkpoints/{}",
        state_id, checkpoint_part_id
    ))
}

pub fn get_latest_json_path(state_id: &str) -> Path {
    Path::from(format!("operators/{}/latest.json", state_id))
}


// Local structure
// ---------------------

const WORKING_DIR: &str = "working";
const CHECKPOINT_DIR: &str = "checkpoint";

pub fn get_base_working_dir_path() -> PathBuf {
    PathBuf::from(WORKING_DIR)
}

pub fn make_working_dir_path() -> PathBuf {
    get_base_working_dir_path().join(format!("workdir_{}", uuid::Uuid::new_v4().to_string()))
}

pub fn get_base_checkpoint_dir_path() -> PathBuf {
    PathBuf::from(CHECKPOINT_DIR)
}

pub fn make_checkpoint_dir_path(checkpoint: &str) -> PathBuf {
    get_base_checkpoint_dir_path().join(format!("{}__unique-suffix_{}", checkpoint, uuid::Uuid::new_v4()))
}
