use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::file_structure_constants::make_checkpoint_dir_path;
use crate::streaming::state::file_system::{FileSystemStorage, PrefixedLocalFileSystemStorage};
use crate::streaming::state::latest_checkpoint_model_2::ParentState;
use crate::streaming::state::local_rocksdb::LocalRocksDB;
use crate::streaming::state::remote_checkpoint_storage::{ObjectStoreRef, RemoteCheckpointStorage};
use datafusion::common::{internal_datafusion_err, DataFusionError};
use futures::{StreamExt, TryStreamExt};
use futures_util::TryFutureExt;
use object_store::ObjectStore;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

fn make_remote_state_subdir(checkpoint_id: &str) -> String {
    format!("{}__unique-suffix_{}", checkpoint_id, uuid::Uuid::new_v4())
}

pub struct RocksDBStateBackend {
    operator_id: String,
    state_id: String,
    partitions: PartitionRange,
    remote_checkpoint_storage: Arc<RemoteCheckpointStorage>,
    local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    local_rocksdb: LocalRocksDB,
    background_task: JoinHandle<()>,
    background_sync_channel: Sender<(String, PartitionRange, HashMap<String, String>, Vec<(usize, ParentState)>)>,
}

impl RocksDBStateBackend {
    pub async fn open_new(
        operator_id: String,
        state_id: String,
        partitions: PartitionRange,
        remote_checkpoint_storage: Arc<RemoteCheckpointStorage>,
        local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    ) -> Result<Self, DataFusionError> {
        let root_dir = Self::create_root_directory_path(&state_id, &partitions);
        let scoped_file_system = Arc::new(PrefixedLocalFileSystemStorage::new(local_file_system.get_physical_path(&root_dir)?));

        // Open the main database
        let local_rocksdb = LocalRocksDB::open_new(scoped_file_system.clone()).await?;

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let background_task = tokio::spawn({
            let operator_id = operator_id.clone();
            let remote_checkpoint_storage = remote_checkpoint_storage.clone();
            let local_file_system = local_file_system.clone();
            async move {
                async_checkpoint_background_task(
                    &operator_id,
                    receiver,
                    local_file_system,
                    &remote_checkpoint_storage,
                ).await
            }
        });

        Ok(Self {
            operator_id,
            state_id,
            partitions,
            remote_checkpoint_storage,
            local_file_system: scoped_file_system,
            local_rocksdb,
            background_sync_channel: sender,
            background_task,
        })
    }

    // pub async fn open_from(
    //     state_id: String,
    //     checkpoint: usize,
    //     partitions: PartitionRange,
    //     file_system: Box<dyn FileSystemStorage>,
    // ) -> Self {
    //     // Iterate through each saved db instance, find the ones that match the checkpoint and
    //     // partition range, download the files, and clone them into a new RocksDB instance.
    // }

    pub fn put(&mut self, k: impl AsRef<[u8]>, v: impl AsRef<[u8]>) -> Result<(), DataFusionError> {
        self.local_rocksdb.put(k, v)
    }

    pub fn get(&mut self, k: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, DataFusionError> {
        self.local_rocksdb.get(k)
    }

    pub fn iterate(&self) -> impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), DataFusionError>> {
        self.local_rocksdb.iterate()
    }

    pub async fn checkpoint(
        &mut self,
        checkpoint_id: usize,
        partition_range: PartitionRange,
        parents: Vec<(usize, ParentState)>,
    ) -> Result<(), DataFusionError> {
        let checkpoint_id = format!("{}", checkpoint_id);
        let checkpoint_dir = self.local_rocksdb.create_checkpoint(&checkpoint_id)?;

        // Sends the checkpoint to the background task to persist it to the remote file system
        self.background_sync_channel.send((
            checkpoint_id,
            partition_range,
            [(self.state_id.clone(), checkpoint_dir.to_string_lossy().to_string())].iter().collect(),
            parents
        )).await
            .map_err(|e| internal_datafusion_err!("Failed to send checkpoint to background task: {}", e))?;

        Ok(())
    }

    pub async fn move_to_checkpoint(&mut self, checkpoint: usize, partition_range: &PartitionRange) -> Result<(), DataFusionError> {
        let checkpoint_id = format!("{}", checkpoint);
        println!("Attempting to move to checkpoint {} with partition range {:?} (state id {})", checkpoint, partition_range, self.state_id);
        // Find the checkpoint paths in the remote store we need to use
        let checkpoint_parts = self.find_checkpoint(checkpoint, partition_range).await?;

        // We need to download the checkpoints first. This is also inefficient as we end up
        // downloading the checkpoints before copying them into the working directory.
        let mut downloaded_checkpoint_part_paths = Vec::with_capacity(checkpoint_parts.len());
        for (checkpoint_state_dir, _partition_range) in checkpoint_parts {
            let destination_path = self.download_remote_checkpoint_part(&checkpoint_id, &checkpoint_state_dir).await?;
            downloaded_checkpoint_part_paths.push(destination_path);
        }
        let downloaded_checkpoint_part_paths = downloaded_checkpoint_part_paths;

        println!("Found {} checkpoints to use. Paths {:?}", downloaded_checkpoint_part_paths.len(), downloaded_checkpoint_part_paths);

        // Now load all the partial checkpoints into the current database
        self.local_rocksdb.load_from_checkpoint_parts(downloaded_checkpoint_part_paths).await
    }

    async fn download_remote_checkpoint_part(
        &mut self,
        checkpoint_id: &str,
        checkpoint_state_dir: &str,
    ) -> Result<PathBuf, DataFusionError> {
        let remote_checkpoint_file_system = self.remote_checkpoint_storage.get_state_file_system(&self.operator_id, &self.state_id);
        let remote_path = object_store::path::Path::from(checkpoint_state_dir);
        let destination_path = make_checkpoint_dir_path(&checkpoint_id);
        Self::download_dir(
            &remote_checkpoint_file_system,
            &remote_path,
            self.local_file_system.as_ref(),
            &destination_path,
        ).await?;
        Ok(destination_path)
    }

    async fn download_dir(
        source_system: &ObjectStoreRef,
        source_dir: &object_store::path::Path,
        destination_system: &(dyn FileSystemStorage + Send + Sync),
        destination_dir: impl AsRef<Path>,
    ) -> Result<(), DataFusionError> {
        let destination_dir = destination_dir.as_ref();
        
        // Create destination directory
        destination_system.mkdir_all(destination_dir).await?;
        
        // List all objects with the source_dir as prefix
        let list_stream = source_system.list(Some(source_dir));
        let objects: Vec<_> = list_stream.try_collect().await
            .map_err(|e| internal_datafusion_err!("Failed to list objects: {}", e))?;
        
        for object in objects {
            // Remove the source_dir prefix to get the relative path
            let relative_path = if source_dir.parts().count() == 0 {
                object.location.as_ref()
            } else if let Some(stripped) = object.location.as_ref().strip_prefix(&format!("{}/", source_dir)) {
                stripped
            } else if object.location.as_ref() == source_dir.as_ref() {
                // Handle case where the object is exactly the source_dir (shouldn't happen for directories but just in case)
                continue;
            } else {
                // This shouldn't happen if we're listing with the correct prefix, but handle it gracefully
                return Err(internal_datafusion_err!(
                    "Object {} does not match source directory prefix {}",
                    object.location,
                    source_dir
                ));
            };
            
            let entry_destination_path = destination_dir.join(relative_path);
            
            // Ensure parent directory exists
            if let Some(parent) = entry_destination_path.parent() {
                destination_system.mkdir_all(parent).await?;
            }
            
            // Download the file content
            let result = source_system.get(&object.location).await
                .map_err(|e| internal_datafusion_err!("Failed to get object: {}", e))?;
            let content = result.bytes().await
                .map_err(|e| internal_datafusion_err!("Failed to read object bytes: {}", e))?;
            
            // Write to destination
            destination_system.write_file(&entry_destination_path, &content).await?;
        }

        Ok(())
    }

    async fn find_checkpoint(&self, checkpoint: usize, partition_range: &PartitionRange) -> Result<Vec<(String, PartitionRange)>, DataFusionError> {
        // let checkpoint_root_dir = Self::base_checkpoint_directory_path(&self.local_root_dir);

        // // Find the checkpoint in the local file system first. This is fairly inefficient
        // // TODO make use of the partition range when searching locally
        // let checkpoint_files = self.local_file_system.list_files(&checkpoint_root_dir).await?;
        // let checkpoint_dir = checkpoint_files
        //     .iter()
        //     .find(|entry| entry.name.to_str().map_or(false, |s| s.starts_with(&format!("{}__", checkpoint))));
        // if let Some(checkpoint_dir) = checkpoint_dir {
        //     let checkpoint_path = checkpoint_root_dir.join(&checkpoint_dir.name);
        //     println!("Moving to checkpoint at {}", self.local_file_system.get_physical_path(&checkpoint_path)?.display());
        //     return Ok((true, vec![(checkpoint_path.to_string_lossy().to_string(), partition_range)]));
        // }

        // If no local checkpoint was found, try to find it in the remote file system
        println!("Searching remotely");
        let remote_checkpoint_parts = self.remote_checkpoint_storage.get_operator_checkpoint_parts(
            &self.operator_id,
            &self.partitions,
            &format!("{}", checkpoint),
        ).await?
            .ok_or_else(|| {;
                internal_datafusion_err!(
                    "No checkpoints found for operator {} with partition range {:?} and checkpoint {}",
                    self.operator_id,
                    self.partitions,
                    checkpoint
                )
            })?;

        remote_checkpoint_parts.into_iter()
            .map(|(part, range)| {
                part.states_refs.get(&self.state_id)
                    .map(|path| (path.clone(), range))
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "No state reference found for state {} in checkpoint part {}",
                            self.state_id,
                            part.checkpoint_id
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn create_root_directory_path(state_id: &String, partitions: &PartitionRange) -> PathBuf {
        Path::new(state_id).join(format!(
            "partition-{}-{}-{}",
            partitions.start(),
            partitions.end(),
            partitions.partitions(),
        ))
    }
}

async fn async_checkpoint_background_task(
    operator_id: &str,
    mut checkpoints: tokio::sync::mpsc::Receiver<(String, PartitionRange, HashMap<String, String>, Vec<(usize, ParentState)>)>,
    local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    remote_checkpoint_storage: &RemoteCheckpointStorage,
) {
    // Uploads checkpoints in the background to the remote file system
    while let Some((checkpoint_id, partition_range, local_state_paths, parents)) = checkpoints.recv().await {
        upload_checkpoint(
            operator_id,
            &checkpoint_id,
            local_state_paths,
            parents,
            partition_range,
            local_file_system.as_ref(),
            remote_checkpoint_storage,
        ).await.unwrap_or_else(|e| {
            eprintln!("Failed to upload checkpoint directory {:?}: {}", local_state_paths, e);
        });
    }
}

async fn upload_checkpoint(
    operator_id: &str,
    checkpoint_id: &str,
    local_state_paths: HashMap<String, String>,
    parents: Vec<(usize, ParentState)>,
    partition_range: PartitionRange,
    local_file_system: &(dyn FileSystemStorage + Send + Sync),
    remote_checkpoint_storage: &RemoteCheckpointStorage,
) -> Result<(), DataFusionError> {
    println!("Uploading checkpoint {} for operator {} from {:?} to remote storage", checkpoint_id, operator_id, local_state_paths);

    let mut remote_state_refs = HashMap::new();
    for (state_id, state_dir) in local_state_paths.into_iter() {
        let remote_state_file_system = remote_checkpoint_storage.get_state_file_system(operator_id, &state_id);
        let destination_path = object_store::path::Path::from(make_remote_state_subdir(checkpoint_id));
        upload_dir(
            local_file_system,
            &state_dir,
            &remote_state_file_system,
            &destination_path,
        ).await?;
        remote_state_refs.insert(state_id, destination_path.to_string());
    }

    // Finish the checkpoint on the remote file system
    remote_checkpoint_storage.complete_checkpoint(
        operator_id.to_string(),
        checkpoint_id.to_string(),
        partition_range,
        parents,
        remote_state_refs,
    ).await?;

    Ok(())
}

async fn upload_dir(
    source_system: &(dyn FileSystemStorage + Send + Sync),
    source_dir: impl AsRef<Path>,
    destination_system: &ObjectStoreRef,
    destination_dir: &object_store::path::Path,
) -> Result<(), DataFusionError> {
    let mut queue = vec![(source_dir.as_ref().to_path_buf(), destination_dir.clone())];

    while let Some((current_source_dir, current_dest_dir)) = queue.pop() {
        let entries = source_system.list_files(&current_source_dir).await?;

        for entry in entries {
            let entry_source_path = current_source_dir.join(&entry.name);
            let entry_destination_path = object_store::path::Path::from(format!("{}/{}", current_dest_dir, entry.name.to_string_lossy()));

            if entry.directory {
                queue.push((entry_source_path, entry_destination_path));
            } else {
                let contents = source_system.read_file(&entry_source_path).await?;
                destination_system.put(&entry_destination_path, contents.into()).await
                    .map_err(|e| internal_datafusion_err!("Failed to put file: {}", e))?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::streaming::partitioning::PartitionRange;
    use crate::streaming::state::file_system::PrefixedLocalFileSystemStorage;
    use crate::streaming::state::remote_checkpoint_storage::RemoteCheckpointStorage;
    use crate::streaming::state::rocksdb_state_backend::RocksDBStateBackend;
    use crate::streaming::utils::retry::retry_future;
    use object_store::local::LocalFileSystem;
    use rocksdb::checkpoint::Checkpoint;
    use rocksdb::{IngestExternalFileOptions, IteratorMode, Options, SstFileWriter, DB};
    use std::sync::Arc;

    #[tokio::test]
    async fn storage_test() {
        let database_dir = tempfile::tempdir().unwrap();
        let backup_dir = tempfile::tempdir().unwrap();

        let database_file_system = Arc::new(PrefixedLocalFileSystemStorage::new(database_dir.path()));
        let backup_object_store = Arc::new(LocalFileSystem::new_with_prefix(backup_dir.path()).unwrap());
        let remote_checkpoint_storage = Arc::new(RemoteCheckpointStorage::new(
            backup_object_store,
        ));

        let partition_range = PartitionRange::unit();
        let mut backend = RocksDBStateBackend::open_new(
            "test_state".to_string(),
            PartitionRange::new(0, 10, 100),
            remote_checkpoint_storage.clone(),
            database_file_system,
        ).await.unwrap();

        backend.put(b"key1", b"value1").unwrap();
        assert_eq!(backend.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(backend.get("key2").unwrap(), None);

        backend.checkpoint(1, partition_range.clone()).await.unwrap();
        backend.put(b"key2", b"value2").unwrap();
        assert_eq!(backend.get("key2").unwrap(), Some(b"value2".to_vec()));

        // Wait for the checkpoint to finish
        retry_future(10, || {
            remote_checkpoint_storage.get_latest_state_checkpoint_parts("test_state", &partition_range)
        }).await.unwrap();

        backend.move_to_checkpoint(1, partition_range).await.unwrap();
        assert_eq!(backend.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(backend.get("key2").unwrap(), None);
    }

    fn test() {
        let tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let path = tempdir.path();
        println!("Temp path for RocksDB storage: {}", path.display());

        // Create the first database
        {
            let db = DB::open_default(path).unwrap();
            db.put(b"my key", b"my value").unwrap();
            for i in 0..1000 {
                db.put(format!("key {}", i).as_bytes(), format!("value {}", i).as_bytes()).unwrap();
            }

            println!("Wrote all keys");
            println!("Live files: {:?}", db.live_files().unwrap());
            println!("Flushing");
            db.flush().unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());

            match db.get(b"my key") {
                Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
                Ok(None) => println!("value not found"),
                Err(e) => println!("operational problem encountered: {}", e),
            }
        }

        let checkpoint_tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let checkpoint_path = checkpoint_tempdir.path().join("checkpoint");

        // Try to reopen the database at the same path
        {
            println!("Reopening the database at the same path");
            let db = DB::open_default(path).unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());

            match db.get(b"my key") {
                Ok(Some(value)) => println!("retrieved value {}", String::from_utf8(value).unwrap()),
                Ok(None) => println!("value not found"),
                Err(e) => println!("operational problem encountered: {}", e),
            }
            db.delete(b"my key").unwrap();
            db.flush().unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());
            // db.live_files().unwrap()

            // Create a checkpoint
            Checkpoint::new(&db).unwrap()
                .create_checkpoint(&checkpoint_path)
                .unwrap();
        };

        // List all the files in the directory
        println!("Listing all files in the db directory:");
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            println!("File: {}", entry.path().display());
        }

        // List all the files in the checkpoint directory
        println!("Listing all files in the checkpoint directory:");
        for entry in std::fs::read_dir(&checkpoint_path).unwrap() {
            let entry = entry.unwrap();
            println!("File: {}", entry.path().display());
        }

        // Reopen the checkpoint as a database
        {
            println!("Reopening the checkpoint as a database");
            let db = DB::open_default(&checkpoint_path).unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());
            println!("Current entries in second db {:?}", db.iterator(IteratorMode::Start).count());

            db.delete(b"my key").unwrap();
            db.put(b"new key", b"new value").unwrap();
            db.flush().unwrap();
        }

        // List all the files in the checkpoint directory
        println!("Listing all files in the checkpoint directory:");
        for entry in std::fs::read_dir(&checkpoint_path).unwrap() {
            let entry = entry.unwrap();
            println!("File: {}", entry.path().display());
        }

        // Creating a new db
        let second_tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let second_path = second_tempdir.path();
        println!("Second temp path for RocksDB storage: {}", second_path.display());

        let sst_tempdir = tempfile::Builder::new()
            .prefix("_path_for_rocksdb_storage")
            .tempdir()
            .expect("Failed to create temporary path for the _path_for_rocksdb_storage");
        let sst_path = sst_tempdir.path().join("exported.sst");

        {
            let old_db = DB::open_default(path).unwrap();


            // Export SSTs from the old database to fresh files
            let sst_writer_options = Options::default();
            let mut sst_writer = SstFileWriter::create(&sst_writer_options);
            sst_writer.open(&sst_path).unwrap();
            for entry in old_db.iterator(IteratorMode::Start) {
                let (key, value) = entry.unwrap();
                sst_writer.put(&key, &value).unwrap();
            }
            sst_writer.finish().unwrap();
            println!("Exported SST file");


            let mut open_options = Options::default();
            open_options.prepare_for_bulk_load();
            open_options.create_if_missing(true);
            let db = DB::open(&open_options, second_path).unwrap();

            // Now ingest the newly created SST file
            let mut ingest_opts = IngestExternalFileOptions::default();
            ingest_opts.set_move_files(true);
            db.ingest_external_file_opts(&ingest_opts, vec![&sst_path]).unwrap();
            // Explicitly compact database as recommended by prepare_for_bulk_load
            db.compact_range::<Vec<u8>, Vec<u8>>(None, None);
            drop(db);

            // Reopen the database with default options (I have no idea if this is necessary)
            let db = DB::open_default(second_path).unwrap();
            println!("Live files: {:?}", db.live_files().unwrap());
            println!("Current entries in second db {:?}", db.iterator(IteratorMode::Start).count());
        }

        let _ = DB::destroy(&Options::default(), second_path);
        let _ = DB::destroy(&Options::default(), path);
    }
}
