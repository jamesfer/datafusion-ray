use std::ffi::OsString;
use std::{fs, mem};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use futures::{StreamExt, TryStreamExt};
use futures_util::TryFutureExt;
use object_store::ObjectStore;
use rocksdb::checkpoint::Checkpoint;
use rocksdb::{IteratorMode, Options, SstFileWriter};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::state::checkpoint_storage::{FileSystemStateStorage, ObjectStoreRef};
use crate::streaming::state::file_system::FileSystemStorage;

pub struct RocksDBStateBackend {
    state_id: String,
    local_root_dir: OsString,
    partitions: PartitionRange,
    remote_checkpoint_storage: Arc<FileSystemStateStorage>,
    local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    working_dir: OsString,
    open_db: rocksdb::DB,
    background_task: JoinHandle<()>,
    background_sync_channel: Sender<(String, PartitionRange, PathBuf)>,
}

impl RocksDBStateBackend {
    pub async fn open_new(
        state_id: String,
        partitions: PartitionRange,
        remote_checkpoint_storage: Arc<FileSystemStateStorage>,
        local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    ) -> Result<Self, DataFusionError> {
        let root_dir = Self::create_root_directory_path(&state_id, &partitions);

        // Create the initial main directories to prevent directory does not exist errors
        local_file_system.mkdir_all(&Self::base_working_directory_path(&root_dir)).await?;
        local_file_system.mkdir_all(&Self::base_checkpoint_directory_path(&root_dir)).await?;

        // Open the main database
        let working_dir = Self::create_working_directory_path(&root_dir);
        let db = Self::open_rocksdb_database(local_file_system.as_ref(), &working_dir)?;

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let background_task = tokio::spawn({
            let state_id = state_id.clone();
            let remote_checkpoint_storage = remote_checkpoint_storage.clone();
            let local_file_system = local_file_system.clone();
            async move {
                RocksDBStateBackend::async_checkpoint_background_task(
                    &state_id,
                    receiver,
                    local_file_system,
                    &remote_checkpoint_storage,
                ).await
            }
        });

        Ok(Self {
            state_id,
            local_root_dir: root_dir.into_os_string(),
            partitions,
            remote_checkpoint_storage,
            local_file_system,
            working_dir: working_dir.into_os_string(),
            open_db: db,
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
        self.open_db.put(k, v)
            .map_err(|e| internal_datafusion_err!("Failed to put key-value pair in RocksDB: {}", e))
    }

    pub fn get(&mut self, k: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, DataFusionError> {
        self.open_db.get(k)
            .map_err(|e| internal_datafusion_err!("Failed to get key from RocksDB: {}", e))
    }

    pub fn iterate(&self) -> impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), DataFusionError>> {
        self.open_db.iterator(IteratorMode::Start)
            .map(|entry| entry.map_err(|e| internal_datafusion_err!("Failed to iterate over RocksDB: {}", e)))
    }

    pub async fn checkpoint(
        &mut self,
        checkpoint_id: usize,
        partition_range: PartitionRange
    ) -> Result<(), DataFusionError> {
        // It is necessary to flush all database mem-tables to disk before creating a checkpoint
        self.open_db.flush()
            .map_err(|e| internal_datafusion_err!("Failed to flush RocksDB: {}", e))?;

        // Creates a new directory, and writes the checkpoint to it. Rocksdb throws an error if the
        // directory already exists.
        let checkpoint_dir = Self::create_checkpoint_directory_path(&self.local_root_dir, checkpoint_id);
        println!("Creating checkpoint directory at {} with {} entries", checkpoint_dir.display(), self.open_db.iterator(IteratorMode::Start).count());
        self.create_local_rocksdb_checkpoint(&checkpoint_dir)?;

        // Sends the checkpoint to the background task to persist it to the remote file system
        self.background_sync_channel.send((
            format!("{}", checkpoint_id), // TODO make all checkpoint_ids strings
            partition_range,
            checkpoint_dir,
        )).await
            .map_err(|e| internal_datafusion_err!("Failed to send checkpoint to background task: {}", e))?;

        Ok(())
    }

    pub async fn move_to_checkpoint(&mut self, checkpoint: usize, partition_range: PartitionRange) -> Result<(), DataFusionError> {
        println!("Attempting to move to checkpoint {} with partition range {:?} (state id {})", checkpoint, partition_range, self.state_id);
        // Find the directory for the checkpoint with a fairly inefficient search
        let checkpoints = self.find_checkpoint(checkpoint, partition_range).await?;

        // Copy the checkpoint into another working directory
        // We need to download the checkpoints first. This is also inefficient as we end up
        // downloading the checkpoints before copying them into the working directory.
        let mut paths = Vec::with_capacity(checkpoints.len());
        for (checkpoint_part_id, _partition_range) in checkpoints {
            let remote_checkpoint_dir = self.remote_checkpoint_storage.get_checkpoint_file_system(&self.state_id, &checkpoint_part_id);
            let remote_path = object_store::path::Path::default();
            let destination_path = Self::create_checkpoint_directory_path(&self.local_root_dir, checkpoint);
            Self::download_dir(
                &remote_checkpoint_dir,
                &remote_path,
                self.local_file_system.as_ref(),
                &destination_path,
            ).await?;
            paths.push(destination_path);
        }
        let paths = paths;

        println!("Found {} checkpoints to use. Paths {:?}", paths.len(), paths);

        // Now we need to merge all of the partial checkpoints into a single working directory.
        let new_working_dir = Self::create_working_directory_path(&self.local_root_dir);
        let new_db = if paths.len() == 1 {
            // Fast path when there is only one checkpoint
            println!("Copying single checkpoint from {} to {}", &paths[0].display(), new_working_dir.display());
            self.local_file_system.copy(Path::new(&paths[0]), &new_working_dir).await?;
            let new = Self::open_rocksdb_database(self.local_file_system.as_ref(), &new_working_dir)?;
            println!("Opened new RocksDB database with {} entries", new.iterator(IteratorMode::Start).count());
            new
        } else {
            // For each checkpoint, we need to open it, read all the keys that are in the partition
            // range, write out the SST file, then ingest it into the new database.
            let sst_dir = tempfile::TempDir::new()?;
            let options = Options::default();
            let mut sst_writer = SstFileWriter::create(&options);

            let mut sst_paths = Vec::with_capacity(paths.len());
            for checkpoint_path in &paths {
                let sst_path = sst_dir.path().join(checkpoint_path);
                fs::create_dir_all(&sst_path)?;

                sst_writer.open(&sst_path).unwrap();

                let checkpoint_db = Self::open_rocksdb_database(self.local_file_system.as_ref(), checkpoint_path)?;
                for entry in checkpoint_db.iterator(IteratorMode::Start) {
                    // TODO filter keys by partition range to prevent unnecessary writes and bugs
                    let (key, value) = entry.unwrap();
                    sst_writer.put(&key, &value).unwrap();
                }
                sst_writer.finish()
                    .map_err(|err| DataFusionError::External(Box::new(err)))?;
                sst_paths.push(sst_path);
            }

            // Ingest all the SST files into the new database
            let db = Self::open_rocksdb_database(self.local_file_system.as_ref(), &new_working_dir)?;
            db.ingest_external_file(sst_paths)
                .map_err(|err| DataFusionError::External(Box::new(err)))?;

            db
        };

        // Clean up the old working directory
        let old_working_dir = mem::replace(&mut self.working_dir, new_working_dir.into_os_string());
        let old_db = mem::replace(&mut self.open_db, new_db);
        drop(old_db);
        self.destroy_rocksdb(&old_working_dir)?;

        Ok(())
    }

    async fn async_checkpoint_background_task(
        state_id: &str,
        mut checkpoints: tokio::sync::mpsc::Receiver<(String, PartitionRange, PathBuf)>,
        local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
        remote_checkpoint_storage: &FileSystemStateStorage,
    ) {
        // Uploads checkpoints in the background to the remote file system
        while let Some((checkpoint_id, partition_range, next_path)) = checkpoints.recv().await {
            Self::upload_checkpoint(
                state_id,
                &checkpoint_id,
                &next_path,
                partition_range,
                local_file_system.as_ref(),
                remote_checkpoint_storage,
            ).await.unwrap_or_else(|e| {
                eprintln!("Failed to upload checkpoint directory {}: {}", next_path.to_string_lossy(), e);
            });
        }
    }

    async fn upload_checkpoint(
        state_id: &str,
        checkpoint_id: &str,
        checkpoint_path: &Path,
        partition_range: PartitionRange,
        local_file_system: &(dyn FileSystemStorage + Send + Sync),
        remote_checkpoint_storage: &FileSystemStateStorage,
    ) -> Result<(), DataFusionError> {
        // Create a new checkpoint
        let checkpoint_part_id = remote_checkpoint_storage.start_checkpoint_part(state_id).await?;
        let checkpoint_part_file_system = remote_checkpoint_storage.get_checkpoint_file_system(state_id, &checkpoint_part_id);

        // Upload the checkpoint directory to the remote file system
        let destination_path = object_store::path::Path::default();
        println!("Uploading checkpoint {} from {} to remote storage with remote id {}", checkpoint_id, checkpoint_path.display(), checkpoint_part_id);
        Self::upload_dir(
            local_file_system,
            checkpoint_path,
            &checkpoint_part_file_system,
            &destination_path,
        ).await?;

        // Finish the checkpoint on the remote file system
        remote_checkpoint_storage.complete_checkpoint(state_id, checkpoint_id, checkpoint_part_id, partition_range).await?;

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

    // async fn copy_dir_between_systems(
    //     source_system: &(dyn FileSystemStorage + Send + Sync),
    //     source_dir: impl AsRef<Path>,
    //     destination_system: &(dyn FileSystemStorage + Send + Sync),
    //     dest_dir: impl AsRef<Path>,
    // ) -> Result<(), DataFusionError> {
    //     let source_dir = source_dir.as_ref();
    //     let dest_dir = dest_dir.as_ref();
    //
    //     destination_system.mkdir_all(&dest_dir).await?;
    //
    //     let mut queue = vec![PathBuf::from("")];
    //     while let Some(next) = queue.pop() {
    //         let next_source_path = source_dir.join(&next);
    //         for entry in source_system.list_files(&next_source_path).await? {
    //             let entry_source_path = source_dir.join(&next).join(&entry.name);
    //             let entry_dest_path = dest_dir.join(&next).join(&entry.name);
    //
    //             if entry.directory {
    //                 destination_system.mkdir_all(&entry_dest_path).await?;
    //                 queue.push(next.join(&entry.name));
    //             } else {
    //                 let contents = source_system.read_file(&entry_source_path).await?;
    //                 destination_system.write_file(&entry_dest_path, &contents).await?;
    //             }
    //         }
    //     }
    //
    //     Ok(())
    // }

    async fn find_checkpoint(&self, checkpoint: usize, partition_range: PartitionRange) -> Result<Vec<(String, PartitionRange)>, DataFusionError> {
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
        let remote_checkpoints = self.remote_checkpoint_storage.get_operator_checkpoints(
            &self.state_id,
            &self.partitions,
            &format!("{}", checkpoint),
        ).await?;
        Ok(remote_checkpoints)
    }

    fn create_local_rocksdb_checkpoint(&self, checkpoint_dir: &PathBuf) -> Result<(), DataFusionError> {
        let absolute_checkpoint_dir = self.local_file_system.get_physical_path(&checkpoint_dir)?;
        Checkpoint::new(&self.open_db)
            .map_err(|e| internal_datafusion_err!("Failed to create checkpoint: {}", e))?
            .create_checkpoint(&absolute_checkpoint_dir)
            .map_err(|e| internal_datafusion_err!("Failed to create checkpoint at {}: {}", absolute_checkpoint_dir.display(), e))?;
        println!("Created RocksDB checkpoint at {}", absolute_checkpoint_dir.display());
        Ok(())
    }

    fn open_rocksdb_database(file_system: &dyn FileSystemStorage, working_dir: impl AsRef<Path>) -> Result<rocksdb::DB, DataFusionError> {
        let absolute_working_dir = file_system.get_physical_path(working_dir.as_ref())?;
        rocksdb::DB::open_default(&absolute_working_dir)
            .map_err(|e| internal_datafusion_err!("Failed to open RocksDB: {}", e))
    }

    fn destroy_rocksdb(&mut self, working_dir: impl AsRef<Path>) -> Result<(), DataFusionError> {
        let options = rocksdb::Options::default();
        let absolute_old_working_dir = self.local_file_system.get_physical_path(working_dir.as_ref())?;
        rocksdb::DB::destroy(&options, &absolute_old_working_dir)
            .map_err(|e| internal_datafusion_err!("Failed to destroy old RocksDB at {}: {}", working_dir.as_ref().to_string_lossy(), e))
    }

    fn create_root_directory_path(state_id: &String, partitions: &PartitionRange) -> PathBuf {
        Path::new(state_id).join(format!(
            "partition-{}-{}-{}",
            partitions.start(),
            partitions.end(),
            partitions.partitions(),
        ))
    }

    fn base_working_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        root_dir.as_ref().join("working")
    }

    fn create_working_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        Self::base_working_directory_path(&root_dir).join(uuid::Uuid::new_v4().to_string())
    }

    fn base_checkpoint_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        root_dir.as_ref().join("checkpoint")
    }

    fn create_checkpoint_directory_path(root_dir: impl AsRef<Path>, checkpoint: usize) -> PathBuf {
        Self::base_checkpoint_directory_path(&root_dir)
            .join(format!("{}__{}", checkpoint, uuid::Uuid::new_v4()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use object_store::local::LocalFileSystem;
    use rocksdb::{IngestExternalFileOptions, IteratorMode, Options, SstFileWriter, DB};
    use rocksdb::checkpoint::Checkpoint;
    use crate::streaming::partitioning::PartitionRange;
    use crate::streaming::state::checkpoint_storage::FileSystemStateStorage;
    use crate::streaming::state::file_system::PrefixedLocalFileSystemStorage;
    use crate::streaming::state::rocksdb_state_backend::RocksDBStateBackend;
    use crate::streaming::utils::retry::retry_future;

    #[tokio::test]
    async fn storage_test() {
        let database_dir = tempfile::tempdir().unwrap();
        let backup_dir = tempfile::tempdir().unwrap();

        let database_file_system = Arc::new(PrefixedLocalFileSystemStorage::new(database_dir.path()));
        let backup_object_store = Arc::new(LocalFileSystem::new_with_prefix(backup_dir.path()).unwrap());
        let remote_checkpoint_storage = Arc::new(FileSystemStateStorage::new(
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
            remote_checkpoint_storage.get_latest_operator_checkpoints("test_state", &partition_range)
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
