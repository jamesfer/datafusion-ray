use std::ffi::OsString;
use std::{fs, mem};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use pyo3::PyResult;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use crate::streaming::state::checkpoint_storage::ObjectStoreRef;
use crate::streaming::state::file_system::FileSystemStorage;

pub struct LocalRocksDBState {
    local_root_dir: PathBuf,
    local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    open_db: rocksdb::DB,
    working_dir: PathBuf,
}

impl LocalRocksDBState {
    pub async fn open_new(
        local_root_dir: PathBuf,
        local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    ) -> Result<Self, DataFusionError> {
        // Create the initial main directories to prevent directory does not exist errors
        local_file_system.mkdir_all(&Self::base_working_directory_path(&local_root_dir)).await?;
        local_file_system.mkdir_all(&Self::base_checkpoint_directory_path(&local_root_dir)).await?;

        let working_dir = Self::create_working_directory_path(&local_root_dir);
        let open_db = Self::open_rocksdb_database(&local_file_system, &working_dir)?;
        Ok(Self {
            local_root_dir,
            local_file_system,
            open_db,
            working_dir,
        })
    }

    pub fn put(&mut self, k: impl AsRef<[u8]>, v: impl AsRef<[u8]>) -> Result<(), DataFusionError> {
        self.open_db.put(k, v)
            .map_err(|e| internal_datafusion_err!("Failed to put key-value pair in RocksDB: {}", e))
    }

    pub fn get(&mut self, k: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, DataFusionError> {
        self.open_db.get(k)
            .map_err(|e| internal_datafusion_err!("Failed to get key from RocksDB: {}", e))
    }

    pub fn iterate(&self) -> impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), DataFusionError>> {
        self.open_db.iterator(rocksdb::IteratorMode::Start)
            .map(|entry| entry.map_err(|e| internal_datafusion_err!("Failed to iterate over RocksDB: {}", e)))
    }

    // Takes a mutable reference to ensure that we are the only user of the db when we flush it
    pub fn create_checkpoint(&mut self, checkpoint_id: &str) -> PathBuf {
        // It is necessary to flush all database mem-tables to disk before creating a checkpoint
        self.open_db.flush()
            .map_err(|e| internal_datafusion_err!("Failed to flush RocksDB: {}", e))?;

        // Creates a new directory, and writes the checkpoint to it. Rocksdb throws an error if the
        // directory already exists.
        let checkpoint_dir = self.create_checkpoint_directory_path(checkpoint_id);
        println!("Creating checkpoint directory at {} with {} entries", checkpoint_dir.display(), self.open_db.iterator(rocksdb::IteratorMode::Start).count());
        self.create_local_rocksdb_checkpoint(&checkpoint_dir)?;
        checkpoint_dir
    }

    pub async fn load_from_checkpoint_parts(&mut self, checkpoint_part_paths: Vec<PathBuf>) -> Result<(), DataFusionError> {
        let new_working_dir = Self::create_working_directory_path(&self.local_root_dir);

        let new_db = if checkpoint_part_paths.len() == 1 {
            // Fast path when there is only one checkpoint
            println!("Copying single checkpoint from {} to {}", &checkpoint_part_paths[0].display(), new_working_dir.display());
            self.local_file_system.copy(Path::new(&checkpoint_part_paths[0]), &new_working_dir).await?;
            let new_db = Self::open_rocksdb_database(self.local_file_system.as_ref(), &new_working_dir)?;
            println!("Opened new RocksDB database with {} entries", new_db.iterator(rocksdb::IteratorMode::Start).count());
            new_db
        } else {
            // For each checkpoint, we need to open it, read all the keys that are in the partition
            // range, write out the SST file, then ingest it into the new database.
            let sst_dir = tempfile::TempDir::new()?;
            let options = rocksdb::Options::default();
            let mut sst_writer = rocksdb::SstFileWriter::create(&options);

            let mut sst_paths = Vec::with_capacity(checkpoint_part_paths.len());
            for checkpoint_path in &checkpoint_part_paths {
                let sst_path = sst_dir.path().join(checkpoint_path);
                tokio::fs::create_dir_all(&sst_path).await?;
                sst_writer.open(&sst_path).to_datafusion_result()?;

                let checkpoint_part_db = Self::open_rocksdb_database(self.local_file_system.as_ref(), checkpoint_path)?;
                for entry in checkpoint_part_db.iterator(rocksdb::IteratorMode::Start) {
                    // TODO filter keys by partition range to prevent unnecessary writes and bugs
                    let (key, value) = entry.to_datafusion_result()?;
                    sst_writer.put(&key, &value).to_datafusion_result()?;
                }
                sst_writer.finish().to_datafusion_result()?;
                sst_paths.push(sst_path);
            }

            // Ingest all the SST files into the new database
            let db = Self::open_rocksdb_database(self.local_file_system.as_ref(), &new_working_dir)?;
            db.ingest_external_file(sst_paths).to_datafusion_result()?;

            db
        };

        // Clean up the old working directory
        let old_working_dir = mem::replace(&mut self.working_dir, new_working_dir);
        let old_db = mem::replace(&mut self.open_db, new_db);
        // Explicitly drop the old database instance before removing the directory
        drop(old_db);
        self.destroy_rocksdb(&old_working_dir)?;

        Ok(())
    }

    fn create_local_rocksdb_checkpoint(&self, checkpoint_dir: impl AsRef<Path>) -> Result<(), DataFusionError> {
        let absolute_checkpoint_dir = self.local_file_system.get_physical_path(&checkpoint_dir)?;
        rocksdb::checkpoint::Checkpoint::new(&self.open_db)
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
        rocksdb::DB::destroy(&options, &absolute_old_working_dir).to_datafusion_result()
    }

    fn create_working_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        Self::base_working_directory_path(&root_dir).join(uuid::Uuid::new_v4().to_string())
    }

    fn base_working_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        root_dir.as_ref().join("working")
    }

    pub fn create_checkpoint_directory_path(&self, checkpoint: &str) -> PathBuf {
        Self::base_checkpoint_directory_path(&self.local_root_dir)
            .join(format!("{}__unique-suffix_{}", checkpoint, uuid::Uuid::new_v4()))
    }

    fn base_checkpoint_directory_path(root_dir: impl AsRef<Path>) -> PathBuf {
        root_dir.as_ref().join("checkpoint")
    }
}

pub trait RocksDBResultToDataFusionResult<T> {
    fn to_datafusion_result(self) -> Result<T, DataFusionError>;
}

impl<T> RocksDBResultToDataFusionResult<T> for Result<T, rocksdb::Error> {
    fn to_datafusion_result(self) -> Result<T, DataFusionError> {
        self.map_err(|err| DataFusionError::External(Box::new(err)))
    }
}
