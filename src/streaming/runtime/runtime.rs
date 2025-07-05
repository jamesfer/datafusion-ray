use datafusion::common::DataFusionError;
use std::net::IpAddr;
use std::sync::Arc;
use crate::streaming::runtime::data_client_manager::DataClientManager;
use crate::streaming::runtime::DataExchangeManager;
use crate::streaming::state::remote_checkpoint_storage::RemoteCheckpointStorage;
use crate::streaming::state::file_system::FileSystemStorage;

pub struct Runtime {
    data_client_manager: DataClientManager,
    data_exchange_manager: DataExchangeManager,
    local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
    remote_checkpoint_file_system: Arc<RemoteCheckpointStorage>,
}

impl Runtime {
    pub async fn start(
        local_ip_address: IpAddr,
        local_file_system: Arc<dyn FileSystemStorage + Send + Sync>,
        remote_checkpoint_file_system: Arc<RemoteCheckpointStorage>,
    ) -> Result<Self, DataFusionError> {
        let data_exchange_manager = DataExchangeManager::start(local_ip_address).await?;
        Ok(Self {
            data_client_manager: DataClientManager::new(),
            data_exchange_manager,
            local_file_system,
            remote_checkpoint_file_system,
        })
    }

    pub fn data_client_manager(&self) -> &DataClientManager {
        &self.data_client_manager
    }
    pub fn data_exchange_manager(&self) -> &DataExchangeManager {
        &self.data_exchange_manager
    }
    pub fn local_file_system(&self) -> &Arc<dyn FileSystemStorage + Send + Sync> {
        &self.local_file_system
    }
    pub fn remote_checkpoint_file_system(&self) -> &Arc<RemoteCheckpointStorage> {
        &self.remote_checkpoint_file_system
    }
}
