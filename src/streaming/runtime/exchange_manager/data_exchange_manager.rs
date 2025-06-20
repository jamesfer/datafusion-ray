use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use std::collections::{HashMap, VecDeque};
use futures::Stream;
use async_trait::async_trait;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use tonic::{Request, Response, Status};
use arrow_flight::Ticket;
use std::net::IpAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::Server;
use std::mem;
use std::ops::{Deref, DerefMut};
use futures_util::TryFutureExt;
use prost::Message;
use crate::flight::{DoGetStream, FlightHandler, FlightServ};
use crate::proto::generated::streaming::StreamingFlightTicketData;
use crate::streaming::model::stream_item::StreamItem;
use crate::streaming::partitioning::PartitionRange;
use crate::streaming::serialisation::flight_stream_serialization::encode_stream_to_flight;
use crate::streaming::runtime::DataChannelSender;
use crate::streaming::runtime::exchange_manager::data_channels::{ChannelPartitioningDetails, ExchangeChannelStore};
use crate::streaming::serialisation::proto_serialisation::SerialiseToProto;

pub struct DataExchangeManager {
    listening_address: String,
    exchange_channel_store: Arc<ExchangeChannelStore>,
    close_signal_sender: Option<tokio::sync::oneshot::Sender<()>>,
    handle: JoinHandle<Result<(), DataFusionError>>,
}

impl DataExchangeManager {
    pub async fn start(local_ip_address: IpAddr) -> Result<Self, DataFusionError> {
        let listener = Self::create_listener(local_ip_address).await?;
        let listening_address = listener.local_addr()
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .to_string();

        let exchange_channel_store = Arc::new(ExchangeChannelStore::new());
        let exchange_service = DataExchangeFlightService {
            exchange_channel_store: exchange_channel_store.clone(),
        };
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let flight_service_handle = Self::start_flight_service(listener, exchange_service, receiver).await?;

        Ok(Self {
            listening_address,
            exchange_channel_store,
            close_signal_sender: Some(sender),
            handle: flight_service_handle,
        })
    }

    pub fn get_exchange_address(&self) -> &str {
        self.listening_address.as_str()
    }

    pub async fn create_channel(&self, stream_id: String, partitioning_details: Option<ChannelPartitioningDetails>) -> DataChannelSender {
        self.exchange_channel_store.create(stream_id, partitioning_details).await
    }

    async fn create_listener(local_ip_address: IpAddr) -> Result<TcpListener, DataFusionError> {
        TcpListener::bind(&format!("{local_ip_address}:0"))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn start_flight_service(
        listener: TcpListener,
        exchange_service: DataExchangeFlightService,
        close_signal: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<JoinHandle<Result<(), DataFusionError>>, DataFusionError>{
        let flight_server = FlightServiceServer::new(FlightServ {
            handler: Arc::new(exchange_service),
        });

        let close_signal_future = async move {
            // Whether the close signal finishes with an error or not, we will shut down the server
            close_signal.await.unwrap_or(())
        };

        let result = tokio::spawn(
            Server::builder()
                .add_service(flight_server)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    close_signal_future,
                )
                .map_err(|transport_error| DataFusionError::External(Box::new(transport_error)))
        );
        Ok(result)
    }

    pub fn close(&mut self) {
        match self.close_signal_sender.take() {
            None => println!("Close signal sender was already taken, ignoring close request."),
            Some(sender) => {
                match sender.send(()) {
                    Ok(_) => {}
                    Err(_) => println!("Failed to send close signal, it may have already been closed.")
                }
            }
        }
    }
}

struct DataExchangeFlightService {
    exchange_channel_store: Arc<ExchangeChannelStore>,
}

#[async_trait]
impl FlightHandler for DataExchangeFlightService {
    async fn get_stream(&self, request: Request<Ticket>) -> Result<Response<DoGetStream>, Status> {
        let bytes = request.into_inner().ticket;
        let ticket = StreamingFlightTicketData::decode(bytes)
            .map_err(|decode_error| {
                Status::invalid_argument(format!("Failed to decode ticket data: {}", decode_error))
            })?;

        println!("Flight service received request for stream id: {}, partitions: {:?}",
            ticket.stream_id, ticket.partitions);

        // TODO
        let request_checkpoint = 123usize;
        let request_stream_id = ticket.stream_id;
        let request_partitions = ticket.partitions
            .map(PartitionRange::from_proto)
            .unwrap_or(PartitionRange::empty());

        let data_stream = self.exchange_channel_store.stream(&request_stream_id, &request_partitions).await
            .map_err(|e| Status::not_found(format!("Failed to get stream: {}", e)))?;
        Ok(Response::new(Box::pin(encode_stream_to_flight(data_stream))))
    }
}
