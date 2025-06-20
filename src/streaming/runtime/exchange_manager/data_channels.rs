use crate::streaming::model::stream_item::StreamItem;
use crate::streaming::partitioning::{filter_by_partition_range, PartitionRange, PartitioningSpec};
use crate::streaming::runtime::exchange_manager::once_notify::OnceNotify;
use datafusion::common::{internal_datafusion_err, DataFusionError};
use futures::Stream;
use futures::StreamExt;
use futures_util::FutureExt;
use std::collections::{HashMap, VecDeque};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::RwLock;

struct DataChannelState {
    data: std::sync::RwLock<(VecDeque<StreamItem>, Option<Arc<OnceNotify>>)>,
    status: std::sync::RwLock<ChannelStatus>,
}

pub struct DataChannelSender {
    state: Arc<DataChannelState>,
    next_index: usize,
    last_checkpoint: usize,
}

impl DataChannelSender {
    fn new(state: Arc<DataChannelState>) -> Self {
        Self {
            state,
            next_index: 0,
            last_checkpoint: 0,
        }
    }

    pub async fn send(&mut self, item: Result<StreamItem, DataFusionError>) {
        match item {
            Ok(item) => {
                let notification = {
                    let mut data_state = self.state.data.write().unwrap();
                    let (vec, notification) = data_state.deref_mut();

                    // Write the item to the channel
                    self.next_index += 1;
                    if let StreamItem::Marker(marker) = &item {
                        self.last_checkpoint = marker.checkpoint_number as usize;
                    }
                    vec.push_back(item);

                    // Notify all waiters and reset the notification
                    mem::replace(notification, Some(Arc::new(OnceNotify::new())))
                };
                // Notification is only ever None after the channel has been dropped
                notification.unwrap().notify();
            },
            Err(err) => self.cancel_with_status(ChannelStatus::Error(Arc::new(err))),
        }
    }

    fn cancel_with_status(&mut self, channel_status: ChannelStatus) {
        {
            let mut status = self.state.status.write().unwrap();
            *status = channel_status;
        }
        let notification = {
            let mut data = self.state.data.write().unwrap();
            let (_, notification) = data.deref_mut();
            mem::replace(notification, None)
        };
        notification.map(|n| n.notify());
    }
}

impl Drop for DataChannelSender {
    fn drop(&mut self) {
        self.cancel_with_status(ChannelStatus::Cancelled {
            last_checkpoint: self.last_checkpoint,
            valid_until: self.next_index,
        });
        println!("Dropping DataChannelSender. last_checkpoint: {}, valid_until: {}", self.last_checkpoint, self.next_index);
    }
}

enum ChannelStatus {
    Running,
    Finished,
    Error(Arc<DataFusionError>),
    Cancelled { last_checkpoint: usize, valid_until: usize,},
}

// An array of data that allows you to wait for the next element to be available
struct DataChannel {
    state: Arc<DataChannelState>,
}

impl DataChannel {
    fn create() -> (Self, DataChannelSender) {
        let state = Arc::new(DataChannelState {
            // sender_alive: AtomicBool::new(true),
            data: std::sync::RwLock::new((VecDeque::new(), Some(Arc::new(OnceNotify::new())))),
            status: std::sync::RwLock::new(ChannelStatus::Running),
        });

        let channel = Self::new(state.clone());
        let sender = DataChannelSender::new(state);

        (channel, sender)
    }

    fn new(state: Arc<DataChannelState>) -> Self {
        Self { state }
    }

    // This is explicitly marked as 'static to tell rust that it doesn't use the lifetime of &self
    pub fn stream(&self, partitioning_filter: Option<ChannelPartitioningDetails>) -> impl Stream<Item=Result<StreamItem, Arc<DataFusionError>>> + Sync + Send + 'static + use<> {
        let state = self.state.clone();

        let s = futures_util::stream::try_unfold(0, move |index| {
            let state = state.clone();
            async move {
                loop {
                    // Check the state
                    let is_finished = {
                        let status = state.status.read().unwrap();
                        match *status.deref() {
                            ChannelStatus::Running => false,
                            ChannelStatus::Finished => true,
                            // Immediately return the error
                            ChannelStatus::Error(ref e) => return Err(e.clone()),
                            ChannelStatus::Cancelled { last_checkpoint, valid_until } => {
                                println!("Checking stream status, cancellation at checkpoint {}, valid until index {}, current index {}", last_checkpoint, valid_until, index);

                                // Check if we have passed the last completed checkpoint
                                if index == valid_until {
                                    return Ok(None);
                                } else if index >= valid_until {
                                    return Err(Arc::new(DataFusionError::Execution(format!(
                                        "Channel was cancelled at checkpoint {}, last valid index {}",
                                        last_checkpoint, valid_until
                                    ))));
                                } else {
                                    true
                                }
                            }
                        }
                    };

                    // Attempt to read the next item
                    let next_item_result = {
                        let guard = state.data.read().unwrap();
                        let (vec, notification) = guard.deref();
                        vec.get(index)
                            .cloned()
                            .ok_or_else(|| notification.clone())
                    };
                    match next_item_result {
                        // Item is ready, return it
                        Ok(item) => {
                            return Ok(Some((item, index + 1)));
                        },
                        // No item was ready
                        Err(Some(notification)) => {
                            if is_finished {
                                // If the channel is finished, return None because no other items
                                // are coming
                                return Ok(None);
                            }
                            // Otherwise, wait for the next notification
                            println!("Waiting for next item on channel, current index: {}", index);
                            notification.wait().await;
                        },
                        Err(None) => {
                            // If there is no notification, it means the channel has been closed
                            return Err(Arc::new(DataFusionError::Execution("Channel has been closed".to_string())));
                        }
                    }
                }
            }
        });
            s.map(move |result| {
                match &partitioning_filter {
                    None => result,
                    Some(partitioning_filter) => {
                        match result {
                            Err(e) => Err(e),
                            Ok(StreamItem::Marker(marker)) => Ok(StreamItem::Marker(marker.clone())),
                            Ok(StreamItem::RecordBatch(record_batch)) => {
                                let filtered_record_batch = filter_by_partition_range(&record_batch, &partitioning_filter.partitions, &partitioning_filter.partitioning_spec.expressions)?;
                                Ok(StreamItem::RecordBatch(filtered_record_batch))
                            },
                        }
                    }
                }
            })
    }
}

struct OutputStream {
    channel: DataChannel,
    partitioning_details: Option<ChannelPartitioningDetails>,
}

#[derive(Clone)]
pub struct ChannelPartitioningDetails {
    pub partitioning_spec: PartitioningSpec,
    pub partitions: PartitionRange,
}

pub struct ExchangeChannelStore {
    channels: RwLock<HashMap<String, OutputStream>>,
}

impl ExchangeChannelStore {
    pub(crate) fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    pub async fn create(&self, stream_id: String, partitioning_details: Option<ChannelPartitioningDetails>) -> DataChannelSender {
        let (channel, sender) = DataChannel::create();
        let output_stream = OutputStream {
            channel,
            partitioning_details,
        };

        let mut channels = self.channels.write().await;
        channels.insert(stream_id, output_stream);
        sender
    }

    pub async fn stream(&self, stream_id: &str, partitions: &PartitionRange) -> Result<impl Stream<Item=Result<StreamItem, Arc<DataFusionError>>> + Sync + Send + use<>, DataFusionError> {
        let channels = self.channels.read().await;
        let channel = channels.get(stream_id)
            .ok_or_else(|| internal_datafusion_err!("No channel found for stream id: {}", stream_id))?;

        let partitioning_filter = match &channel.partitioning_details {
            None => {
                if !partitions.is_empty() {
                    return Err(internal_datafusion_err!(
                        "A partitioned stream was requested for a non partitioned output: {}",
                        stream_id
                    ));
                }

                println!("Starting stream for {} with no partitioning filter", stream_id);
                None
            },
            Some(details) => {
                let intersection = details.partitions.intersection(partitions);
                if intersection.is_empty() {
                    return Err(internal_datafusion_err!(
                        "Requested partitions {:?} do not intersect available partitions {:?}",
                        partitions, details.partitions
                    ));
                }

                Some(ChannelPartitioningDetails {
                    partitioning_spec: details.partitioning_spec.clone(),
                    partitions: intersection,
                })
            },
        };

        let stream_id = stream_id.to_string();
        let stream = channel.channel.stream(partitioning_filter)
            .chain(async move {
                println!("Exchange channel store finished streaming all results over the network for stream: {}", stream_id);
                futures::stream::empty()
            }.flatten_stream());
        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use crate::streaming::model::stream_item::StreamItem;
    use crate::streaming::partitioning::PartitionRange;
    use crate::streaming::runtime::exchange_manager::data_channels::ExchangeChannelStore;
    use datafusion::common::record_batch;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn fails_when_stream_id_does_not_exist() {
        let store = ExchangeChannelStore::new();
        let _sender = store.create("test_stream".to_string(), None).await;
        let result = store.stream("other_stream", &PartitionRange::empty()).await;
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("No channel found for stream id: other_stream"));
    }

    #[tokio::test]
    async fn fails_when_partitions_do_not_match() {
        let store = ExchangeChannelStore::new();
        let _sender = store.create("test_stream".to_string(), None).await;
        let result = store.stream("test_stream", &PartitionRange::empty()).await;
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("Requested partitions [1] do not match available partitions [0]"));
    }

    #[tokio::test]
    async fn can_create_and_stream_from_channel() {
        let record_batch_1 = record_batch!(("a", Int32, vec![1, 2, 3])).unwrap();
        let record_batch_2 = record_batch!(("a", Int32, vec![4, 5, 6])).unwrap();
        let store = ExchangeChannelStore::new();

        // Ensure the sender is dropped
        {
            let mut sender = store.create("test_stream".to_string(), None).await;
            sender.send(Ok(StreamItem::RecordBatch(record_batch_1.clone()))).await;
            sender.send(Ok(StreamItem::RecordBatch(record_batch_2.clone()))).await;
        }

        // Stream the items
        let mut stream = Box::pin(store.stream("test_stream", &PartitionRange::empty()).await.unwrap());

        assert_eq!(stream.next().await.unwrap().unwrap(), StreamItem::RecordBatch(record_batch_1));
        assert_eq!(stream.next().await.unwrap().unwrap(), StreamItem::RecordBatch(record_batch_2));
        assert!(stream.next().await.is_none());
    }
}
