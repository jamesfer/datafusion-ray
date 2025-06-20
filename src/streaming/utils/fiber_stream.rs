use async_stream::{stream, try_stream};
use async_trait::async_trait;
use futures::Stream;
use futures_util::StreamExt;
use datafusion::common::DataFusionError;
use crate::streaming::utils::sync_safe::SyncSafe;

#[async_trait]
pub trait FiberStream: Send + Sync {
    type Item: Sync + Send;

    // TODO how to indicate that the stream is complete (probably an empty vector)
    async fn pull<'a>(&'a mut self) -> Result<Vec<Box<dyn Stream<Item=Self::Item> + Sync + Send + 'a>>, DataFusionError>;

    fn combined<'a>(mut self: Box<Self>) -> Result<Box<dyn Stream<Item=Self::Item> + Sync + Send + 'a>, DataFusionError>
    where
        Self: 'a,
        Self::Item: 'a
    {
        Ok(Box::new(stream! {
            loop {
                // TODO handle errors properly
                let pull_future = SyncSafe::new(self.pull());
                let fibers = pull_future.await.unwrap();
                if fibers.is_empty() {
                    break;
                }

                let mut merged_stream = futures::stream::select_all(fibers.into_iter().map(|b| Box::into_pin(b)));
                while let Some(next) = merged_stream.next().await {
                    yield next;
                }
            }
        }) as Box<dyn Stream<Item=Self::Item> + Sync + Send + 'a>)
    }
}

pub struct SingleFiberStream<S> {
    stream: Option<S>,
}

impl <S> SingleFiberStream<S> {
    pub fn new(stream: S) -> Self {
        Self { stream: Some(stream) }
    }
}

#[async_trait]
impl <S> FiberStream for SingleFiberStream<S>
where
    S: Stream + Sync + Send,
    S::Item: Sync + Send,
{
    type Item = S::Item;

    async fn pull<'a>(&'a mut self) -> Result<Vec<Box<dyn Stream<Item=Self::Item> + Sync + Send + 'a>>, DataFusionError> {
        Ok(match self.stream.take() {
            Some(stream) => vec![Box::new(stream)],
            None => vec![],
        })
    }
}
