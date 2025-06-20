use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use futures::stream::Fuse;
use futures::Stream;
use futures_util::StreamExt;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::streaming::model::stream_item::{Marker, StreamItem, StreamResult};

#[derive(Clone)]
pub enum OrdinalStreamItem {
    Marker(Marker),
    RecordBatch(usize, RecordBatch),
}

pub type OrdinalStreamResult = Result<OrdinalStreamItem, DataFusionError>;

pub trait ActionStream: Stream<Item=StreamResult> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;
}

pub type SendableActionStream = Pin<Box<dyn ActionStream + Send>>;

#[pin_project::pin_project]
pub struct ActionStreamAdapter<S> {
    schema: SchemaRef,

    #[pin]
    inner: S,
}

impl <S> ActionStreamAdapter<S> {
    pub fn new(schema: SchemaRef, inner: S) -> Self {
        Self {
            schema,
            inner,
        }
    }
}

impl <S> Stream for ActionStreamAdapter<S>
where S: Stream<Item=StreamResult>
{
    type Item = StreamResult;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}

impl <S> ActionStream for ActionStreamAdapter<S>
where S: Stream<Item=StreamResult>
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[pin_project::pin_project]
struct InputStream {
    ordinal: usize,
    blocked: bool,
    #[pin]
    stream: Fuse<SendableActionStream>,
}

#[pin_project::pin_project]
pub struct MergedActionStream {
    #[pin]
    inputs: Vec<InputStream>,
    current_checkpoint: Option<u64>,
    rng: rand::rngs::StdRng,
}

impl MergedActionStream {
    pub fn merge_inputs(inputs: Vec<(usize, SendableActionStream)>) -> Self {
        Self {
            inputs: inputs.into_iter()
                .map(|x| InputStream {
                    ordinal: x.0,
                    blocked: false,
                    stream: x.1.fuse()
                })
                .collect(),
            rng: rand::prelude::StdRng::seed_from_u64(1),
            current_checkpoint: None,
        }
    }
}

impl Stream for MergedActionStream {
    type Item = OrdinalStreamResult;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        let mut unblocked_inputs: Vec<_> = this.inputs.deref_mut().iter_mut()
            .filter(|input| !input.blocked)
            .collect();
        // Randomize the indexes into our streams array. This ensures that when
        // multiple streams are ready at the same time, we don't accidentally
        // exhaust one stream before another.
        unblocked_inputs.shuffle(&mut this.rng);

        // let mut non_blocked_indexes: Vec<_> = this.blocked_inputs.iter()
        //     .enumerate()
        //     .filter(|(_, blocked)| !**blocked)
        //     .map(|(index, _)| index)
        //     .collect();
        //
        // // Randomize the indexes into our streams array. This ensures that when
        // // multiple streams are ready at the same time, we don't accidentally
        // // exhaust one stream before another.
        // non_blocked_indexes.shuffle(&mut this.rng);

        // Iterate over our streams one-by-one. If a stream yields a value,
        // we exit early. By default, we'll return `Poll::Ready(None)`, but
        // this changes if we encounter a `Poll::Pending`.
        let mut some_pending = false;
        for input in unblocked_inputs {
            // let input = input.project();
            // let stream = this.inputs.deref_mut().get_mut(index).unwrap();
            let ordinal = input.ordinal;
            match input.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(StreamItem::RecordBatch(batch)))) => {
                    return Poll::Ready(Some(Ok(OrdinalStreamItem::RecordBatch(ordinal, batch))))
                },
                Poll::Ready(Some(Ok(StreamItem::Marker(marker)))) => {
                    // This stream should be added to the blocked list
                    input.blocked = true;

                    // Check if this is the first of the checkpoints we are seeing
                    if let Some(current_checkpoint) = this.current_checkpoint {
                        if marker.checkpoint_number != *current_checkpoint {
                            println!("ERROR encountered out of order checkpoints")
                        }
                    } else {
                        println!("Input stream discovered checkpoint {}", marker.checkpoint_number);
                        *this.current_checkpoint = Some(marker.checkpoint_number);
                    }

                    continue;
                },
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Some(Err(err))),
                Poll::Pending => {
                    some_pending = true;
                },
                Poll::Ready(None) => continue,
            }
        }

        if some_pending {
            return Poll::Pending;
        }

        // At this point, all inputs are either blocked, or finished. So we can reset all the
        // blocked inputs to false and pass the marker downstream
        if this.inputs.iter().any(|input| input.blocked) {
            if let Some(checkpoint_number) = this.current_checkpoint {
                let checkpoint_number = *checkpoint_number;

                // Reset state
                *this.current_checkpoint = None;
                for input in this.inputs.iter_mut() {
                    input.blocked = false;
                }

                println!("Input stream sending marker {} downstream", checkpoint_number);
                return Poll::Ready(Some(Ok(OrdinalStreamItem::Marker(Marker { checkpoint_number }))));
            }
        }

        // We have finished streaming all inputs
        Poll::Ready(None)
    }
}
