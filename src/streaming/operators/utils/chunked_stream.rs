use std::cell::RefMut;
use std::ops::DerefMut;
use std::pin::{pin, Pin};
use std::task::{Context, Poll};
use arrow::array::RecordBatch;
use futures::future::ready;
use datafusion::common::DataFusionError;
use futures::Stream;
use futures::StreamExt;
use futures_util::FutureExt;
use futures_util::stream::{iter, FusedStream};
use crate::streaming::model::sitem::SItem;
use crate::streaming::model::stream_item::Marker;
use crate::streaming::operators::utils::borrowed_stream::BorrowedStream;

// struct Guard<'a> {
//
// }
//
// impl <'a, T> Guard<'a, T> {
//     pub fn new(cell: Arc<Cell<Option<T>>>) -> Self {
//         Self {
//
//         }
//     }
// }
//
// struct Loanable<T> {
//     cell: Arc<Cell<Option<T>>>,
// }
//
// impl<T> Loanable<T> {
//     pub fn new(value: T) -> Self {
//         Self {
//             cell: Arc::new(Cell::new(Some(value))),
//         }
//     }
//
//     pub fn loan(&mut self) -> Guard {
//         self.cell.take()
//     }
// }

// #[pin_project]
// struct SB<'a, S> {
//     #[pin]
//     borrowed: ,
// }
//
// impl <'a, S> SB<'a, S> {
//     pub fn new(borrowed: &'a mut S) -> Self {
//         Self { borrowed }
//     }
// }
//
// impl <'a, S, T> Stream for SB<'a, S>
// where S: Stream<Item=T>
// {
//     type Item = SItem;
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
//         let this = self.project();
//         this.borrowed.poll_next(cx)
//     }
// }


// // Didn't work because pin_project prevents us from implementing Drop
// // #[pin_project]
// struct SB<'a, S> {
//     // #[pin]
//     stream: Pin<Box<&'a mut S>>,
//     return_cell: Arc<Cell<Option<S>>>,
// }
//
// impl <S> Drop for SB<S> {
//     fn drop(&mut self) {
//         // Return the value to the cell
//         self.return_cell.set(self.stream.take());
//     }
// }
//
// impl <S> Stream for SB<S>
// where S: Stream
// {
//     type Item = ();
//
//     fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         self.stream.poll_next(cx)
//     }
// }

struct StreamChannel<'a> {
    channel: RefMut<'a, flume::Receiver<SItem>>,
}

impl <'a> Stream for StreamChannel<'a>
{
    type Item = SItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let fut = self.channel.deref_mut().recv_async();
        pin!(fut).poll(cx).map(|result| result.ok())
    }
}

pub struct ChunkedStream<S> {
    stream: Pin<Box<S>>,
    current_marker: Option<Result<Marker, GenerationMarker>>,
}

// impl FusedStream for ChunkedStream {
//     fn is_terminated(&self) -> bool {
//         // TODO
//         false
//     }
// }

#[derive(PartialEq, Debug)]
pub struct GenerationMarker {}

impl <S> ChunkedStream<S>
where S: Stream<Item=SItem>
{
    pub fn new(stream: Pin<Box<S>>) -> Self {
        Self {
            stream,
            current_marker: None,
        }
    }

    // Stream record batches from the inner stream until a marker is found
    pub fn stream_record_batches<'a>(&'a mut self) -> impl Stream<Item=RecordBatch> + 'a {
        BorrowedStream::new(&mut self.stream)
            .map(|item| {
                match item {
                    SItem::RecordBatch(record_batch) => {
                        self.current_marker = None;
                        Some(record_batch)
                    },
                    SItem::Marker(marker) => {
                        self.current_marker = Some(Ok(marker));
                        None
                    },
                    SItem::Generation(generation) => {
                        // TODO
                        // self.current_marker = Some(Err(generation));
                        None
                    },
                }
            })
            .take_while(|maybe_record_batch| ready(maybe_record_batch.is_some()))
            .flat_map(|option| iter(option))
    }

    pub fn get_marker(&self) -> Result<Result<Marker, GenerationMarker>, DataFusionError> {
        todo!();
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use futures_util::stream::iter;
    use futures::StreamExt;
    use crate::streaming::model::sitem::SItem;
    use crate::streaming::model::stream_item::Marker;
    use crate::streaming::operators::utils::chunked_stream::ChunkedStream;

    #[tokio::test]
    async fn test_chunked_stream() {
        let schema = SchemaRef::new(Schema::empty());
        let input_stream = Box::pin(iter(vec![
            SItem::RecordBatch(RecordBatch::new_empty(schema.clone())),
            SItem::RecordBatch(RecordBatch::new_empty(schema.clone())),
            SItem::Marker(Marker { checkpoint_number: 0 }),
            SItem::RecordBatch(RecordBatch::new_empty(schema.clone())),
        ]));
        let mut chunked_stream = ChunkedStream::new(input_stream);

        let record_batches = chunked_stream.stream_record_batches()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(record_batches.len(), 2);
        assert_eq!(chunked_stream.current_marker, Some(Ok(Marker { checkpoint_number: 0 })));

        let record_batches = chunked_stream.stream_record_batches().collect::<Vec<_>>().await;
        assert_eq!(record_batches.len(), 1);

        assert_eq!(chunked_stream.current_marker, None);
    }
}
