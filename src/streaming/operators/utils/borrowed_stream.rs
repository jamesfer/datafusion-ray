use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_util::StreamExt;
use pin_project::pin_project;

#[pin_project]
pub struct BorrowedStream<'a, S: ?Sized, T> {
    #[pin]
    stream: &'a mut Pin<Box<S>>,
    phantom: std::marker::PhantomData<T>,
}

impl <'a, S, T> BorrowedStream<'a, S, T>
where S: Stream<Item=T> + ?Sized
{
    pub fn new(stream: &'a mut Pin<Box<S>>) -> Self {
        Self {
            stream,
            phantom: std::marker::PhantomData,
        }
    }
}

impl <'a, S, T> Stream for BorrowedStream<'a, S, T>
where S: Stream<Item=T> + ?Sized
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        this.stream.poll_next_unpin(cx)
    }
}
