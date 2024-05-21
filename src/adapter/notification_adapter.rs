use crate::error::AdapterError;
use crate::Event;
use alloc::boxed::Box;
use async_trait::async_trait;
use core::fmt;
use futures::stream::BoxStream;

#[async_trait]
pub trait NotificationAdapter<A, E>: fmt::Debug + Send + Sync {
    async fn send_event(
        &self,
        event: &Event<E>,
        new_aggregate: &A,
        old_aggregate: Option<&A>,
    ) -> Result<(), AdapterError>;
    async fn listen_for_events(
        &self,
    ) -> Result<BoxStream<Result<ListenForEventData<A, E>, AdapterError>>, AdapterError>;
}

#[derive(Debug, Clone)]
pub struct ListenForEventData<A, E> {
    pub event: Event<E>,
    pub old_aggregate: Option<A>,
    pub new_aggregate: A,
}
