use crate::error::AdapterError;
use crate::Event;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use async_trait::async_trait;
use core::fmt;
use futures::stream::BoxStream;
use uuid::Uuid;

#[async_trait]
pub trait EventStoreAdapter<E>: fmt::Debug + Send + Sync {
    async fn get_events(&self, aggregate_id: Uuid) -> Result<Vec<Event<E>>, AdapterError>;
    async fn stream_ids(&self) -> Result<BoxStream<Uuid>, AdapterError>;

    async fn aggregate_id_from_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<Uuid>, AdapterError>;

    async fn save_aggregate_id_to_external_ids(
        &self,
        aggregate_id: Uuid,
        external_ids: &[String],
    ) -> Result<(), AdapterError>;

    async fn save_events(&self, events: &[Event<E>]) -> Result<(), AdapterError>;

    async fn remove(&self, aggregate_id: Uuid) -> Result<(), AdapterError>;
}
