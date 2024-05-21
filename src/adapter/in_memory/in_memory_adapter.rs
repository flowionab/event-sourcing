use crate::adapter::notification_adapter::ListenForEventData;
use crate::adapter::{EventStoreAdapter, NotificationAdapter};
use crate::error::AdapterError;
use crate::Event;
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec;
use alloc::vec::Vec;
use async_trait::async_trait;
use core::fmt;
use futures::stream::{iter, BoxStream};
use futures::StreamExt;
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct InMemoryAdapter<A, E> {
    store: Arc<Mutex<BTreeMap<Uuid, Vec<Event<E>>>>>,
    external_ids: Arc<Mutex<BTreeMap<String, Uuid>>>,
    sender: Sender<ListenForEventData<A, E>>,
}

impl<A: Send + Clone, E: Clone> InMemoryAdapter<A, E> {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(16);
        Self {
            store: Arc::new(Mutex::new(BTreeMap::new())),
            external_ids: Arc::new(Mutex::new(BTreeMap::new())),
            sender,
        }
    }
}

#[async_trait]
impl<A: fmt::Debug + Send, E: Clone + fmt::Debug + Send + Sync> EventStoreAdapter<E>
    for InMemoryAdapter<A, E>
{
    async fn get_events(&self, aggregate_id: Uuid) -> Result<Vec<Event<E>>, AdapterError> {
        let lock = self.store.lock().await;
        Ok(lock.get(&aggregate_id).cloned().unwrap_or_default())
    }

    async fn stream_ids(&self) -> Result<BoxStream<Uuid>, AdapterError> {
        let lock = self.store.lock().await;
        let keys = lock.clone().into_keys();
        Ok(iter(keys).boxed())
    }

    async fn aggregate_id_from_external_id(
        &self,
        external_id: &str,
    ) -> Result<Option<Uuid>, AdapterError> {
        let lock = self.external_ids.lock().await;
        Ok(lock.get(external_id).cloned())
    }

    async fn save_aggregate_id_to_external_ids(
        &self,
        aggregate_id: Uuid,
        external_ids: &[String],
    ) -> Result<(), AdapterError> {
        let mut lock = self.external_ids.lock().await;

        for id in external_ids {
            lock.insert(id.to_string(), aggregate_id);
        }

        Ok(())
    }

    async fn save_events(&self, events: &[Event<E>]) -> Result<(), AdapterError> {
        for event in events {
            let mut lock = self.store.lock().await;

            match lock.get_mut(&event.aggregate_id) {
                None => {
                    lock.insert(event.aggregate_id, vec![event.clone()]);
                }
                Some(list) => {
                    list.push(event.clone());
                }
            }
        }
        Ok(())
    }

    async fn remove(&self, aggregate_id: Uuid) -> Result<(), AdapterError> {
        let mut lock = self.store.lock().await;
        lock.remove(&aggregate_id);
        Ok(())
    }
}

#[async_trait]
impl<
        A: fmt::Debug + Send + Sync + Clone + 'static,
        E: Clone + fmt::Debug + Send + Sync + 'static,
    > NotificationAdapter<A, E> for InMemoryAdapter<A, E>
{
    async fn send_event(
        &self,
        event: &Event<E>,
        new_aggregate: &A,
        old_aggregate: Option<&A>,
    ) -> Result<(), AdapterError> {
        let _ = self.sender.send(ListenForEventData {
            event: event.clone(),
            new_aggregate: new_aggregate.clone(),
            old_aggregate: old_aggregate.cloned(),
        });
        Ok(())
    }

    async fn listen_for_events(
        &self,
    ) -> Result<BoxStream<Result<ListenForEventData<A, E>, AdapterError>>, AdapterError> {
        let receiver = self.sender.subscribe();

        let stream = BroadcastStream::new(receiver)
            .map(|i| {
                i.map_err(|e| AdapterError::Other {
                    error: e.to_string(),
                })
            })
            .boxed();

        Ok(stream)
    }
}
