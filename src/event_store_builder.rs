use core::marker::PhantomData;
use crate::adapter::{EventStoreAdapter, NotificationAdapter};
use crate::{Aggregate, EventStore};

/// Utility for building a new event store
pub struct EventStoreBuilder<A, E, ESA, NA> {
    store_attempts: usize,
    event_store_adapter: ESA,
    notification_adapter: NA,
    phantom_data: PhantomData<(A, E)>
}

impl<A, E> EventStoreBuilder<A, E, (), ()> {
    pub fn new() -> Self {
        Self {
            store_attempts: 30,
            event_store_adapter: (),
            notification_adapter: (),
            phantom_data: PhantomData
        }
    }

    pub fn store_attempts(mut self, store_attempts: usize) -> Self {
        self.store_attempts = store_attempts;
        self
    }
}

impl<A, E, ESA, NA> EventStoreBuilder<A, E, ESA, NA> {
    pub fn event_store_adapter<T: EventStoreAdapter<A, E>>(self, event_store_adapter: T) -> EventStoreBuilder<A, E, T, NA> {
        EventStoreBuilder {
            event_store_adapter,
            notification_adapter: self.notification_adapter,
            store_attempts: self.store_attempts,
            phantom_data: PhantomData
        }
    }

    pub fn notification_adapter<T: NotificationAdapter<A, E>>(self, notification_adapter: T) -> EventStoreBuilder<A, E, ESA, T> {
        EventStoreBuilder {
            event_store_adapter: self.event_store_adapter,
            notification_adapter,
            store_attempts: self.store_attempts,
            phantom_data: PhantomData
        }
    }
}

impl<A: Aggregate<E> + Send + Sync + Clone, E, ESA: EventStoreAdapter<A, E> + 'static, NA: NotificationAdapter<A, E> + 'static> EventStoreBuilder<A, E, ESA, NA> {

    pub fn build(self) -> EventStore<A, E> {
        EventStore::new(self.event_store_adapter, self.notification_adapter, self.store_attempts)
    }
}