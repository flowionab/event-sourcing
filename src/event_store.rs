use crate::adapter::EventStoreAdapter;
use crate::adapter::NotificationAdapter;
use crate::error::create_error::{
    AdapterSnafu, AggregateAlreadyExistsSnafu, EmptyEventListSnafu, ExternalIdAlreadyExistsSnafu,
    InconsistentAggregateIdSnafu, InconsistentEventOrderingSnafu, NilUuidSnafu,
};
use crate::error::{AdapterError, CreateError, StoreError};
use crate::{Aggregate, Event, EventStoreBuilder, IntoEventList};
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::marker::PhantomData;
use core::time::Duration;
use futures::future::{try_join_all};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use snafu::{ensure, ResultExt};
use uuid::Uuid;

#[cfg(feature = "prometheus")]
lazy_static::lazy_static! {
    static ref STORED_EVENT_COUNTER: prometheus::IntCounter =
        prometheus::register_int_counter!("num_stored_events_count", "Number of stored events").unwrap();
    static ref CREATED_AGGREGATES_COUNTER: prometheus::IntCounter =
        prometheus::register_int_counter!("num_aggregates_created_count", "Number of newly created aggregates").unwrap();
    static ref READ_EVENTS_COUNTER: prometheus::IntCounter =
        prometheus::register_int_counter!("num_read_events_count", "Number of read events").unwrap();
    static ref AGGREGATE_APPLY_TIME_HISTOGRAM: prometheus::HistogramVec =
        prometheus::register_histogram_vec!("aggregate_apply_time", "Time fully build an aggregate", &["snapshot", "aggregate_name"], vec![0.001, 0.005, 0.010, 0.025, 0.05, 0.075, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0]).unwrap();
}

/// The event store used to persisting events. Besides from using the store, to well, store events,
/// it can also be used to fetch them, and to stream updates synced between different instances
#[derive(Debug, Clone)]
pub struct EventStore<A: Aggregate<E>, E> {
    adapter: Arc<dyn EventStoreAdapter<A, E>>,
    notification_adapter: Arc<dyn NotificationAdapter<A, E>>,
    store_attempts: usize,
    phantom_data: PhantomData<A>,
}

impl<A: Aggregate<E> + Send + Sync + Clone, E: std::marker::Send> EventStore<A, E> {
    pub(crate) fn new<T: EventStoreAdapter<A, E> + 'static, NT: NotificationAdapter<A, E> + 'static>(
        adapter: T,
        notification_adapter: NT,
        store_attempts: usize
    ) -> Self {
        Self {
            adapter: Arc::new(adapter),
            notification_adapter: Arc::new(notification_adapter),
            store_attempts,
            phantom_data: PhantomData,
        }
    }

    pub fn builder() -> EventStoreBuilder<A, E, (), ()> {
        EventStoreBuilder::new()
    }

    /// Fetches a single aggregate
    pub async fn aggregate(&self, aggregate_id: Uuid) -> Result<Option<A>, AdapterError> {
        #[cfg(feature = "prometheus")]
        let timer = AGGREGATE_APPLY_TIME_HISTOGRAM.with_label_values(&["true", A::name()]).start_timer();

        let result = self.aggregate_inner(aggregate_id).await;

        #[cfg(feature = "prometheus")]
        timer.observe_duration();

        result
    }

    /// Fetches a single aggregate
    pub async fn aggregate_without_snapshot(&self, aggregate_id: Uuid) -> Result<Option<A>, AdapterError> {
        #[cfg(feature = "prometheus")]
            let timer = AGGREGATE_APPLY_TIME_HISTOGRAM.with_label_values(&["false", A::name()]).start_timer();

        let result = self.aggregate_inner_without_snapshot(aggregate_id).await;

        #[cfg(feature = "prometheus")]
        timer.observe_duration();

        result
    }
    async fn aggregate_inner(&self, aggregate_id: Uuid) -> Result<Option<A>, AdapterError> {
        match self.adapter.get_snapshot(aggregate_id).await? {
            Some(mut aggregate) => {
                let start_version = aggregate.version();
                let stream = self.adapter.get_events(aggregate_id, Some(aggregate.version())).await?;

                aggregate = stream.try_fold(aggregate, |mut a, event| async move {
                    ensure!(
                        event.event_id == (a.version() + 1) as u64,
                        crate::error::adapter_error::InconsistentEventOrderingSnafu
                    );
                    a.apply(&event);
                    Ok(a)
                }).await?;

                #[cfg(feature = "prometheus")]
                READ_EVENTS_COUNTER.inc_by(aggregate.version() - start_version);

                if aggregate.version() == 0 {
                    Ok(None)
                } else {
                    if (aggregate.version() - start_version) > 10 {
                        self.adapter.save_snapshot(&aggregate).await?;
                    }
                    Ok(Some(aggregate))
                }
            }
            None => {
                self.aggregate_inner_without_snapshot(aggregate_id).await
            }
        }
    }
    async fn aggregate_inner_without_snapshot(&self, aggregate_id: Uuid) -> Result<Option<A>, AdapterError> {
        let stream = self.adapter.get_events(aggregate_id, None).await?;
        let mut aggregate = A::new_with_aggregate_id(aggregate_id);

        aggregate = stream.try_fold(aggregate, |mut a, event| async move {
            ensure!(
                event.event_id == (a.version() + 1) as u64,
                crate::error::adapter_error::InconsistentEventOrderingSnafu
            );
            a.apply(&event);
            Ok(a)
        }).await?;

        #[cfg(feature = "prometheus")]
        READ_EVENTS_COUNTER.inc_by(aggregate.version());

        if aggregate.version() == 0 {
            Ok(None)
        } else {
            self.adapter.save_snapshot(&aggregate).await?;
            Ok(Some(aggregate))
        }
    }

    /// Returns all ids in the store
    pub async fn ids(&self) -> Result<BoxStream<Uuid>, AdapterError> {
        self.adapter.stream_ids().await
    }

    /// Returns all aggregates in the store
    pub async fn all(&self) -> Result<BoxStream<Result<A, AdapterError>>, AdapterError> {
        Ok(self
            .ids()
            .await?
            .map(move |id| async move { self.aggregate(id).await })
            .buffer_unordered(2)
            .try_filter_map(|i| async move { Ok(i) })
            .boxed())
    }

    /// Creates a new aggregate
    pub async fn create<T: IntoEventList<E>>(&self, events: T) -> Result<A, CreateError> {
        let events = events.into_list();
        ensure!(!events.is_empty(), EmptyEventListSnafu);

        let first_aggregate_id = events.first().unwrap().aggregate_id;

        ensure!(first_aggregate_id != Uuid::nil(), NilUuidSnafu);

        for (index, event) in events.iter().enumerate() {
            ensure!(
                event.aggregate_id == first_aggregate_id,
                InconsistentAggregateIdSnafu
            );
            ensure!(
                event.event_id == (index + 1) as u64,
                InconsistentEventOrderingSnafu
            );
        }

        ensure!(
            self.aggregate(first_aggregate_id)
                .await
                .context(crate::error::create_error::AdapterSnafu {})?
                .is_none(),
            AggregateAlreadyExistsSnafu {
                id: first_aggregate_id
            }
        );

        self.adapter
            .save_events(&events)
            .await
            .context(AdapterSnafu)?;

        let mut aggregate = A::new_with_aggregate_id(first_aggregate_id);

        for event in events {
            let old_aggregate = if aggregate.version() == 0 {
                None
            } else {
                Some(aggregate.clone())
            };

            aggregate.apply(&event);
            self.notification_adapter
                .send_event(&event, &aggregate, old_aggregate.as_ref())
                .await
                .context(AdapterSnafu {})?;
        }


        #[cfg(feature = "prometheus")]
        CREATED_AGGREGATES_COUNTER.inc_by(1);

        Ok(aggregate)
    }

    /// Creates a new aggregate
    pub async fn create_with_external_ids(
        &self,
        events: Vec<Event<E>>,
        external_ids: Vec<String>,
    ) -> Result<A, CreateError> {
        ensure!(!events.is_empty(), EmptyEventListSnafu);

        let first_aggregate_id = events.first().unwrap().aggregate_id;

        let results: Vec<(Option<Uuid>, String)> = try_join_all(
            external_ids
                .iter()
                .map(|external_id| async move {
                    let aggregate_id = self
                        .adapter
                        .aggregate_id_from_external_id(external_id)
                        .await?;
                    Ok((aggregate_id, external_id.to_string()))
                })
                .collect::<Vec<_>>(),
        )
        .await
        .context(AdapterSnafu {})?;

        for (aggregate_id_opt, external_id) in results {
            ensure!(
                aggregate_id_opt.is_none() || aggregate_id_opt == Some(first_aggregate_id),
                ExternalIdAlreadyExistsSnafu {
                    id: aggregate_id_opt.unwrap(),
                    external_id
                }
            );
        }

        self.adapter
            .save_aggregate_id_to_external_ids(first_aggregate_id, &external_ids)
            .await
            .context(AdapterSnafu {})?;

        self.create(events).await
    }

    /// Stores events to an existing aggregate
    pub async fn store<T: IntoEventList<E>, F: Fn(&A) -> T + Send + Sync>(
        &self,
        aggregate_id: Uuid,
        callback: F,
    ) -> Result<A, StoreError> {
        let mut error = None;
        for _i in 0..self.store_attempts {
            match self
                .aggregate(aggregate_id)
                .await
                .context(crate::error::store_error::AdapterSnafu {})?
            {
                Some(mut aggregate) => {
                    let events = callback(&aggregate);
                    let events = events.into_list();
                    if events.is_empty() {
                        return Ok(aggregate);
                    }
                    match self.adapter.save_events(&events).await {
                        Ok(_) => {

                            #[cfg(feature = "prometheus")]
                            STORED_EVENT_COUNTER.inc_by(events.len() as u64);

                            for event in events {
                                let old_aggregate = aggregate.clone();
                                aggregate.apply(&event);
                                self.notification_adapter
                                    .send_event(&event, &aggregate, Some(&old_aggregate))
                                    .await
                                    .context(crate::error::store_error::AdapterSnafu {})?;
                            }
                            return Ok(aggregate);
                        }
                        Err(err) => {
                            error = Some(err);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                    }
                }
                None => {
                    return Err(StoreError::AggregateDoesNotExist { aggregate_id });
                }
            }
        }
        match error {
            None => Err(StoreError::Unknown),
            Some(err) => Err(StoreError::AdapterError { source: err }),
        }
    }

    /// Stores events to an existing aggregate with the option to throw a validation error
    pub async fn try_store<
        T: IntoEventList<E>,
        F: Fn(&A) -> Result<T, I> + Send + Sync,
        I: Send + Sync,
    >(
        &self,
        aggregate_id: Uuid,
        callback: F,
    ) -> Result<Result<A, I>, StoreError> {
        let mut error = None;
        for _i in 0..self.store_attempts {
            match self
                .aggregate(aggregate_id)
                .await
                .context(crate::error::store_error::AdapterSnafu {})?
            {
                Some(mut aggregate) => match callback(&aggregate) {
                    Ok(events) => {
                        let events = events.into_list();
                        if events.is_empty() {
                            return Ok(Ok(aggregate));
                        }
                        match self.adapter.save_events(&events).await {
                            Ok(_) => {

                                #[cfg(feature = "prometheus")]
                                STORED_EVENT_COUNTER.inc_by(events.len() as u64);

                                for event in events {
                                    let old_aggregate = aggregate.clone();
                                    aggregate.apply(&event);
                                    self.notification_adapter
                                        .send_event(&event, &aggregate, Some(&old_aggregate))
                                        .await
                                        .context(crate::error::store_error::AdapterSnafu {})?;
                                }
                                return Ok(Ok(aggregate));
                            }
                            Err(err) => {
                                error = Some(err);
                                tokio::time::sleep(Duration::from_millis(50)).await;
                                continue;
                            }
                        }
                    }
                    Err(err) => return Ok(Err(err)),
                },
                None => {
                    return Err(StoreError::AggregateDoesNotExist { aggregate_id });
                }
            }
        }

        match error {
            None => Err(StoreError::Unknown),
            Some(err) => Err(StoreError::AdapterError { source: err }),
        }
    }

    pub async fn stream_realtime(
        &self,
    ) -> Result<BoxStream<Result<RealtimeStreamData<A, E>, AdapterError>>, AdapterError> {
        let stream = self.notification_adapter.listen_for_events().await?;

        let mapped_stream = stream
            .map_ok(|data| RealtimeStreamData {
                event: data.event,
                new_aggregate: data.new_aggregate,
                old_aggregate: data.old_aggregate,
            })
            .boxed();

        Ok(mapped_stream)
    }

    /// Removes the aggregate permanently
    pub async fn remove(&self, aggregate_id: Uuid) -> Result<(), AdapterError> {
        self.adapter.remove(aggregate_id).await
    }
}

pub struct RealtimeStreamData<A, E> {
    pub event: Event<E>,
    pub old_aggregate: Option<A>,
    pub new_aggregate: A,
}

#[cfg(test)]
mod tests {
    extern crate std;

    use crate::adapter::in_memory::InMemoryAdapter;
    use crate::adapter::{EventStoreAdapter, NotificationAdapter};
    use crate::{Aggregate, Event, EventStore};
    use alloc::string::ToString;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::convert::Infallible;
    use futures::{StreamExt, TryStreamExt};
    use uuid::Uuid;

    #[derive(Debug, Clone, PartialEq)]
    enum TestEvent {
        Test(),
    }

    #[derive(Debug, Clone, Default, Eq, PartialEq)]
    struct TestAggregate {
        aggregate_id: Uuid,
        version: u64,
    }

    impl Aggregate<TestEvent> for TestAggregate {
        fn version(&self) -> u64 {
            self.version
        }

        fn aggregate_id(&self) -> Uuid {
            self.aggregate_id
        }

        fn apply(&mut self, event: &Event<TestEvent>) {
            self.version = event.event_id
        }

        fn new_with_aggregate_id(aggregate_id: Uuid) -> Self {
            Self {
                aggregate_id,
                version: 0,
            }
        }

        fn name() -> &'static str {
            "test"
        }
    }

    #[tokio::test]
    async fn get_none_existing_aggregate() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();

        let aggregate = store.aggregate(id).await.unwrap();
        assert_eq!(aggregate, None);
    }

    #[tokio::test]
    async fn get_existing_aggregate() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();

        adapter
            .save_events(
                &Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let aggregate = store.aggregate(id).await.unwrap();
        assert!(aggregate.is_some());
    }

    #[tokio::test]
    async fn get_ids_should_return_all_ids_in_store() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        adapter
            .save_events(
                &Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let aggregate: Vec<_> = store.ids().await.unwrap().collect().await;
        assert_eq!(aggregate, vec![id]);
    }

    #[tokio::test]
    async fn get_all_aggregates_should_return_all_aggregates_in_store() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        adapter
            .save_events(
                &Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let aggregate: Vec<_> = store.all().await.unwrap().try_collect().await.unwrap();
        assert_eq!(aggregate[0].aggregate_id(), id);
    }

    #[tokio::test]
    async fn create_without_external_ids_should_work() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let events = adapter.get_events(id, None).await.unwrap().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn create_without_external_ids_should_not_work_twice() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn create_without_external_ids_should_send_notifications() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let stream = adapter.listen_for_events().await.unwrap();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let event = stream.into_future().await.0.unwrap().unwrap().event.payload;

        assert_eq!(event, TestEvent::Test())
    }

    #[tokio::test]
    async fn create_with_external_ids_should_work() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create_with_external_ids(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
                vec!["123".to_string()],
            )
            .await
            .unwrap();

        let events = adapter.get_events(id, None).await.unwrap().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn create_with_external_ids_should_not_work_twice() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        store
            .create_with_external_ids(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(Uuid::new_v4()),
                vec!["123".to_string()],
            )
            .await
            .unwrap();
        store
            .create_with_external_ids(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(Uuid::new_v4()),
                vec!["123".to_string()],
            )
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn store_should_store_events() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        store
            .store(id, |aggregate| {
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build(aggregate)
            })
            .await
            .unwrap();

        let events = adapter.get_events(id, None).await.unwrap().try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(events.len(), 2)
    }

    #[tokio::test]
    async fn store_should_fail_if_aggregate_does_not_exist() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();

        store
            .store(id, |aggregate| {
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build(aggregate)
            })
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn store_should_fail_if_the_event_sequence_is_invalid() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();

        store
            .store(id, |_aggregate| {
                vec![
                    Event {
                        aggregate_id: id,
                        event_id: 1,
                        created_at: Default::default(),
                        user_id: None,
                        payload: TestEvent::Test(),
                    },
                    Event {
                        aggregate_id: id,
                        event_id: 1, // Duplicated event id
                        created_at: Default::default(),
                        user_id: None,
                        payload: TestEvent::Test(),
                    },
                ]
            })
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn store_should_send_events() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let stream = adapter.listen_for_events().await.unwrap();

        store
            .store(id, |aggregate| {
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build(aggregate)
            })
            .await
            .unwrap();

        let event = stream.into_future().await.0.unwrap().unwrap().event.payload;

        assert_eq!(event, TestEvent::Test())
    }

    #[tokio::test]
    async fn try_store_should_store_events() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        store
            .try_store(id, |aggregate| {
                Result::<_, Infallible>::Ok(
                    Event::list_builder()
                        .add_event(TestEvent::Test(), None)
                        .build(aggregate),
                )
            })
            .await
            .unwrap()
            .unwrap();

        let events = adapter.get_events(id, None).await.unwrap().try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(events.len(), 2)
    }

    #[tokio::test]
    async fn try_store_should_fail_if_aggregate_does_not_exist() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();

        store
            .try_store(id, |aggregate| {
                Result::<_, Infallible>::Ok(
                    Event::list_builder()
                        .add_event(TestEvent::Test(), None)
                        .build(aggregate),
                )
            })
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn try_store_should_fail_if_the_event_sequence_is_invalid() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();

        store
            .try_store(id, |_aggregate| {
                Result::<_, Infallible>::Ok(vec![
                    Event {
                        aggregate_id: id,
                        event_id: 1,
                        created_at: Default::default(),
                        user_id: None,
                        payload: TestEvent::Test(),
                    },
                    Event {
                        aggregate_id: id,
                        event_id: 1, // Duplicated event id
                        created_at: Default::default(),
                        user_id: None,
                        payload: TestEvent::Test(),
                    },
                ])
            })
            .await
            .unwrap_err();
    }

    #[tokio::test]
    async fn try_store_error_thrown_inside_should_be_propegated() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let err = store
            .try_store(id, |_aggregate| {
                Err::<Vec<Event<TestEvent>>, &str>("Failure")
            })
            .await
            .unwrap()
            .unwrap_err();

        assert_eq!(err, "Failure")
    }

    #[tokio::test]
    async fn try_store_send_events() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        store
            .try_store(id, |aggregate| {
                Result::<_, Infallible>::Ok(
                    Event::list_builder()
                        .add_event(TestEvent::Test(), None)
                        .build(aggregate),
                )
            })
            .await
            .unwrap()
            .unwrap();

        let events = adapter.get_events(id, None).await.unwrap().try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(events.len(), 2)
    }

    #[tokio::test]
    async fn remove_should_remove_all_events_for_aggregate() {
        let adapter = InMemoryAdapter::new();
        let store = EventStore::<TestAggregate, _>::builder()
            .event_store_adapter(adapter.clone())
            .notification_adapter(adapter.clone())
            .build();

        let id = Uuid::new_v4();
        store
            .create(
                Event::list_builder()
                    .add_event(TestEvent::Test(), None)
                    .build_new(id),
            )
            .await
            .unwrap();

        let events = adapter.get_events(id, None).await.unwrap().try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(events.len(), 1);

        store.remove(id).await.unwrap();

        let events = adapter.get_events(id, None).await.unwrap().try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(events.len(), 0);
    }
}
