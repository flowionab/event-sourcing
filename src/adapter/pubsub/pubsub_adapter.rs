use core::fmt;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use async_trait::async_trait;
use futures::stream::BoxStream;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::{Publisher, PublisherConfig};
use google_cloud_pubsub::topic::Topic;
use crate::adapter::{ListenForEventData, NotificationAdapter};
use crate::error::AdapterError;
use crate::{Aggregate, Event};

#[derive(Debug)]
pub struct PubsubAdapter<A, E> {
    #[allow(dead_code)]
    client: Client,

    #[allow(dead_code)]
    topic: Topic,
    publisher: Publisher,
    phantom_data: PhantomData<(A, E)>,
}

impl<A, E> PubsubAdapter<A, E> {
    pub async fn setup() -> Result<Self, AdapterError> {
        let config = ClientConfig::default().with_auth().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        Self::setup_with_config(config).await
    }
    pub async fn setup_with_config(config: ClientConfig) -> Result<Self, AdapterError> {
        let client = Client::new(config).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let topic = client.topic("event-stream");
        if !topic.exists(None).await.map_err(|err| AdapterError::Other { error: err.to_string() })? {
            topic.create(None, None).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        }
        let publisher = topic.new_publisher(Some(PublisherConfig {
            workers: 3,
            flush_interval: Duration::from_millis(100),
            bundle_size: 3,
            retry_setting: None,
        }));

        Ok(Self {
            client,
            topic,
            publisher,
            phantom_data: PhantomData
        })
    }
}

#[async_trait]
impl<A: Aggregate<E> + Clone + fmt::Debug + Send + Sync + serde::Serialize, E: Clone + fmt::Debug + Send + Sync + serde::Serialize> NotificationAdapter<A, E> for PubsubAdapter<A, E> {
    async fn send_event(&self, event: &Event<E>, new_aggregate: &A, old_aggregate: Option<&A>) -> Result<(), AdapterError> {

        let payload = {
            serde_json::to_string(&ListenForEventData {
                event: event.clone(),
                old_aggregate: old_aggregate.cloned(),
                new_aggregate: new_aggregate.clone(),
            }).map_err(|err| AdapterError::Other { error: err.to_string() })?
        };
        let mut attributes = HashMap::new();
        attributes.insert("aggregate_id".to_string(), event.aggregate_id.to_string());
        attributes.insert("aggregate_name".to_string(), A::name().to_string());
        let _ = self.publisher.publish(PubsubMessage {
            data: payload.into(),
            attributes,
            ..Default::default()
        }).await;

        Ok(())
    }

    async fn listen_for_events(&self) -> Result<BoxStream<Result<ListenForEventData<A, E>, AdapterError>>, AdapterError> {
        todo!()
    }
}