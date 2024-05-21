use core::fmt;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::Duration;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use google_cloud_googleapis::pubsub::v1::{ExpirationPolicy, PubsubMessage};
use google_cloud_pubsub::apiv1::default_retry_setting;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::{Publisher, PublisherConfig};
use google_cloud_pubsub::subscriber::SubscriberConfig;
use google_cloud_pubsub::subscription::{SubscribeConfig, Subscription, SubscriptionConfig};
use google_cloud_pubsub::topic::Topic;
use tracing::warn;
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
    subscription: Subscription,
    phantom_data: PhantomData<(A, E)>,
}

impl<A, E> PubsubAdapter<A, E> {
    pub async fn setup(subscription_name: &str) -> Result<Self, AdapterError> {
        let config = ClientConfig::default().with_auth().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        Self::setup_with_config(config, subscription_name).await
    }
    pub async fn setup_with_config(config: ClientConfig, subscription_name: &str) -> Result<Self, AdapterError> {
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

        let subscription = client.subscription(subscription_name);
        if !subscription.exists(None).await.map_err(|err| AdapterError::Other { error: err.to_string() })? {
            subscription
                .create(
                    topic.fully_qualified_name(),
                    SubscriptionConfig {
                        expiration_policy: Some(ExpirationPolicy {
                            ttl: Some(prost_types::Duration {
                                seconds: 3600 * 24, // 24 hours
                                nanos: 0,
                            }),
                        }),
                        message_retention_duration: Some(Duration::from_secs(600)),
                        ..Default::default()
                    },
                    None,
                )
                .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        }

        Ok(Self {
            client,
            topic,
            publisher,
            subscription,
            phantom_data: PhantomData
        })
    }
}

#[async_trait]
impl<A: Aggregate<E> + Clone + fmt::Debug + Send + Sync + serde::Serialize + serde::de::DeserializeOwned, E: Clone + fmt::Debug + Send + Sync + serde::Serialize + serde::de::DeserializeOwned> NotificationAdapter<A, E> for PubsubAdapter<A, E> {
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
        let subscriber = self.subscription
            .subscribe(Some(SubscribeConfig::default().with_subscriber_config(
                SubscriberConfig {
                    ping_interval: Duration::from_secs(10),
                    retry_setting: Some(default_retry_setting()),
                    stream_ack_deadline_seconds: 10,
                    max_outstanding_messages: 50,
                    max_outstanding_bytes: 1000 * 1000 * 1000,
                },
            )))
            .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let stream = subscriber.map(Ok).try_filter_map(move |message| {
            async move {
                if message.message.attributes.get("aggregate_name") == Some(&A::name().to_string()) {
                    let data: ListenForEventData<A, E> =
                        serde_json::from_slice(&message.message.data)
                            .map_err(|err| AdapterError::Other { error: err.to_string() })?;
                    Ok(Some(data))
                } else {
                    tokio::spawn(async move {
                        if let Err(e) = message.ack().await {
                            warn!(error_message = e.to_string(), "Failed to ack message");
                        }
                    });
                    Ok(None)
                }
            }
        });

        Ok(stream.boxed())
    }
}