use core::fmt;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::str::{from_utf8, FromStr};
use std::{env, time::Duration};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::stream::StreamExt;
use futures::stream::TryStreamExt;
use futures::stream::{iter, BoxStream};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::{Publisher, PublisherConfig};
use google_cloud_pubsub::subscriber::SubscriberConfig;
use google_cloud_pubsub::subscription::{SubscribeConfig, SubscriptionConfig};
use google_cloud_pubsub::topic::Topic;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::{Aggregate, Event, Repository};

#[derive(Debug, Clone)]
pub struct PostgresRepository<A: Aggregate<E>, E> {
    pool: Pool<PostgresConnectionManager<tokio_postgres::NoTls>>,
    service_name: String,
    client: Client,
    topic: Topic,
    publisher: Publisher,
    phantom1: PhantomData<E>,
    phantom2: PhantomData<A>,
}

impl<A: Aggregate<E> + fmt::Debug, E: fmt::Debug> PostgresRepository<A, E> {
    #[tracing::instrument(err)]
    pub async fn setup(
        service_name: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let username = env::var("POSTGRES_USERNAME").unwrap_or("postgres".to_string());
        let password = env::var("POSTGRES_PASSWORD").unwrap_or("postgres".to_string());

        let config = tokio_postgres::config::Config::from_str(&format!(
            "host=localhost user={} password= {}",
            username, password
        ))?;

        let manager = bb8_postgres::PostgresConnectionManager::new(config, tokio_postgres::NoTls);
        let pool = bb8::Pool::builder().max_size(15).build(manager).await?;
        {
            let connection = pool.get().await?;
            connection
                .simple_query(&format!(
                    "CREATE TABLE IF NOT EXISTS {}_event_store (
            aggregate_id    UUID NOT NULL,
            event_id        BIGINT NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL,
            user_id         TEXT,
            payload         JSONB,
            PRIMARY KEY (aggregate_id, event_id)
          )",
                    service_name
                ))
                .await?;
            info!("Updaterted {}_event_store table", service_name);

            connection
                .simple_query(&format!(
                    "CREATE TABLE IF NOT EXISTS {}_event_store_external_ids (
            external_id     TEXT NOT NULL UNIQUE,
            aggregate_id    UUID NOT NULL UNIQUE,
            PRIMARY KEY (external_id)
          )",
                    service_name
                ))
                .await?;
            info!("Updaterted {}_event_store_external_ids table", service_name);
        }

        let config = ClientConfig::default().with_auth().await?;
        let client = Client::new(config).await?;
        info!("Created pub sub client");

        let topic = client.topic("event-stream");
        info!("Connected to event stream topic");
        if !topic.exists(None).await? {
            topic.create(None, None).await?;
        }
        let publisher = topic.new_publisher(Some(PublisherConfig {
            workers: 8,
            flush_interval: Duration::from_millis(50),
            bundle_size: 1,
            retry_setting: None,
        }));

        Ok(Self {
            pool,
            service_name: service_name.to_string(),
            publisher,
            client,
            topic,
            phantom1: PhantomData,
            phantom2: PhantomData,
        })
    }
}

#[async_trait::async_trait]
impl<
        A: Default + Aggregate<E> + Send + Sync + fmt::Debug,
        E: Serialize + DeserializeOwned + Send + Sync + 'static + fmt::Debug,
    > Repository<A, E> for PostgresRepository<A, E>
{
    #[tracing::instrument(level = "debug", err, ret, skip(self))]
    async fn get(&self, aggregate_id: Uuid) -> Result<A, Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.pool.get().await?;

        let rows = connection
            .query(
                &format!(
                    "SELECT aggregate_id, event_id, created_at, user_id, payload FROM {}_event_store WHERE aggregate_id = $1 ORDER BY event_id ASC",
                    self.service_name
                ),
                &[&aggregate_id],
            )
            .await?;

        let mut aggregate = A::default();

        aggregate.set_aggregate_id(aggregate_id);

        for row in rows {
            let event: Event<E> = row.try_into()?;
            aggregate.apply(&event);
        }

        Ok(aggregate)
    }

    #[tracing::instrument(level = "debug", err, skip(self))]
    async fn stream_ids(
        &self,
    ) -> Result<BoxStream<Uuid>, Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.pool.get().await?;

        let rows = connection
            .query_raw::<_, &String, _>(
                &format!(
                    "SELECT DISTINCT aggregate_id FROM {}_event_store",
                    self.service_name
                ),
                &[],
            )
            .await?;

        Ok(rows
            .into_stream()
            .filter_map(|i| async {
                match i {
                    Ok(row) => Some(row.get(0)),
                    Err(_) => None,
                }
            })
            .boxed())
    }

    #[tracing::instrument(level = "debug", err, skip(self))]
    async fn stream_all(
        &self,
    ) -> Result<
        BoxStream<Result<A, Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let ids = self.stream_ids().await?;

        let all: Vec<_> = ids.collect().await;

        Ok(iter(all.into_iter())
            .map(Ok)
            .and_then(|id| self.get(id))
            .boxed())
    }

    #[tracing::instrument(level = "debug", err, ret, skip(self))]
    async fn create(
        &self,
        aggregate_id: Uuid,
        external_id: Option<String>,
        events: Vec<Event<E>>,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        if self.get(aggregate_id).await?.exists() {
            return Err("This aggregate does already exist".into());
        }
        {
            let mut connection = self.pool.get().await?;

            let transaction = connection.transaction().await?;

            let statement = transaction
                .prepare(&format!(
                "INSERT INTO {}_event_store (aggregate_id, event_id, created_at, user_id, payload)\
                 VALUES ($1, $2, $3, $4, $5)", self.service_name))
                .await?;

            if let Some(e_id) = external_id {
                transaction
                    .execute(
                        &format!(
                            "INSERT INTO {}_event_store_external_ids (aggregate_id, external_id)\
                 VALUES ($1, $2)",
                            self.service_name
                        ),
                        &[&aggregate_id, &e_id],
                    )
                    .await?;
            }

            for event in &events {
                transaction
                    .execute(
                        &statement,
                        &[
                            &event.aggregate_id,
                            &(event.event_id as i64),
                            &event.created_at,
                            &event.user_id,
                            &serde_json::to_value(&event.payload).unwrap(),
                        ],
                    )
                    .await?;
            }

            transaction.commit().await?;
        }

        let publisher = self.publisher.clone();
        let service_name = self.service_name.clone();
        let events_len = events.len() as u64;

        tokio::spawn(async move {
            for event in &events {
                let payload = serde_json::to_string_pretty(&event).unwrap();
                let mut attributes = HashMap::new();
                attributes.insert("aggregate_id".to_string(), aggregate_id.to_string());
                attributes.insert("service".to_string(), service_name.clone());
                let msg = PubsubMessage {
                    data: payload.into(),
                    attributes,
                    ..Default::default()
                };
                let awaiter = publisher.publish(msg).await;

                if let Err(err) = awaiter.get().await {
                    error!("Failed to publish event: {}", err);
                }
            }
        });

        Ok(events_len)
    }

    #[tracing::instrument(level = "debug", skip(self, callback), ret, err)]
    async fn store<
        F: Fn(&A) -> Result<Vec<Event<E>>, Box<dyn std::error::Error + Send + Sync>> + Send + Sync,
    >(
        &self,
        aggregate_id: Uuid,
        callback: F,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        for _i in 0..30 {
            let aggregate = self.get(aggregate_id).await?;
            if !aggregate.exists() {
                return Err("This aggregate does not exist".into());
            }

            let events = callback(&aggregate)?;

            if events.is_empty() {
                return Ok(aggregate.version());
            }

            let mut connection = self.pool.get().await.unwrap();

            let transaction = connection.transaction().await.unwrap();

            let statement = transaction
                .prepare(&format!(
                "INSERT INTO {}_event_store (aggregate_id, event_id, created_at, user_id, payload)\
                 VALUES ($1, $2, $3, $4, $5)", self.service_name))
                .await?;

            for event in &events {
                match transaction
                    .execute(
                        &statement,
                        &[
                            &event.aggregate_id,
                            &(event.event_id as i64),
                            &event.created_at,
                            &event.user_id,
                            &serde_json::to_value(&event.payload).unwrap(),
                        ],
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                }
            }

            match transaction.commit().await {
                Ok(_) => {
                    let publisher = self.publisher.clone();
                    let service_name = self.service_name.clone();

                    let events_len = events.len() as u64;

                    tokio::spawn(async move {
                        for event in &events {
                            let payload = serde_json::to_string_pretty(&event).unwrap();
                            let mut attributes = HashMap::new();
                            attributes.insert("aggregate_id".to_string(), aggregate_id.to_string());
                            attributes.insert("service".to_string(), service_name.clone());
                            let msg = PubsubMessage {
                                data: payload.into(),
                                attributes,
                                ..Default::default()
                            };
                            let awaiter = publisher.publish(msg).await;

                            if let Err(err) = awaiter.get().await {
                                error!("Failed to publish event: {}", err);
                            }
                        }
                    });
                    return Ok(aggregate.version() + events_len);
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            }
        }

        Err("Failed to store events".into())
    }

    #[tracing::instrument(level = "debug", err, skip(self))]
    async fn stream_realtime(
        &self,
        subscription_name: &str,
    ) -> Result<BoxStream<(Event<E>, A)>, Box<dyn std::error::Error + Send + Sync>> {
        let subscription = self.client.subscription(subscription_name);
        if !subscription.exists(None).await? {
            subscription
                .create(
                    self.topic.fully_qualified_name(),
                    SubscriptionConfig {
                        message_retention_duration: Some(Duration::from_secs(600)),
                        ..Default::default()
                    },
                    None,
                )
                .await?;
        }

        let subscriber = subscription
            .subscribe(Some(SubscribeConfig::default().with_subscriber_config(
                SubscriberConfig {
                    ping_interval: Duration::from_millis(100),
                    retry_setting: None,
                    stream_ack_deadline_seconds: 10,
                    max_outstanding_messages: 100,
                    max_outstanding_bytes: 10_000,
                },
            )))
            .await?;
        let service_name = self.service_name.clone();

        let stream = subscriber.filter_map(move |message| {
            let service_name = service_name.clone();

            debug!(
                "Got new message, {}",
                from_utf8(&message.message.data).unwrap()
            );

            async move {
                if message.message.attributes.get("service") == Some(&service_name) {
                    let event_res: Result<Event<E>, _> =
                        serde_json::from_slice(&message.message.data);
                    match event_res {
                        Ok(event) => match self.get(event.aggregate_id).await {
                            Ok(aggregate) => {
                                if let Err(e) = message.ack().await {
                                    warn!("Failed to ack message, error was: {}", e);
                                }
                                Some((event, aggregate))
                            }
                            Err(err) => {
                                warn!("Failed to get aggregate, error was: {}", err);
                                if let Err(e) = message.ack().await {
                                    warn!("Failed to ack message, error was: {}", e);
                                }
                                None
                            }
                        },
                        Err(err) => {
                            warn!("Failed to parse event, error was: {}", err);
                            if let Err(e) = message.ack().await {
                                warn!("Failed to ack message, error was: {}", e);
                            }
                            None
                        }
                    }
                } else {
                    if let Err(e) = message.ack().await {
                        warn!("Failed to ack message, error was: {}", e);
                    }
                    None
                }
            }
        });

        Ok(stream.boxed())
    }

    #[tracing::instrument(level = "debug", ret, skip(self))]
    async fn fully_remove(
        &self,
        aggregate_id: Uuid,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let connection = self.pool.get().await?;

        connection
            .execute(
                &format!(
                    "DELETE FROM {}_event_store WHERE aggregate_id = $1",
                    self.service_name
                ),
                &[&aggregate_id],
            )
            .await?;

        Ok(())
    }
}
