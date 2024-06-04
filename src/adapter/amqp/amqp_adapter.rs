use core::fmt;
use std::marker::PhantomData;
use std::mem::forget;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind};
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::{AMQPValue, FieldTable};
use tracing::warn;
use crate::adapter::{ListenForEventData, NotificationAdapter};
use crate::{Aggregate, Event};
use crate::error::AdapterError;

const EXCHANGE_NAME: &str = "event-stream";

#[derive(Debug)]
pub struct AmqpAdapter<A, E> {
    channel: Channel,
    service_name: String,
    phantom_data: PhantomData<(A, E)>,
}

impl<A, E> AmqpAdapter<A, E> {
    pub async fn setup(service_name: &str) -> Result<Self, AdapterError> {
        let addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());

        Self::setup_with_address(&addr, service_name).await
    }

    pub async fn setup_with_address(addr: &str, service_name: &str) -> Result<Self, AdapterError> {
        let connection = Connection::connect(
            &addr,
            ConnectionProperties::default().with_connection_name(service_name.into()).with_executor(tokio_executor_trait::Tokio::current())
                .with_reactor(tokio_reactor_trait::Tokio),
        )
            .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;


        let channel = connection.create_channel().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        forget(connection);

        Self::setup_with_channel(service_name, channel).await
    }

    pub async fn setup_with_channel(service_name: &str, channel: Channel) -> Result<Self, AdapterError> {
        channel.exchange_declare(EXCHANGE_NAME, ExchangeKind::Direct, ExchangeDeclareOptions::default(), FieldTable::default()).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        Ok(Self {
            channel,
            service_name: service_name.to_string(),
            phantom_data: PhantomData,
        })
    }
}


#[async_trait]
impl<A: Aggregate<E> + Clone + fmt::Debug + Send + Sync + serde::Serialize + serde::de::DeserializeOwned, E: Clone + fmt::Debug + Send + Sync + serde::Serialize + serde::de::DeserializeOwned> NotificationAdapter<A, E> for AmqpAdapter<A, E> {
    async fn send_event(&self, event: &Event<E>, new_aggregate: &A, old_aggregate: Option<&A>) -> Result<(), AdapterError> {
        let payload = serde_json::to_string(&ListenForEventData {
            event: event.clone(),
            new_aggregate: new_aggregate.clone(),
            old_aggregate: old_aggregate.cloned(),
        }).map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let _ = self.channel.basic_publish(EXCHANGE_NAME, A::name(), BasicPublishOptions::default(), &payload.as_bytes().to_vec(), BasicProperties::default()).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        Ok(())
    }

    async fn listen_for_events(&self) -> Result<BoxStream<Result<ListenForEventData<A, E>, AdapterError>>, AdapterError> {
        let mut fields = FieldTable::default();
        fields.insert("x-queue-type".into(), AMQPValue::LongString("quorum".into()));
        let queue = self.channel
            .queue_declare(
                &format!("{}_{}", self.service_name, A::name()),
                QueueDeclareOptions {
                    passive: false,
                    durable: true,
                    exclusive: false,
                    auto_delete: false,
                    nowait: false,
                },
                fields,
            )
            .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        self.channel.queue_bind(
            &queue.name().as_str(),
            EXCHANGE_NAME,
            A::name(),
            QueueBindOptions::default(),
            FieldTable::default(),
        ).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let consumer = self.channel.basic_consume(
            &queue.name().as_str(),
            &self.service_name,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        ).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let stream = consumer
            .map_err(|err| AdapterError::Other { error: err.to_string() })
            .try_filter_map(|delivery| {
            async move {
                let event_data: ListenForEventData<A, E> = serde_json::from_slice(&delivery.data).map_err(|err| AdapterError::Other { error: err.to_string() })?;
                tokio::spawn(async move {
                    if let Err(e) = delivery.ack(BasicAckOptions::default()).await {
                        warn!(error_message = e.to_string(), "Failed to ack message");
                    }
                });
                Ok(Some(event_data))
            }
        });

        Ok(stream.boxed())
    }
}