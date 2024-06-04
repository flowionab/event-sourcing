use alloc::format;
use alloc::string::{String, ToString};
use core::marker::PhantomData;
use core::str::FromStr;
use crate::error::AdapterError;
use std::env;
use async_trait::async_trait;
use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio_postgres::Config;
use uuid::Uuid;
use crate::adapter::EventStoreAdapter;
use crate::{Aggregate, Event};

#[derive(Debug, Clone)]
pub struct PostgresAdapter<A, E> {
    pool: Pool<PostgresConnectionManager<tokio_postgres::NoTls>>,
    phantom_data: PhantomData<(A, E)>,
}

impl<A: Aggregate<E>, E> PostgresAdapter<A, E> {
    pub async fn setup(
    ) -> Result<Self, AdapterError> {
        let username = env::var("POSTGRES_USERNAME").unwrap_or("postgres".to_string());
        let password = env::var("POSTGRES_PASSWORD").unwrap_or("postgres".to_string());

        let config = tokio_postgres::config::Config::from_str(&format!(
            "host=localhost user={} password= {}",
            username, password
        )).map_err(|err| AdapterError::Other { error: err.to_string() })?;

        Self::setup_with_config(config).await
    }
    pub async fn setup_with_config(
        config: Config
    ) -> Result<Self, AdapterError> {

        let manager = PostgresConnectionManager::new(config, tokio_postgres::NoTls);
        let pool = Pool::builder().build(manager).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        {
            let connection = pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
            connection
                .simple_query(&format!(
                    "CREATE TABLE IF NOT EXISTS {}_event_store (
            aggregate_id    UUID NOT NULL,
            event_id        BIGINT NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL,
            user_id         TEXT,
            payload         JSON,
            PRIMARY KEY (aggregate_id, event_id)
          )",
                    A::name()
                ))
                .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

            connection
                .simple_query(&format!(
                    "CREATE TABLE IF NOT EXISTS {}_event_store_external_ids (
            external_id     TEXT NOT NULL UNIQUE,
            aggregate_id    UUID NOT NULL UNIQUE,
            PRIMARY KEY (external_id)
          )",
                    A::name()
                ))
                .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

            connection
                .simple_query(&format!(
                    "CREATE TABLE IF NOT EXISTS {}_event_store_snapshots (
            aggregate_id    UUID NOT NULL UNIQUE,
            payload         JSON,
            PRIMARY KEY (aggregate_id)
          )",
                    A::name()
                ))
                .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        }

        Ok(Self {
            pool,
            phantom_data: PhantomData
        })
    }
}

#[async_trait]
impl<A: Aggregate<E> + std::fmt::Debug + Send + Sync + Serialize + DeserializeOwned, E: std::fmt::Debug + Send + Sync + Serialize + DeserializeOwned> EventStoreAdapter<A, E> for PostgresAdapter<A, E> {
    async fn get_events(&self, aggregate_id: Uuid, from: Option<u64>) -> Result<BoxStream<Result<Event<E>, AdapterError>>, AdapterError> {
        let connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let rows = match from {
            None => {
                connection
                    .query_raw(
                        &format!(
                            "SELECT aggregate_id, event_id, created_at, user_id, payload FROM {}_event_store WHERE aggregate_id = $1 ORDER BY event_id ASC",
                            A::name()
                        ),
                        &[&aggregate_id],
                    )
                    .await.map_err(|err| AdapterError::Other { error: err.to_string() })?
            }
            Some(num) => {
                connection
                    .query_raw(
                        &format!(
                            "SELECT aggregate_id, event_id, created_at, user_id, payload FROM {}_event_store WHERE aggregate_id = $1 AND event_id > {} ORDER BY event_id ASC",
                            A::name(), num
                        ),
                        &[&aggregate_id],
                    )
                    .await.map_err(|err| AdapterError::Other { error: err.to_string() })?
            }
        };


        Ok(rows.map_err(|err| AdapterError::Other { error: err.to_string() }).and_then(|i| async move { i.try_into()}).boxed())
    }

    async fn stream_ids(&self) -> Result<BoxStream<Uuid>, AdapterError> {
        let connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let rows = connection
            .query_raw::<_, &String, _>(
                &format!(
                    "SELECT DISTINCT aggregate_id FROM {}_event_store",
                    A::name()
                ),
                &[],
            )
            .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

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

    async fn aggregate_id_from_external_id(&self, external_id: &str) -> Result<Option<Uuid>, AdapterError> {
        let connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        let row_opt = connection.query_opt(&format!("SELECT aggregate_id FROM {}_event_store_external_ids \
                 WHERE external_id = $1", A::name()), &[&external_id]).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        Ok(row_opt.map(|i| i.get("aggregate_id")))
    }

    async fn save_aggregate_id_to_external_ids(&self, aggregate_id: Uuid, external_ids: &[String]) -> Result<(), AdapterError> {
        let connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        for id in external_ids {
            connection.execute(&format!("INSERT INTO {}_event_store_external_ids (aggregate_id, external_id)\
                 VALUES ($1, $2)", A::name()), &[&aggregate_id, &id]).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        }
        Ok(())
    }

    async fn save_events(&self, events: &[Event<E>]) -> Result<(), AdapterError> {
        let mut connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        let transaction = connection.transaction().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        let statement = transaction
            .prepare(&format!(
                "INSERT INTO {}_event_store (aggregate_id, event_id, created_at, user_id, payload)\
                 VALUES ($1, $2, $3, $4, $5)", A::name()))
            .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        for event in events {
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
                .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        }

        transaction.commit().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        Ok(())
    }

    async fn remove(&self, aggregate_id: Uuid) -> Result<(), AdapterError> {
        let connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        connection
            .execute(
                &format!(
                    "DELETE FROM {}_event_store WHERE aggregate_id = $1",
                    A::name()
                ),
                &[&aggregate_id],
            )
            .await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        Ok(())
    }

    async fn get_snapshot(&self, aggregate_id: Uuid) -> Result<Option<A>, AdapterError> {
        let connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        let row_opt = connection.query_opt(&format!("SELECT payload FROM {}_event_store_snapshots \
                 WHERE aggregate_id = $1", A::name()), &[&aggregate_id]).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;

        match row_opt {
            Some(row) => {
                let payload = serde_json::from_value(row.get::<_, serde_json::Value>(0)).map_err(|err| AdapterError::Other { error: err.to_string() })?;
                Ok(Some(payload))
            },
            None => Ok(None),
        }
    }

    async fn save_snapshot(&self, aggregate: &A) -> Result<(), AdapterError> {
        let connection = self.pool.get().await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        connection.execute(&format!("INSERT INTO {}_event_store_snapshots (aggregate_id, payload)\
                 VALUES ($1, $2) ON CONFLICT (aggregate_id) DO UPDATE SET payload = $2", A::name()), &[&aggregate.aggregate_id(), &serde_json::to_value(aggregate).unwrap()]).await.map_err(|err| AdapterError::Other { error: err.to_string() })?;
        Ok(())
    }
}

impl<T: DeserializeOwned> TryFrom<tokio_postgres::Row> for Event<T> {
    type Error = AdapterError;

    fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
        let payload = serde_json::from_value(row.get::<_, serde_json::Value>(4)).map_err(|err| AdapterError::Other { error: err.to_string() })?;
        Ok(Self {
            aggregate_id: row.try_get(0).map_err(|err| AdapterError::Other { error: err.to_string() })?,
            event_id: row.try_get::<_, i64>(1).map_err(|err| AdapterError::Other { error: err.to_string() })? as u64,
            created_at: row.try_get(2).map_err(|err| AdapterError::Other { error: err.to_string() })?,
            user_id: row.try_get(3).map_err(|err| AdapterError::Other { error: err.to_string() })?,
            payload,
        })
    }
}