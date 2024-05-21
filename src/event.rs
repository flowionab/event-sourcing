use alloc::string::String;
use core::fmt;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::EventListBuilder;

/// Represents a single event
///
/// Besides from the payload, the event structure contains some additional helpful fields
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "serde_support",
    derive(serde::Serialize, serde::Deserialize)
)]
pub struct Event<T> {
    /// The id of the aggregate this event belongs to
    pub aggregate_id: Uuid,

    /// The id of this particular event. Events always use a numbered sequence starting from 1
    pub event_id: u64,

    /// When this event did happen
    pub created_at: DateTime<Utc>,

    /// An optional reference to the user who caused this event
    pub user_id: Option<String>,

    /// The event payload
    pub payload: T,
}

impl<T: fmt::Debug> Event<T> {
    pub fn list_builder() -> EventListBuilder<T> {
        EventListBuilder::new()
    }
}

// impl<T: DeserializeOwned> TryFrom<tokio_postgres::Row> for Event<T> {
//     type Error = Box<dyn std::error::Error + Send + Sync>;
//
//     fn try_from(row: tokio_postgres::Row) -> Result<Self, Self::Error> {
//         let payload = serde_json::from_value(row.get::<_, serde_json::Value>(4))?;
//         Ok(Self {
//             aggregate_id: row.get(0),
//             event_id: row.get::<_, i64>(1) as u64,
//             created_at: row.get(2),
//             user_id: row.get(3),
//             payload,
//         })
//     }
// }
