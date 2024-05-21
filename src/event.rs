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
    feature = "serde",
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


