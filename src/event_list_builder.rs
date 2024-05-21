use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::{Aggregate, Event};

/// A utility for creating a sequence of events
#[derive(Debug)]
pub struct EventListBuilder<E> {
    list: Vec<(E, Option<DateTime<Utc>>)>,
    user_id: Option<String>,
}

impl<E: fmt::Debug> EventListBuilder<E> {
    pub fn new() -> Self {
        Self {
            list: vec![],
            user_id: None,
        }
    }

    pub fn add_event(mut self, payload: E, created_at: Option<DateTime<Utc>>) -> Self {
        self.list.push((payload, created_at));
        self
    }

    pub fn user_id(mut self, user_id: &str) -> Self {
        self.user_id = Some(user_id.to_string());
        self
    }

    pub fn build<A: Aggregate<E> + fmt::Debug>(self, aggregate: &A) -> Vec<Event<E>> {
        let mut offset = 0;
        self.list
            .into_iter()
            .map(|(payload, created_at)| {
                let event = Event {
                    aggregate_id: aggregate.aggregate_id(),
                    event_id: aggregate.version() + 1 + offset,
                    created_at: created_at.unwrap_or_else(|| Utc::now()),
                    user_id: self.user_id.clone(),
                    payload,
                };
                offset += 1;
                event
            })
            .collect()
    }

    pub fn build_new(self, aggregate_id: Uuid) -> Vec<Event<E>> {
        let mut offset = 0;
        self.list
            .into_iter()
            .map(|(payload, created_at)| {
                let event = Event {
                    aggregate_id,
                    event_id: 1 + offset,
                    created_at: created_at.unwrap_or_else(|| Utc::now()),
                    user_id: self.user_id.clone(),
                    payload,
                };
                offset += 1;
                event
            })
            .collect()
    }
}
