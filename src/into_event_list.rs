use crate::Event;
use alloc::vec;
use alloc::vec::Vec;

/// Utility trait for making it easier to return events
pub trait IntoEventList<T> {
    fn into_list(self) -> Vec<Event<T>>;
}

impl<T> IntoEventList<T> for Vec<Event<T>> {
    fn into_list(self) -> Vec<Event<T>> {
        self
    }
}

impl<T> IntoEventList<T> for Event<T> {
    fn into_list(self) -> Vec<Event<T>> {
        vec![self]
    }
}

#[cfg(test)]
mod tests {
    use crate::{Event, IntoEventList};
    use alloc::vec;

    #[test]
    fn init_list_should_work_for_vec() {
        let data = vec![Event {
            aggregate_id: Default::default(),
            event_id: 0,
            created_at: Default::default(),
            user_id: None,
            payload: (),
        }];

        let _ = data.into_list();
    }

    #[test]
    fn init_list_should_work_for_single_event() {
        let data = Event {
            aggregate_id: Default::default(),
            event_id: 0,
            created_at: Default::default(),
            user_id: None,
            payload: (),
        };

        let _ = data.into_list();
    }
}
