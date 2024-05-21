use super::Event;
use uuid::Uuid;

/// Trait used for implementing an aggregate root. The most interesting part being the apple()
/// method that is used for replaying the events onto the aggregate
pub trait Aggregate<T> {
    fn version(&self) -> u64;

    fn aggregate_id(&self) -> Uuid;

    fn apply(&mut self, event: &Event<T>);

    fn new_with_aggregate_id(aggregate_id: Uuid) -> Self;
    fn name() -> &'static str;
}
