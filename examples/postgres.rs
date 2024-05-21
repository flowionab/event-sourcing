use uuid::Uuid;
use event_sourcing::{Aggregate, Event, EventStore};
use event_sourcing::adapter::postgres::PostgresAdapter;
use event_sourcing::adapter::pubsub::PubsubAdapter;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
enum ExampleEvent {
    Example(),
}

#[derive(Debug, Clone, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct ExampleAggregate {
    aggregate_id: Uuid,
    version: u64,
}

impl Aggregate<ExampleEvent> for ExampleAggregate {
    fn version(&self) -> u64 {
        self.version
    }

    fn aggregate_id(&self) -> Uuid {
        self.aggregate_id
    }

    fn apply(&mut self, event: &Event<ExampleEvent>) {
        self.version = event.event_id
    }

    fn new_with_aggregate_id(aggregate_id: Uuid) -> Self {
        Self {
            aggregate_id,
            version: 0,
        }
    }

    fn name() -> &'static str {
        "example"
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let event_store = EventStore::<ExampleAggregate, _>::builder().event_store_adapter(PostgresAdapter::setup().await?).notification_adapter(PubsubAdapter::setup("test-subscription").await?).build();

    let id = Uuid::new_v4();
    event_store.create(Event::list_builder().add_event(ExampleEvent::Example(), None).build_new(id)).await?;

    Ok(())
}