use crate::error::AdapterError;
use snafu::Snafu;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum StoreError {
    AdapterError {
        source: AdapterError,
    },

    #[snafu(display("Aggregate with id '{aggregate_id}' does not exist"))]
    AggregateDoesNotExist {
        aggregate_id: Uuid,
    },

    #[snafu(display("An unknown error occurred when storing the events"))]
    Unknown,
}
