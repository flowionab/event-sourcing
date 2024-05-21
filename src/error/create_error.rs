use crate::error::AdapterError;
use alloc::string::String;
use snafu::Snafu;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum CreateError {
    #[snafu(display("Adapter error"))]
    Adapter { source: AdapterError },

    #[snafu(display("At least one event must be provided, the list of events was empty"))]
    EmptyEventList,

    #[snafu(display("Nil UUID can not be used as an aggregate id, perhaps you forgot to set it"))]
    NilUuid,

    #[snafu(display("The events had varying aggregate ids, this isn not allowed, all events must have the same aggregate id"))]
    InconsistentAggregateId,

    #[snafu(display(
        "The events was in wrong order, the events must follow a consistent ordering such as 1,2,3"
    ))]
    InconsistentEventOrdering,

    #[snafu(display("The aggregate with id '{id}' does already exist"))]
    AggregateAlreadyExists { id: Uuid },

    #[snafu(display(
        "An aggregate with id '{external_id}' does already exist for the external id 'id'"
    ))]
    ExternalIdAlreadyExists { external_id: String, id: Uuid },
}
