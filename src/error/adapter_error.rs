use alloc::string::String;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum AdapterError {
    InconsistentEventOrdering,
    Other { error: String },
}
