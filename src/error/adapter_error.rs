use alloc::string::String;
use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum AdapterError {
    Other { error: String },
}
