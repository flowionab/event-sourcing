#![doc = include_str!("../README.md")]
#![deny(warnings)]
#![no_std]

extern crate alloc;

/// The adapter modules contains both types used for implementing an adapter, and prebuilt adapters
/// ready for use
pub mod adapter;
mod aggregate;
mod event;
mod event_list_builder;
mod event_store;
mod into_event_list;

/// This module contains different errors used throughout the crate
pub mod error;
mod event_store_builder;

pub use self::aggregate::Aggregate;
pub use self::event::Event;
pub use self::event_list_builder::EventListBuilder;
pub use self::event_store::EventStore;
pub use self::into_event_list::IntoEventList;
pub use self::event_store_builder::EventStoreBuilder;
