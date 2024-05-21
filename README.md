# Event Sourcing

```toml
[dependencies]
event-sourcing = "0.1"
```

*Compiler support: requires rustc 1.56+*

The event sourcing crate contains the core logic for implementing a event sourced backend logic, preferably combined 
with CQRS.

The main struct in this project is the **EventStore**. This struct is used for persisting, fetching and streaming 
aggregates and events.

#### License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>