**THIS IS AN ABANDONED PROBLEM SPACE EXPLORATION AND NOT COMPLETE OR FUNCTIONAL FOR REAL DATA WORKFLOWS**

There may be references in comments and code to VISAG and ESCHATOM. These were domain data and service libraries built on top of Heraclitus that are unlikely to be made public.

# Heraclitus [![Build Status](https://travis-ci.org/aschampion/heraclitus.svg?branch=master)](https://travis-ci.org/aschampion/heraclitus)

Heraclitus is a framework for the specification, persistence, and production of dependent, versioned, derived data artifacts. This framework provides a differential dataflow computation graph engine focused on the persistence and versioning of data artifacts at each edge of the graph.

Succinctly, imagine one has a data *artifact* `A` (such as a table or schema) and a data artifact `B` that is partially derived (such as denormalized) by production process `P` from data in `A`. If a new *version* of `A`, `A_2`, can be created that descends from the original version, `A_1`, there are several affordances in describing, accessing, and producing our data artifacts that would be useful:

- Describing the dependence structure between artifacts (`B` depends on `A`) as a directed acyclic graph (DAG).
- Describing the version structure of individual artifacts (`A_2` descends from `A_1`) as a DAG.
- Describing the dependence structure between artifact versions (`B_1` depends on `A_1`) as a DAG.
- Triggering and tracking the asynchronous production of dependent data artifacts (producing `B_2` once `A_2` is created), so that this does not have be done synchronously and intra-transactionally with creation of dependency artifacts.
- Allowing descendant versions to be more parsimoniously described as state deltas rather than complete states when appropriate (`A_2` is described by `A_1 + A_1Δ2`).
- Allowing dependent data to be produced either as state or deltas through combinations of state and deltas of dependency artifact versions as appropriate (`B_2 = B_1 + P(A_1, B_1, A_1Δ2)`).

Heraclitus provides a framework for all of these capabilities, as well as many others such as partitioning, version reference tracking (similar to branches and reflists in git), version merging, and data schema migration.

Currently Heraclitus supports these backing databases:

- PostgreSQL

## Name

> B12. *potamoisi toisin autoisin embainousin hetera kai hetera hudata epirrei.*
>
> On those stepping into rivers staying the same other and other waters flow. (Cleanthes from Arius Didymus from Eusebius)
>
> ...
>
> If this interpretation is right, the message of the one river fragment, B12, is not that all things are changing so that we cannot encounter them twice, but something much more subtle and profound. It is that some things stay the same only by changing. One kind of long-lasting material reality exists by virtue of constant turnover in its constituent matter. **Here constancy and change are not opposed but inextricably connected.**
>
> &mdash; <cite>[Daniel W. Graham on Heraclitus, *SEP 2015*][1]</cite>

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.


[1]:https://plato.stanford.edu/entries/heraclitus/#Flu
