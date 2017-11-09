Ignoring
========
- Partitions, hunks
- Datatype versioning
- Datatype migration DAG
- Dependent dtype versioning resolution/sequencing
- Cross-store dependencies
- Sub-store dependencies (e.g., PG image stack backed by DVID/etc.)
- Multi repo DBs
- Different dtype graphs for different AGs
- AG versioning
- AG/VG caching


Guidelines
==========
- Raw AG and assoc datastructures should know nothing about context, controller, store or version
- [Is this true?] Dtype controllers are specific to a type of store (e.g., PG), but not to the specific store (e.g., single DB/conn). Instance context is passed to FNs or included as state in a wrapping object.
- Dtype descriptions are ways to express dtypes in serializable ways. So is there a AG description? (VG should never need description, can only be loaded through a reified AG.)


- [x] First Goal: create PG repo with dtypes
  - Assume 1 DB == 1 Repo for now
  - Assume dtype graph shared between all repo's AGs
  - Where does init of dtypes happen?
    - I.e., who calls `build_module_datatype_modules`/`DatatypesController.register_datatype_models`?
  - How to store dtype controllers register (so that, e.g., store repo init can run their migrations)?
  - How do PG store dtype controllers specify their migrations (*DO NOT* worry about dependent dtype version sequencing for now)
- [ ] Goal: load artifact graph from DOT (correction - JSON) (with dummy/trivial datatypes?), persist to Postgres
  - Things that have to work for this:
    - [x] De/serialize artifact graph with serde, or some sort of graph description?
    - Who is responsible for loading/creating artifact graphs? How is that associated with stores? Is it a memory-store AG mirroring a pg-store AG, or just an inmemory version of the pg-store AG?
      - I think this all boils down to: how is the AG-store controller/interface different than that for dtypes?
  - TODO:
    - [x] AG schema
    - [x] Artifact schema
    - [x] How do the lifetimes work? Need to depend on DatatypeRegistry somehow?
    - [ ] list_graphs
    - [x] create_graph
      - [x] Who creates identity? Client, store controller, or store/DB?
        - DB would seem to be out because some backends may not have generators, plus would seem to be
          best to do it at common locus where the data is already loaded, so client or store control.
    - [x] get_graph
- [ ] Goal: version graph for blob dtype (no partitions?)
  - [x] Who owns version tables?
    - If each datatype, may still want to provide macro for creating default PG handling for version tables.
    - If artifact graph datatype controller, will want to provide macros for other dtypes to use those tables (unless the version graph getter is also moved to the ag controller)
    - Do datatypes need to directly access version tables to do clever partitioning, materialization, etc.? This is the only motivation I can imagine for having dtype-specific version tables.
    - Answer: for now, clearly AG or another general controller.
  - [x] Can multiple roots exist in a version graph?
    - Git can, so may as well.
  - May not be able to ignore partitions.
    - Instead, assume partitions are static/immutable, and only deal with the unary partitioning.
    - Must every datatype depend on a partitioning, even if the partitioning is unary?
      - If access is through Hunks/Chunks, would seem to be simplest.
      - Partitions would usually point to the unary partition for their partitioning. Unary partition points to itself (identity) -- but this is a cycle (self-loop), so maybe special case None partition handling is fine? Yes, no "partition" datatype relation will load a fake unary partition. Tricky how not to let this get serialized back with AGs or affect their hash, though.
    - How to get Version's map of current partition chunks? Chunks only reference their creation version.
      - For now, get a list of all partition IDs from the partition. Walk backwards down the version graph accumulating partitions at each version for partion indices that weren't previously set. If you encounter an unexpected ID, means something is malformed bc partition changed w/o updating partitions right. Will have to think through how to handle unset partition and other partition completion issues separately. Can further optimize on top of this fallback later.
    - Need a few things:
      - [x] Partitioning trait that all partition datatypes implement
        - [ ] Will also eventually need more tailored generic types, e.g., spatial partitionings for partitions that have bounds to allow for generic repartitioning/partition split/merge
      - [x] Way to get this from datatype controllers
      - [x] Unary partition
      - [x] Dummy unary partition instance/singleton
      - [.] When does dummy partition get injected?
        - When retrieving partition relation from version graph
          - Problem: all of these interfere with uniform hashing behavior
        - Art/version graph must have it
        - Art/version graph must have it AND all artifacts must explicitly relate
        - Only injected when partitioning is needed, possibly even with a dummy partition directly, not even dummy partitioning
      - [x] Decide: how are dummy unary partitions not serialized/etc? UnaryPartitioning controller handles it.
      - [ ] Postgres: need way for controllers to get unique IDs/prefix for dtype+artifact+version
        - Do we really need this? Already have hunk ID.
- [ ] Goal: artifact graph with producer: test fake dtypes `nodes` and `components`, with a producer that computes CCs of node arborescences
  - Demonstrates:
    - Producer flow
    - Registration of custom dtypes
    - Dependent states
- [ ] Goal: delta state updates in fake dtypes test
- [ ] Goal: partitions in fake dtypes test
- [ ] Goal: organize, e.g., postgres stores out of datatypes
- [ ] Goal: branches/tags/reflist
- [ ] Goal: rocket list of dtypes/ags (hera-server)
- [ ] Goal: plotly plot of dtypes/ags (hera-server)
  - 3 stratified plot areas: dtypes, AG, VG


- Are producers just datatypes?
  - Need to be in version graph to preserve art vers relationships across producer nodes
  - Their version ID would make it easy to store logs/metadata output
  - OK: for now leave them as separate concept but incude them in version graph


Dtype controllers and web view:
- Dtypes also have (non-model) APIControllers. The expose higher-level semantic operations, but are only able to interface with (non-store-specific) ModelControllers. Not clear if these are HTTP specific. (Decision: they aren't)
  - These higher level controls should also be more focused on the payload datatypes and note take the internal datastructures (AGs, VGs, etc.) as arguments, only identifiers for them.
  - FFI use should be able to call either the ModelControllers or APIControllers
    - Scratch that, use as a rust lib should be able to call either, FFI will most likely only be able to call API controllers because of lifetimes
- Also have view controllers (in JS/TypeScript or via emscripten?) that can only talk to HTTP APIControllers. These can be embedded in the web frontend, so that it doesn't need to be aware of every dtype.


- There is some common abstracted structure between descriptions, identified entities, instantiated entities, etc. For example, AG is both the DAG and the DAG + ID. Should structure this.
  - Originally had store model methods take descriptions and return identities, but store should take identified, non-description objects



This is how it works:

- There *is* some type of datatype registry/controller.
  - When you init a repo, it ingests one (and mirrors it into the server).
  - When you connect to an existing repo, you take in a reference mapping and attempt to reconstruct one out of what's specified in the repo.? *OR* you just say fuck it if the repo is out of date and offer to migrate. <-- Do this for now!
    - So for now just verify that the repo version matches the materialized version from the lib
  - SO: either Context has a registry or repo does
    - What's the difference?
  - So this may mean there's no difference btwn Model/DatatypeDescription/Datatype
    - Except maybe the model still builds the description/datatype, which is what gets put in the graph?
  - Datatype library modules have a function to build their registry? But calling bin/lib has responsibility to build a final registry and pass it around to kick things off.

- Generic lib structs on some store type are out of the question because they will be impossible to do reasonable FFI with. Ugh.
- Options:
  - Whole model is generic on store
    - If model is a struct and instead attempt to make controller generic on something, will still end up needing to make model generic
    - No, bc then RepoController can't be made into a trait object
      - Or maybe this is indication this call chain here needs to be inverted. Can a scheme be designed where either only SMCs call RC or RC calls SMCs?
        - Option 1: RC only calls SMCs:
          - This one is terrible because RC needs to mirror all SMC interfaces
        - Option 2: SMCs only call RC
          - Then how do SMCs do, e.g., schema migration? They can't, without a tedious event hook system.
        - These points are in favor of the enum approach as the way to move forward for prototyping.
  - Go back to having model return different (non-generic) MetaControllers based on store
    - Can't share impl
    - Worse (breaking): repo controller couldn't call store-specific methods on MC
- Maybe traits are the wrong way to go about repo contexts? Instead an enum wrapping the specialized controller type? Could even have MetaController be enum wrapping different impls
  - This works (b74c9fec8817d84b2ee13b8bf17b06ca199c2888), but involves seemingly needless casting/unpacking of enums. Might look to schema manager inplementation of its adapters for insight into this (although the schema manager would not need to have circular calls like PR -> SMC -> PR)
