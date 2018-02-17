Ignoring
========
- ~~Partitions, hunks~~
- Datatype versioning
- ~~Datatype migration DAG~~
- Dependent dtype versioning resolution/sequencing
- Cross-store dependencies
- Sub-store dependencies (e.g., PG image stack backed by DVID/etc.)
- Multi repo DBs
- Different dtype graphs for different AGs
- AG versioning
- AG/VG caching
- Grouped update of multiple artifacts/versions
- Partition/dtype coupling problem


Guidelines
==========
- Raw AG and assoc datastructures should know nothing about context, controller, store or version
- [Is this true?] Dtype controllers are specific to a type of store (e.g., PG), but not to the specific store (e.g., single DB/conn). Instance context is passed to FNs or included as state in a wrapping object.
- Dtype descriptions are ways to express dtypes in serializable ways. So is there a AG description? (VG should never need description, can only be loaded through a reified AG.)


Milestone Goals
===============
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
- [x] Goal: version graph for blob dtype (no partitions?)
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
- [x] Easier goal: artifact graph with producer: producer that performs a trivial operation (negate) on a `blob` to yield another `blob`
  - Demonstrates:
    - Producer registration
    - Producer flow
    - Dependent states
  - When do producers become notified of changes?
    - Dependent artifacts get notification hook when parent artifact has new version committed? (Or only for producers since they're the only type which can handle such evenst?)
- [x] Goal: chained negation producer
  - Demonstrates:
    - Cascading/propagating production
    - [x] Content hashing (for equivalence between source and sink blobs)
  - Requires:
    - [x] De-DAGging datatypes
- [x] Goal: partitions in fake dtypes test
  - Requires:
    - [x] Fixed arbitrary partitioning
      - [ ] Should only be able to mutate fixed partition ID set when bootstrapping new version.
    - [ ] Better ergonomics for building AGs, invoking producers
- [x] Goals: partial partitions update in fake dtypes test
  - Requires:
    - [ ] Clearer distinction between delta and partial state representations
      - One possibility: separate these at version level, that is, one representation kind that specifies partition density and another that specifies compatible representation kinds of those partitions
        - If this, would the same representation kinds work for the partition-level repr? Specifically, what is a partition-level cumulative delta? Would seem to be collection of all changed partitions since some previous full state -- not unreasonable, but unlikely to be frequently used.
        - Doesn't seem that this is actually needed anywhere, and can be determined from the version easily.
      - Let's enumerate the problems:
        - Even with a stateful version, how to efficiently enumerate all the hunks fulfilling sufficiency for that version (i.e., single-hunk-per-partition resolution)
        - Extending this, with delta versions, enumerating the sets of hunks fulfilling sufficiency
        - Not requiring O(|Partitions|) ops when making changes (that means, no full mapping out of hunk<>partitions for each version)
        - Allowing delta-aware producers to be efficient
          - Crtically, how is partition-masking vs. intra-partition deltas handled differently
    - [x] Partition hunk history resolution
      - [x] How to resolve from branched history
        - [ ] Partition-level conflicts must be enumerated
          - Merge version must either
            - [ ] Provide a new state hunk
            - [ ] Designate a "winning" conflict hunk
            - [ ] Designate a "winning" conflict hunk and provide own delta/cumulative delta hunk
          - Requires
            - Hunk precedence relating version to hunk of some ancestor version
              - Should point to hunk directly, or just partition?
              - Model constraints: requires no hunks for that partition on *some* ancestry path from version to ancestor
          - Partition-level conflicts can be determined using only the more proximate of (a) nearest common ancestor (3-way merge base) or (b) sufficient ancestors
          - What about >2 branch merge?
          - Rebasing can reuse much of this
    - [x] Sufficient ancestry
      - Must be to node with state hunk representation, but can be partition-sparse
- [x] Goal: delta state updates in producer test
  - Requires:
    - [x] Representation persistence
    - [x] Resolving sufficient ancestry for materialized state
    - [ ] Producer policies for input and output representations
      - [x] Call this ProductionRepresentationPolicy
        - Producers should provide sets of representation capabilities for different internal production paths, identified somehow (names or ids, etc.)
        - Like with ProductionPolicies, AG selects from among these by some manager, selected policy is stored w/ production version
          - Could be partition-local or something, but really down the rabbit hole with that. For now uniform.
- [x] Goal: organize, e.g., postgres stores out of datatypes
- [x] Goal: branches/tags/reflog
  - Tags: (semi)-immutable VG states
  - Branches: tracking VGs for AG subsets
    - What about branch managers, e.g., squashing policies?
  - A reflog is not necessary as it's persisted in the VG itself.
- [ ] Goal: version merging
  - Can only be done to committed versions w/ identical dependency versions
  - [ ] Partition-wise LCtipA for n versions (lowest common ancestor, but not considering representation sufficiency)
    - Note that this can be descendent or ancestral to each version's sufficient ancestor.
    - How does this deal with partitioning changes?
      - Doesn't have to -- if version dependencies differ, versions are unmergeable?
      - But then have to have needless re- and de-partitioning. Pseudo-cyclic dependence between partitioning and dtypes strikes again.
  - [ ] If partition's compositions differ AND more than one composition's tip is not the LCA hunk, conflict exists.
    - [ ] TODO: prove this.
    - Not actually clear this is the case. If V1P1 has hunk A and V2P1 has hunk C merging A,B with precedence on B, this should not actually be a conflict because a successful merge over A has been made, but A is *not* the LCSA. So can't assume LSCAs per partition, because it would mark this as a conflict.
      - This is a **major** problem, but maybe one that should be ignored for now.
        - If collect all ancestors, conflict free (for n=2 **only**) would be either:
          - Composition map completes ancestral (inclusive to LCA, comp map tip) to *all* LCAs
          - At most one composition map contains *any* descendents to *all* LCAs (messy)
            - Becomes much more complex for n-way merge base, not as simple as git octopus
            - https://www.kernel.org/pub/software/scm/git/docs/git-merge.html#_merge_strategies
              - Git is combining several things hera is factoring: conflict-free merging, generic conflict resolution, dtype-specific conflict resolution, and manual conflict resolution. Further, notion of what is being merged is different because of partitioning and precedence. Finally, git seems to assume single root (uncertain of this), where as nothing in hera yet does (precedence results in single per-partition roots, but could belong to different root versions).
              - Should also refresh memory of DARCS.
      - Should this just be considered a non-dtype-specific resolvable conflict, rather than conflict-free? E.g., `ExtantPrecedenceResolver`?
        - Even if so, still need to detect this common ancestry.
  - [ ] Conflict resolution strategies:
    - Yield precedence maps
    - Don't have to resolve all conflicts, to allow layering and manual resolution
    - Need some way to persist for staging/conflict versions?
    - [ ] Non-dtype specific: last-hunk-mtime-wins. But does require knowledge of partition dependence relationships.
      - [ ] Need to add ctime, mtime to versions
    - [ ] Dtypes must be able to declare custom resolution strategies. May also want policies for strategy selection per-artifact, but not immediately necessary.
- [ ] Goal: branch merging
  - Zipper up toposort artifact merge
    - Includes arts not tracked by branch ref?
- [ ] Goal: artifact graph with producer: test fake dtypes `nodes` and `components`, with a producer that computes CCs of node arborescences
  - Demonstrates:
    - Registration of custom dtypes
    - Optionally: change sets
  - Data structure design:
    - This would not support a CATMAID-like design, where component specifications are in same rows as nodes
      - But with partitioning would make sense, partition local CCs in the lower level (a la CATMAID) with the global component mapping happening via producer
      - Is this a case of dependent dtype or partition-hierarchy heterogenous content?
    - Node hunks:
      - Nodes table per hunk id: local tn id (32b), local frag id (32b), local parent id (32b)
      - Neighbor table: partition id a (64b), partition id b (64b), local tn id a (32b), local tn id b (32b)
    - Component hunks:
      - Mapping tables per hunk id: global skel id (64b), local frag id (32b)
      - Unary mapping table: hunk id (64b), global skel id (64b), partition id (64b)
      - OR just: unary table: hunk id (block), global skel id, local frag id
    - Drawbacks:
      - Getting skels requires either fetching and processing all node blocks for global skel, or having a materialized skeleton dep datatype
      - Node history for moves across partititions is a mess
      - How to get ID-invariant hashes? Or are they even wanted?
        - ID invariant hashes wanted so that clients performing same edits yield equivalent hunks
          - May not be valuable for skeleton nodes in any case, since chance of clients performing identical edits even for loosely semantically equivalent actions is vanishingly small (e.g., because of float positions, arbitrariness of placement, etc.)
          - But not true for other types of edits, e.g., split, deletion.
      - Lots of trivial tasks become tractable but not trivial (i.e., going from a global skel to an individual node)
  - Seems inefficient for updates, requiring creation of many hunks for each action as simple as, e.g., skeleton merging. May need to make a decision re: squashing head versions or streaming changes. Could be ameliorated at the hunk vs. changeset layer?
- [ ] Goal: rocket list of dtypes/ags (hera-server)
- [ ] Goal: plotly plot of dtypes/ags (hera-server)
  - 3 stratified plot areas: dtypes, AG, VG


Design Questions
================

General
-------
- Can versions depend on staging versions?
- Can dependencies change after a version is created (i.e., if it is still staging)? (~~Related to above question~~ ~~not related to above because this only constrains dependencies, not dependents~~)
  - Would greatly simplify if the case. Only hunks/content/hash of a staging version could change.
- [x] Datatypes Registry should not be a DAG. This is cruft from early testing of daggy. E.g., even the testing negating blob producer datatype both inputs and outputs blobs. Only AGs/VGs must be DAGs.


Producers
---------
- Are producers just datatypes?
  - Need to be in version graph to preserve art vers relationships across producer nodes
  - Their version ID would make it easy to store logs/metadata output
  - OK: for now leave them as separate concept but incude them in version graph
  - More motivation for this: for Rust/FFI call producers, need to be a defined set in a registry so that they can be de/serialized
    - But for producers to be datatypes in this way, would also need to have datatype extension/implementation working, since many types of producers would be implementing a common production interface (for version event hooks)
    - Would mean a `Producer` (trait-like) is a datatype that can create new versions of itself and child artifacts in response to events
      - Has some nice parity with the revised perspective of `Partitions` being datatypes that provide a particular controller
  - Any downsides to abstracting producers to datatypes?
    - Loss of immediate variant disambiguation in AG (although this simplifies many AG ops)
      - Downsides:
        - If only producers are notified of version changes to parent arifacts, how to determine if a child is a producer w/o constructing a controller?
          - Datatypes could specify set of Trait-likes (producer/partitioning) they implement (e.g., with an EnumSet)
    - Risk inventing a whole interface/trait system in the datatype/metadata graph
      - For example, if VISAG provides a hierarchy of spatial partitioning types (with multiple implementing datatypes for each), how would this work with the dtype trait paradigm?
      - Illustrated in that a natural way to represent these Trait-likes would be yet another level in the graph hierarchy, one of trait subsumption sitting above the datatype graph
        - An alternative is having abstract types in the datatype graph


Dtype controllers and Web View
------------------------------
- Dtypes also have (non-model) APIControllers. The expose higher-level semantic operations, but are only able to interface with (non-store-specific) ModelControllers. Not clear if these are HTTP specific. (Decision: they aren't)
  - These higher level controls should also be more focused on the payload datatypes and note take the internal datastructures (AGs, VGs, etc.) as arguments, only identifiers for them.
  - FFI use should be able to call either the ModelControllers or APIControllers
    - Scratch that, use as a rust lib should be able to call either, FFI will most likely only be able to call API controllers because of lifetimes
- Also have view controllers (in JS/TypeScript or via emscripten?) that can only talk to HTTP APIControllers. These can be embedded in the web frontend, so that it doesn't need to be aware of every dtype.


- There is some common abstracted structure between descriptions, identified entities, instantiated entities, etc. For example, AG is both the DAG and the DAG + ID. Should structure this.
  - Originally had store model methods take descriptions and return identities, but store should take identified, non-description objects

Fixing the Trait Object/Dtype Madness [DONE]
--------------------------------------------
- Datatypes registry is generic over an end-lib defined enum of Datatypes (also one of interfaces), which implements some traits for iterating over them
  - Means no longer need to Box up Model as a trait obj, which would allow for generic methods (e.g., for getting interface controllers)
  - Instead enum has methods for getting its contained variant as &Model, etc.
  - ~~interface controllers are InterfaceController<I: Interface>, so that Model::interface_controller<I>(&self, store: Store) -> InterfaceController<I>~~ interface controllers have different Traits, doesn't solve interface_controller problem
    - Could also have iface controller enum (w/ variants like Producer(Box<ProducerController>)), so Model<IfaceEnum, IfaceControllerEnum>::interface_controller(&self, store: Store, iface: IfaceEnum) -> Option<IfaceControllerEnum>
      - Can simplify IfaceControllerEnum with macros
  - [x] To get rid of unsafe transmute in interface controllers:
    - Make interface_controller<T>'s generic specific to the controllers it can produce
      - For (a nonsensical) example, <T: PartitioniningInterfaceController + ProducerInterfaceController>
        - Need to check if this works when a trait is a subtrait, e.g., SpatialPartitioningControler : PartitioningController

Uniform Grid Partitioning
-------------------------
Must either:
- Have bounds
- Be sparse or lazy (and append-only)
  - Either way, artifact co-dependence issues
- Deterministically create IDs from grid coordinates, persist nothing but parameterization (z-order)
  - Can't `get_partition_ids` -- could change partition interfaces into enumerable/unenumerable
    - Currently only used for convenience in AG test cases
    - How would unenumerable partitions work for hunk resolution? Nothing to pass into `get_composition_map`

Artifact co-dependence resolution options
-----------------------------------------
- Don't, and let dtypes call methods on their partitions against the dependence flow, trust to not produce endless cycles
- Define disjoint, DAG-preserving atomic subgraphs which are internally not necessary DAG
  - Note for any reasonable use case this would likely have same constraints as below
- Roll partitioning *into* datatypes (essentially a tuple version of above subgraph w/o exposure to hera), other dtypes can depend on partitioning from another dtype but cannot affect it
  - I.e., a product type like `ProductDtype<A: Model, B: Model>`, can impl Model using union of A, B, but A, B implements must be disjoint
    - Representations are intersection of representations?
    - Dependencies could be more involved
  - How would update resolution actually work? Product type would need to know which relationships are exposed and which are internal, coordinate versioning schemas, etc. A mess.
- Version of above, but make the tupled datatype a producer on which both partitioning and paired dtype are dependent, so that the producer can manage the synced dependence update. Again effective limits coupling to be between partitioning and a single dtype
- [More specific than above] Dtypes can only successfully apply changes to existing parts; part mutating changes must go through ancestral producer.
  - A way to combine this with special interfaces on dep dtypes to avoid creating custom prods for each case like this? E.g., prod proxies changes to dtype, which lets it know if change requires co-dependent changes with partitioning?
    - E.g., `CoupledProducer<D: Model<T1>, P: Model<T2>> where T1: InterfaceController<PartitioningCouplingController>, T2: InterfaceController<PartitioningController>` (or do it through reflection in `Description.implements`)
      - Still have staging version problem, to even check this dtype has to have staging version, which then has to be changed.
    - Would this still limit to single mutating dep dtype per partitioning?
      - Doesn't seem so, since DAG doesn't interfere. Converging cascading changes would have to be thought through.

- Independent of choice of solution, still have problem that for (dtype, part) (A, B), a change in (staging?) version A2 (dep. B1) triggers mutation of B1->B2, which must change dep from B1->B2
  - Can deps change after creation? (def. not after commit)
    - At present, no: `create_staging_version` sets up relations, `commit_version` only changes status and hash.
      - Why even persist volatile versions? (obv. want to keep ability for staging state, but can do so when temporarily disowned, e.g., async processing)
      - OTOH, having deps fixed at staging creation is a useful strict simplifier.
  - In any case, if A has a prod dep on B have the problem of not generating an A3 following commit of B2.
    - Can set up atomic "closures" over commit cascading, but would be more or less a dynamics version of the atomic subgraph solution.
    - Can return to producers as hypergraph DAG instead of in artifact DAG, which would effectively group production outputs into atomic subgraphs in artifact graph DAG. (So dtype would be dep on partitioning, but producer would handle all muts to dtype?)


Solution Sketches
=================

Datatype Registration [DONE]
----------------------------
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


Producers
---------
Assume decide to go with datatype traits. What would solutions to the goals look like?

Trait vs. Interface: latter is signature only, former may include implementation. For now these commonalities are signature-only, so call them interfaces.

Interface Graph: (no need to serialize)
  - Partitioning
    - (Already specified)
  - Producer
    - produce(&mut self, repo_control: &mut repo::StoreRepoController, version: &Version)
  - [x] This makes the unary partition singleton even more tedious. If it has its own static interface graph, then interface checks will fail bc its interfaces are not the same as those in the DtypeReg iface graph instance.
    - Options:
      1. Make an InterfaceRegistry static mut global
      2. Move unary partition singleton out of static into one of:
        - AG
        - Context
          - Would seem an obvious candidate, but won't this run into rental lifetime problems?
            - Yes it does, since AG/Version lifetimes are bound by the DtypesRegistry that Context owns. May be an valid case for using the `rental` crate, though.
        - ~~DtypeRegistry~~ (Can't know anything about versions)
      3. Reify unary partition in AG/VGs and make dtype part dependence explicit & req for all arts
      4. Just rely on callers to provide the unary partitioning singleton iff partitioning is None
        - Go with this option for now since other uses are not yet well-defined enough to weigh options.

Datatype:
  - Implements: set of handles to interfaces
    - Downside: Datatype now also has handles and lifetime bound by a interface registry

DatatypeDescription:
  - Dependencies should be either concrete datatypes or any providing interface
    - Could be done with an enum relation
    - If interfaces were instead abstract datatypes, this would be somewhat more ergonomic to specify (although probably not to check), but interfaces are clearly not abstract datatypes because they have neither method implementations nor associated data members

Easier goal: artifact graph with producer: producer that performs a trivial operation (negate) on a `blob` to yield another `blob`
  - Testing producer datatype `Negate`
  - Unanswered:
    - [x] Who generates producer version before calling producer?
      - Difficulty here is knowing what parent dependency edges the producer version should have for parents other than the triggering artifact version.
        - Consider producer P dependent on artifacts A, B. An obvious strategy (among other possibilities) when a new version A1 -> A2 is generated is to create new versions based on all extant (A1, Bx) -> Py. For most cases this would be only a single set so would not branch the version graph. Other strategies could be based on branch specs, etc.
      - So if there's a producer strategy, a managing entity could generate new producer versions, but what is it?
        - Artifict Graph MC, but it should be responsible for wrapping (i.e., dtype MCs don't call it/notify it)
    - [x] Who actually notifies of change/invokes produce?
      - Issue here could be that AG can't call concrete MCs, but because Producer is a declared interface, AG should be able to construct the correct controller in the same way as it does for partitionings
      - AG MC does after `commit_version`
    - [ ] Does producer have to construct version for dependent artifact or can this be encapsulated somehow?
      - Pre-constructing the dependent artifact versions (e.g., in AG) could have advantages for cross-artifact controllers, like neuron merging, because can already inject changes into downstream artifact versions (like annotation assignment)
    - [ ] What is producer partitioning? E.g., in CATSOP I kick off a resolve for a core, which also requires a remapping of assemblies. In this particular case this would probably be two producers, but in the single case requiring both partition-local and neighborhood/global ops, how is this communicated between hera and the producer?
      - Presumably producer can decide this, based either on the partitioning of its own artifact or of the parent artifact that changed

Production ~~strategies~~ policies:
- Extant (copied from above):
  - When a new version A1 -> A2 is generated is to create new versions based on all extant (A1, Bx) -> Py. For most cases this would be only a single set so would not branch the version graph.
    - Q: How is this bootstrapped?
      - What about the case where A is the only dependency of the producer? Can detect and auto bootstrap in this case, but what if there are other dependencies?
      - Do bootstrap in another strategy (LeafBootstrap), allow and merge multiple strategies
    - Q: What about a new version with multiple old parent versions?
- Leaf bootstrap (LeafBoostrap):
  - If there exist only and exactly a single leaf version for all dependencies, create a producer for these.
So a production strategy takes a version graph and returns a Set of version ID tuples w/ the new version of the triggering artifact.
  - Does not handle multiple artifact versions being updated together.
  - Problem: diff. production policies will require diff. version subgraphs to be filled out
    - Ex: extant needs the neighborhood of all production versions dependent on the parents of the new dependency version, while leaf bootstrap needs all versions of the artifacts on which the production artifact depends.
      - Can this be specified w/ walker types or closures?
      - Would this require the sparse graph ver graph representation?
      - Way to efficiently check vs retrieve?
      - Sounding like a query lang, which is bad. Step back, constrain, de-abstract.
        - Should be able to require things of
          1. Existing producer versions ~~related to the parents of the new dependency node~~
            - Don't care, related to the parents of the new dependency node, all.
          2. All artifacts on which this producer depends
            - Don't care, related to any ver of this producer, all (w/o their neighbors?).
          Because producer can only care about its neighbors, there are no other reasonable requirements of the VG content

Eventually, production policies should be associated with each dependency->producer relation. For example, changes in constraint dependencies for a multicut solver producer might have Extant policy, but changes in a configuration dependency for the same producer might have a more conservative policy.

Eventually, production policies *may* need to take the representation-production capabilities of the producer (e.g., Deltas vs. States for deps and products) into account. Unclear, as this might be handled purely by the producer.
  - [.] Should "representation" of a producer match representation of its products? If so, should all products have the same representation kind?
    - Should be up to the producer to decide.

Change notification/propagation:
  - When a version is committed:
    - [x] Check for producers
      - [x] If any, apply production strategy based on parent version to generate producer versions
      - [x] Notify/invoke producer for generated producer versions
  - First problem: how to specify version to commit
    - ID only requires re-fetching local graph
    - Can have either ID *OR* VG + VGIndex
      - But need to verify VG is current
        - Checking numrows when updating status w/ ID + hash sufficient for changes to node itself, but will not capture changes to version relations. Can't exploit AG versioning because choosing to ignore that for now (for good reason), and it's not clear it would be sufficient because only local changes are relevant.
    - For now, ID only


Squashing Head Versions
-----------------------
Model for squashing head versions:

[History] -> [Proximal parent (for rebasing, non-squashed)] -> [Squashing version] -> [Delta versions pending propagation through the AG]

- Once a delta version has been propagated to all dependent nodes in AG, gets squashed/appended into squashing version
- APIControllers act directly on squashing versions/pending delta versions
- ModelController clients must attempt to work on the squashing version and rebase on proximal parent if squashing version has changed

Alternatively:
[History] -> [Prox. parent] -> [Delta versions being propagated]
                 \\-> [Squashing version]


CATMAID Emulation Naming
------------------------
CHAVES - CATMAID HTTP API VISAGE-backed Emulation Service
ESCHATOM - Emulation Service for the CATMAID HTTP API Towered (O)n MANCCR
VATCAED - VISAGE Annotation Toolkit for CATMAID API Emulation Dataservice


Misc. Cleanup
-------------
- [x] `IdentifiableGraph::find_by_{id,uuid}` should be `get` not `find` to match Rust conventions.
  - [ ] Could also implement index methods to provide panicing access for known items.
- [ ] Empty (hunkless) versions are current allowed. Somewhat nice that this runs through cascade production, etc., without error, but still.
- [ ] Kill magic values
  - [ ] Dependency/input/output name strings
  - [ ] Interface identifiers
  - [ ] Datatype identifiers
- [ ] Model controllers should have a verify_hunk_hash method, e.g., if in trait Foo would impl<T: DatatypesModelController> Foo<T>. Not clear if these sorts of methods should be on datatype::MetaController or datatype::ModelController.
- [ ] Refactoring attempt: Since models no longer need to be boxable, could make controllers an assoc type, which would finally remove need to box them?!
  - Would allow model controllers to specify their payload types


Cons to Hera & VISAG
====================

Architecture
------------
- Complexity
- No good Rust-native image processing yet
- Cyclic dependence between partitioning and datatypes
- [List of relaxing assumptions from notes]

Implementation
--------------
- Reflection of interfaces/datatypes/dependencies into store is a mess
  - E.g., interfaces like traits, prevents generics
- Volatile datatypes like caches are a mess
- No well-defined caching yet
- Stateless vs. stateful design
- Often requires whole graphs loaded

Current Impl
------------
- Production only works from parent hunks
