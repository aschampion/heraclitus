Goal: load artifact graph from DOT (correction - JSON) (with dummy/trivial datatypes?), persist to Postgres


Ignoring
========
- Partitions, hunks
- Datatype versioning
- Dependent dtype versioning resolution/sequencing
- Cross-store dependencies
- Multi repo DBs
- Different dtype graphs for different AGs
- AG versioning


Things that have to work for this:
- De/serialize artifact graph with serde, or some sort of graph description?
- Who is responsible for loading/creating artifact graphs? How is that associated with stores? Is it a memory-store AG mirroring a pg-store AG, or just an inmemory version of the pg-store AG?
	- I think this all boils down to: how is the AG-store controller/interface different than that for dtypes?


Guidelines
==========
- Raw AG and assoc datastructures should know nothing about context, controller, store or version
- [Is this true?] Dtype controllers are specific to a type of store (e.g., PG), but not to the specific store (e.g., single DB/conn). Instance context is passed to FNs or included as state in a wrapping object.
- Dtype descriptions are ways to express dtypes in serializable ways. So is there a AG description? (VG should never need description, can only be loaded through a reified AG.)


First Goal: create PG repo with dtypes
- Assume 1 DB == 1 Repo for now
- Assume dtype graph shared between all repo's AGs
- Where does init of dtypes happen?
	- I.e., who calls `build_module_datatype_modules`/`DatatypesController.register_datatype_models`?
- How to store dtype controllers register (so that, e.g., store repo init can run their migrations)?
- How do PG store dtype controllers specify their migrations (*DO NOT* worry about dependent dtype version sequencing for now)




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
  - Go back to having model return different (non-generic) MetaControllers based on store
    - Can't share impl
    - Worse (breaking): repo controller couldn't call store-specific methods on MC
- Maybe traits are the wrong way to go about repo contexts? Instead an enum wrapping the specialized controller type? Could even have MetaController be enum wrapping different impls