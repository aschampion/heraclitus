use std::collections::HashMap;

use crate::{
    ArtifactGraph,
    ArtifactGraphIndex,
    datatype::{
        artifact_graph::{
            ArtifactGraphDescription,
            ArtifactGraphDtype,
            ArtifactMeta,
            Storage,
        },
        DatatypeEnum,
        DatatypeMarker,
        DatatypesRegistry,
        interface::{
            CustomProductionPolicyController,
            ProducerController,
        },
        InterfaceController,
    },
    Error,
    repo::Repository,
};

pub fn install_fixture<'s, T: DatatypeEnum>(
    dtypes_registry: &DatatypesRegistry<T>,
    repo: &Repository,
    fixture: &dyn Fn() -> (ArtifactGraphDescription, HashMap<&'static str, ArtifactGraphIndex>),
) -> Result<(ArtifactGraph, HashMap<&'s str, ArtifactGraphIndex>), Error>
    where T::InterfaceControllerType: InterfaceController<ArtifactMeta>,
        <T as DatatypeEnum>::InterfaceControllerType :
                InterfaceController<ProducerController> +
                InterfaceController<CustomProductionPolicyController>
{

    let (ag_desc, desc_idx_map) = fixture();

    let mut model_ctrl = ArtifactGraphDtype::store(repo);
    let (origin_ag, mut root_ag) = model_ctrl.get_or_create_origin_root(dtypes_registry, repo)?;
    let root_art_idx = origin_ag.find_by_name("root").expect("TODO: malformed origin AG");

    let mut origin_vg = model_ctrl.get_version_graph(repo, &origin_ag)?;

    let root_tip_v_idx = origin_vg.artifact_tips(&origin_ag[root_art_idx])[0];

    let (_, _, ag, ag_idx_map) = model_ctrl.create_artifact_graph(
        dtypes_registry,
        repo,
        ag_desc,
        &mut root_ag,
        root_tip_v_idx,
        &mut origin_vg)?;

    let idx_map = desc_idx_map.into_iter()
        .map(|(name, idx)| (name, ag_idx_map[&idx]))
        .collect();

    Ok((ag, idx_map))
}
