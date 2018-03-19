pub mod petgraph {
    use petgraph::prelude::*;
    use petgraph::Direction;
    use petgraph::visit::{
        Data,
        GraphBase,
        GraphRef,
        IntoEdgesDirected,
        IntoNeighborsDirected,
        Visitable,
        VisitMap,
    };

    use ::Error;

    /// Perform a topological sort of a node's induced stream in a directed graph.
    ///
    /// Based on petgraph's `toposort` function. Can not return petgraph cycle
    /// errors because they are private.
    pub fn induced_stream_toposort<G, F>(
        g: G,
        sources: &[<G as GraphBase>::NodeId],
        direction: Direction,
        edge_filter: F,
    ) -> Result<Vec<<G as GraphBase>::NodeId>, Error>
        where G: IntoEdgesDirected + IntoNeighborsDirected + Visitable,
              F: Fn(&<G as Data>::EdgeWeight) -> bool,
    {
        with_dfs(g, |dfs| {
            dfs.reset(g);
            let mut finished = g.visit_map();

            let mut finish_stack = Vec::new();

            for i in sources {
                if dfs.discovered.is_visited(i) {
                    continue;
                }
                dfs.stack.push(*i);
                while let Some(&nx) = dfs.stack.last() {
                    if dfs.discovered.visit(nx) {
                        // First time visiting `nx`: Push neighbors, don't pop `nx`
                        for succ in g.edges_directed(nx, direction)
                                .filter_map(|edgeref| {
                                    if edge_filter(edgeref.weight()) {
                                        Some(match direction {
                                            Direction::Incoming => edgeref.source(),
                                            Direction::Outgoing => edgeref.target(),
                                        })
                                    } else {
                                        None
                                    }
                                }) {
                            if succ == nx {
                                // self cycle
                                return Err(Error::TODO("cycle"));
                            }
                            if !dfs.discovered.is_visited(&succ) {
                                dfs.stack.push(succ);
                            }
                        }
                    } else {
                        dfs.stack.pop();
                        if finished.visit(nx) {
                            // Second time: All reachable nodes must have been finished
                            finish_stack.push(nx);
                        }
                    }
                }
            }
            finish_stack.reverse();

            // TODO: Doesn't work with induced stream because reverses whole
            // graph. Not needed for now anyway since all our graphs are known
            // to be DAGs.
            // dfs.reset(g);
            // for &i in &finish_stack {
            //     dfs.move_to(i);
            //     let mut cycle = false;
            //     while let Some(j) = dfs.next(Reversed(g)) {
            //         if cycle {
            //             return Err(Error::TODO("cycle2"));
            //         }
            //         cycle = true;
            //     }
            // }

            Ok(finish_stack)
        })
    }

    /// Create a Dfs if it's needed
    fn with_dfs<G, F, R>(
        g: G,
        f: F
    ) -> R
        where G: GraphRef + Visitable,
              F: FnOnce(&mut Dfs<G::NodeId, G::Map>) -> R
    {
        let mut local_visitor = Dfs::empty(g);
        let dfs = &mut local_visitor;
        f(dfs)
    }
}
