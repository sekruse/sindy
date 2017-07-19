package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.sindy.searchspace.hypergraph.HypercliqueCalculator;
import de.hpi.isg.sindy.searchspace.hypergraph.NaryInd;
import de.hpi.isg.sindy.searchspace.hypergraph.UnaryInd;
import de.hpi.isg.sindy.util.IND;

import java.util.Collection;
import java.util.Set;

/**
 * Generates n-ary IND candidates by modelling the already existing INDs as hypergraph and finding hypercliques
 * within these. The exact proceeding is described by Koehler et al. (Find2).
 *
 * Created by basti on 8/18/15.
 */
public class HypercliqueCandidateGenerator implements OptimisticCandidateGenerator<ZigZagSubspace> {

    @Override
    public void prepareSubspace(IndSubspaceKey indSubspaceKey, ZigZagSubspace zigZagSubspace, NaryIndRestrictions naryIndRestrictions) {
        // Nothing to do.
    }

    @Override
    public void generateOptimisticCandidates(IndSubspaceKey indSubspaceKey,
                                             ZigZagSubspace zigZagSubspace,
                                             NaryIndRestrictions naryIndRestrictions,
                                             int minArity,
                                             Collection<IND> collector) {
        HypercliqueCalculator hypercliqueCalculator = new HypercliqueCalculator(zigZagSubspace.positiveBorder, minArity - 1);
        Collection<Set<UnaryInd>> hypercliques = hypercliqueCalculator.findHypercliques();
        for (Set<UnaryInd> hyperclique : hypercliques) {
            collector.add(new NaryInd(hyperclique).toInclusionDependency());
        }
    }
}
