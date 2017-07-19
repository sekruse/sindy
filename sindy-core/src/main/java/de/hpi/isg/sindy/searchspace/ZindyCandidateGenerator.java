package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.sindy.searchspace.hypergraph.NaryInd;
import de.hpi.isg.sindy.searchspace.hypergraph.TransversalCalculator;
import de.hpi.isg.sindy.searchspace.hypergraph.UnaryInd;
import de.hpi.isg.sindy.util.IND;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * This is the default candidate generator as it is used in the Zigzag algorithm.
 * <p/>
 * Created by basti on 8/17/15.
 */
public class ZindyCandidateGenerator implements OptimisticCandidateGenerator<ZigZagSubspace> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZindyCandidateGenerator.class);

    @Override
    public void prepareSubspace(IndSubspaceKey indSubspaceKey, ZigZagSubspace subspace,
                                NaryIndRestrictions naryIndRestrictions) {
        // Nothing to do.
    }

    @Override
    public void generateOptimisticCandidates(IndSubspaceKey indSubspaceKey, ZigZagSubspace subspace,
                                             NaryIndRestrictions naryIndRestrictions,
                                             int minArity,
                                             Collection<IND> collector) {

        // Calculate the transversal of the NINDs and complement them.
        Set<IND> candidates = new HashSet<>();
        TransversalCalculator transversalCalculator = new TransversalCalculator(subspace.strippedNegativeBorder, subspace.unaryInds);
        for (NaryInd transversalComplement : transversalCalculator.complementAll(transversalCalculator.calculateTransversals())) {

            // Further partition the transversals so that they respect the n-ary IND restrictions.
            Collection<Collection<UnaryInd>> partitions = transversalCalculator.partitionUnaryInds(
                    transversalComplement.getUnaryInds(), naryIndRestrictions);
//            if (partitions.size() != 1 && partitions.iterator().next().size() != transversalComplement.getUnaryInds().size()) {
//                LOGGER.info("Partition {} into {}.", transversalComplement.toInclusionDependency(), partitions);
//            }

            // Add the partitions.
            for (Collection<UnaryInd> partition : partitions) {
                IND positiveOptimisticBorderInd = new NaryInd(new HashSet<>(partition)).toInclusionDependency();
                if (positiveOptimisticBorderInd.getArity() >= minArity && !subspace.positiveBorder.contains(positiveOptimisticBorderInd)) {
                    candidates.add(positiveOptimisticBorderInd);
                }
            }
        }

        collector.addAll(candidates);
        LOGGER.info("Created {} optimistic candidates for {}.", candidates.size(), indSubspaceKey);
    }
}
