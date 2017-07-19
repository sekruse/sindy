package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.sindy.searchspace.hypergraph.NaryInd;
import de.hpi.isg.sindy.searchspace.hypergraph.TransversalCalculator;
import de.hpi.isg.sindy.util.IND;
import de.hpi.isg.sindy.util.INDs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This is the default candidate generator as it is used in the Zigzag algorithm.
 * <p>
 * Created by basti on 8/17/15.
 */
public class TransversalCandidateGenerator implements OptimisticCandidateGenerator<ZigZagSubspace> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransversalCandidateGenerator.class);


    @Override
    public void prepareSubspace(IndSubspaceKey indSubspaceKey, ZigZagSubspace subspace,
                                NaryIndRestrictions naryIndRestrictions) {

        List<IND> unaryInds = new ArrayList<>(subspace.positiveBorder);
        unaryInds.sort(INDs.COMPARATOR);

        // Mark IND candidates with overlapping dep and ref as NINDs.
        if (!naryIndRestrictions.isAllowInterRepetitions()) {
            for (int i = 0; i < unaryInds.size() - 1; i++) {
                IND ind1 = unaryInds.get(i);
                int dep1 = ind1.getDependentColumns()[0];
                int ref1 = ind1.getReferencedColumns()[0];
                for (int j = i + 1; j < unaryInds.size(); j++) {
                    IND ind2 = unaryInds.get(j);
                    int dep2 = ind2.getDependentColumns()[0];
                    int ref2 = ind2.getReferencedColumns()[0];
                    if (dep1 == ref2 || dep2 == ref1) {
                        IND pruningNind = new IND(new int[]{dep1, dep2}, new int[]{ref1, ref2});
                        subspace.strippedNegativeBorder.add(pruningNind);
                    }
                }
            }
        }

        // Mark IND candidates with equal dep or ref as NINDs.
        if (!naryIndRestrictions.isAllowIntraRepetitions()) {
            for (int i = 0; i < unaryInds.size() - 1; i++) {
                IND ind1 = unaryInds.get(i);
                int dep1 = ind1.getDependentColumns()[0];
                int ref1 = ind1.getReferencedColumns()[0];
                for (int j = i + 1; j < unaryInds.size(); j++) {
                    IND ind2 = unaryInds.get(j);
                    int dep2 = ind2.getDependentColumns()[0];
                    int ref2 = ind2.getReferencedColumns()[0];
                    if (dep1 == dep2 || ref1 == ref2) {
                        IND pruningNind = new IND(new int[]{dep1, dep2}, new int[]{ref1, ref2});
                        subspace.strippedNegativeBorder.add(pruningNind);
                    }
                }
            }
        }

        // Add trivial INDs to incorporate them in the candidate generation.
        if (naryIndRestrictions.isAllowTrivialInds() && indSubspaceKey.getDependentTableId() == indSubspaceKey.getReferencedTableId()) {
            throw new RuntimeException("Cannot load trivial INDs to merge in for the candidate generation.");
//            IdUtils idUtils = this.metadataStore.getIdUtils();
//            int schemaId = idUtils.getSchemaId(indSubspaceKey.getDependentTableId());
//            Collection<Column> columns = this.metadataStore
//                    .getSchemaById(schemaId)
//                    .getTableById(indSubspaceKey.getDependentTableId())
//                    .getColumns();
//            for (Column column : columns) {
//                IND trivialInd = new IND(new int[]{column.getId()}, new int[]{column.getId()});
//                subspace.positiveBorder.add(trivialInd);
//            }
        }
    }

    @Override
    public void generateOptimisticCandidates(IndSubspaceKey indSubspaceKey, ZigZagSubspace subspace,
                                             NaryIndRestrictions naryIndRestrictions,
                                             int minArity,
                                             Collection<IND> collector) {

        Set<IND> candidates = new HashSet<>();
        TransversalCalculator transversalCalculator = new TransversalCalculator(subspace.strippedNegativeBorder, subspace.unaryInds);
        for (NaryInd transversalComplement : transversalCalculator.complementAll(transversalCalculator.calculateTransversals())) {

            IND positiveOptimisticBorderInd = transversalComplement.toInclusionDependency();
            if (positiveOptimisticBorderInd.getArity() >= minArity && !subspace.positiveBorder.contains(positiveOptimisticBorderInd)) {
                candidates.add(positiveOptimisticBorderInd);
            }
        }

        collector.addAll(candidates);
        LOGGER.info("Created {} optimistic candidates for {}.", candidates.size(), indSubspaceKey);
    }
}
