package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.sindy.util.IND;

import java.util.*;
import java.util.function.Predicate;

/**
 * Generates {@link IND} candidates as described in the BINDER paper.
 */
public class BinderCandidateGenerator implements SindyCandidateGenerator {

    @Override
    public void generate(Collection<IND> newInds,
                         Collection<IND> consolidatedInds,
                         int nextArity,
                         NaryIndRestrictions naryIndRestrictions,
                         boolean isExcludeVoidIndsFromCandidateGeneration,
                         Predicate<int[]> emptyColumnTest,
                         int columnBitMask,
                         Collection<IND> collector) {
        // Group the INDs into subspaces. We need the unary INDs and the most recent INDs.
        Map<IndSubspaceKey, List<IND>> unaryIndGroups = new HashMap<>();
        Map<IndSubspaceKey, List<IND>> naryIndGroups = new HashMap<>();
        for (IND ind : consolidatedInds) {
            if (ind.getArity() == 1) {
                unaryIndGroups.computeIfAbsent(IndSubspaceKey.createFromInd(ind, columnBitMask), k -> new ArrayList<>()).add(ind);
            } else if (ind.getArity() == nextArity - 1) {
                naryIndGroups.computeIfAbsent(IndSubspaceKey.createFromInd(ind, columnBitMask), k -> new ArrayList<>()).add(ind);
            }
        }
        if (nextArity == 2) naryIndGroups = unaryIndGroups;

        // Process the individual IND subspaces.
        for (Map.Entry<IndSubspaceKey, List<IND>> entry : naryIndGroups.entrySet()) {
            // Retrieve the unary and n-ary INDs and sort them (lexicographically).
            IndSubspaceKey subspaceKey = entry.getKey();
            List<IND> naryInds = entry.getValue();
            naryInds.sort(IND.standardComparator);
            List<IND> unaryInds = unaryIndGroups.get(subspaceKey);
            unaryInds.sort(IND.standardComparator);

            // Co-iterate the unary and n-ary INDs thereby matching them.
            int unaryIndIndex = 0;
            for (IND naryInd : naryInds) {
                // Check if we need to handle the IND in the first place.
                if (isExcludeVoidIndsFromCandidateGeneration && emptyColumnTest.test(naryInd.getDependentColumns())) {
                    continue;
                }

                // Determine the first unary IND is "greater than" the n-ary IND.
                while (unaryIndIndex < unaryInds.size() && IND.standardComparator.compare(unaryInds.get(unaryIndIndex), naryInd) <= 0) {
                    unaryIndIndex++;
                }
                if (unaryIndIndex >= unaryInds.size()) break;

                // Pair the n-ary IND with all following INDs to generate candidates.
                IndCombinations:
                for (int i = unaryIndIndex; i < unaryInds.size(); i++) {
                    IND unaryInd = unaryInds.get(i);
                    int newDep = unaryInd.getDependentColumns()[0];
                    int newRef = unaryInd.getReferencedColumns()[0];

                    // Make sure that we do not create an IND with an empty LHS.
                    if (isExcludeVoidIndsFromCandidateGeneration && emptyColumnTest.test(unaryInd.getDependentColumns())) {
                        continue IndCombinations;
                    }

                    if (!naryIndRestrictions.isAllowIntraRepetitions()) {
                        // Make sure that neither the dependent nor the referenced attributes overlap.
                        for (int j = 0; j < naryInd.getArity(); j++) {
                            if (naryInd.getDependentColumns()[j] == newDep || naryInd.getReferencedColumns()[j] == newRef) {
                                continue IndCombinations;
                            }
                        }
                    }
                    if (!naryIndRestrictions.isAllowInterRepetitions()) {
                        // Make sure that neither the dependent nor the referenced attributes will overlap.
                        for (int j = 0; j < naryInd.getArity(); j++) {
                            if (naryInd.getDependentColumns()[j] == newRef || naryInd.getReferencedColumns()[j] == newDep) {
                                continue IndCombinations;
                            }
                        }
                    }

                    // Generate and collect the IND candidate.
                    int[] candidateDep = Arrays.copyOf(naryInd.getDependentColumns(), nextArity);
                    candidateDep[nextArity - 1] = newDep;
                    int[] candidateRef = Arrays.copyOf(naryInd.getReferencedColumns(), nextArity);
                    candidateRef[nextArity - 1] = newRef;
                    collector.add(new IND(candidateDep, candidateRef));
                }
            }
        }
    }

    @Override
    public void consolidate(Collection<IND> existingInds, Collection<IND> newInds) {
        // We don't consolidate at all.
    }
}
