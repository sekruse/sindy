package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.util.CollectionUtils;
import de.hpi.isg.sindy.util.InclusionDependencies;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.*;

/**
 * Generates {@link InclusionDependency} candidates in an Apriori manner (as described for the MIND algorithm).
 */
public class AprioriCandidateGenerator implements CandidateGenerator {

    @Override
    public void generate(Collection<InclusionDependency> inds,
                                                    IndSubspaceKey indSubspaceKey,
                                                    NaryIndRestrictions naryIndRestrictions,
                                                    int maxArity,
                                                    Collection<InclusionDependency> candidates) {

        // Put all INDs into a prefix map.
        Map<IntList, List<InclusionDependency>> prefixMap = new HashMap<>();
        for (InclusionDependency ind : inds) {
            if (ind.getArity() < maxArity) {
                CollectionUtils.putIntoList(prefixMap, this.extractAprioriPrefix(ind), ind);
            }
        }

        // Process each prefix group independently.
        for (Map.Entry<IntList, List<InclusionDependency>> entry : prefixMap.entrySet()) {
            IntList prefix = entry.getKey();
            List<InclusionDependency> prefixGroup = entry.getValue();
            Collections.sort(prefixGroup, InclusionDependencies.COMPARATOR);

            if (prefixGroup.size() < 2) continue;

            // Create IND candidates within each group.
            Collections.sort(prefixGroup, InclusionDependencies.COMPARATOR);
            for (int i = 0; i < prefixGroup.size() - 1; i++) {
                InclusionDependency ind1 = prefixGroup.get(i);
                int newDep1 = ind1.getTargetReference().getDependentColumns()[ind1.getArity() - 1];
                int newRef1 = ind1.getTargetReference().getReferencedColumns()[ind1.getArity() - 1];

                for (int j = i + 1; j < prefixGroup.size(); j++) {
                    InclusionDependency ind2 = prefixGroup.get(j);
                    int newDep2 = ind2.getTargetReference().getDependentColumns()[ind2.getArity() - 1];
                    int newRef2 = ind2.getTargetReference().getReferencedColumns()[ind2.getArity() - 1];

                    // Check for repetitions within the new dep. and ref. side individually.
                    // Assuming valid given INDs, it suffices to check that the last attribute is different.
                    boolean isDependentSideFine = naryIndRestrictions.isAllowIntraRepetitions() ||
                            newDep1 < newDep2;
                    boolean isReferencedSideFine = naryIndRestrictions.isAllowIntraRepetitions() ||
                            newRef1 != newRef2;
                    if (!isDependentSideFine || !isReferencedSideFine) continue;

                    // Check for repetition within the dep. and ref. side together.
                    // R[X|AB] < R[Y|CD]: X<>YCD, A<>YC, B<>YD, test A<>D, B<>C
                    boolean areBothSidesFine = naryIndRestrictions.isAllowInterRepetitions() ||
                            indSubspaceKey.getDependentTableId() != indSubspaceKey.getReferencedTableId() ||
                            (newDep1 != newRef2 && newDep2 != newRef1);
                    if (!areBothSidesFine) continue;

                    // Create the IND candidate.
                    int prefixPerSideSize = prefix.size() / 2;
                    int[] newDep = new int[prefixPerSideSize + 2];
                    int[] newRef = new int[prefixPerSideSize + 2];
                    if (prefixPerSideSize > 0) {
                        System.arraycopy(ind1.getTargetReference().getDependentColumns(), 0, newDep, 0, prefixPerSideSize);
                        System.arraycopy(ind1.getTargetReference().getReferencedColumns(), 0, newRef, 0, prefixPerSideSize);
                    }
                    newDep[prefixPerSideSize] = newDep1;
                    newDep[prefixPerSideSize + 1] = newDep2;
                    newRef[prefixPerSideSize] = newRef1;
                    newRef[prefixPerSideSize + 1] = newRef2;
                    InclusionDependency candidate = new InclusionDependency(new InclusionDependency.Reference(newDep, newRef));

                    // Check that all generating INDs really exist.
                    boolean areAllGeneratingIndsExisting = true;
                    if (prefixPerSideSize > 0) {
                        List<InclusionDependency> generatingInds = new LinkedList<>();
                        InclusionDependencies.generateImpliedInds(candidate, prefixPerSideSize + 1, generatingInds);
                        Iterator<InclusionDependency> iterator = generatingInds.iterator();
                        iterator.next();
                        iterator.next(); // Skip ind1 and ind2.
                        boolean isAllowTrivialInds = naryIndRestrictions.isAllowTrivialInds();
                        while (iterator.hasNext()) {
                            InclusionDependency generatingInd = iterator.next();
                            // It might be when we allow to embed trivial INDs (e.g., R[ABB] < R[ABC])
                            if (!(isAllowTrivialInds && generatingInd.isTrivial()) && !inds.contains(generatingInd)) {
                                areAllGeneratingIndsExisting = false;
                                break;
                            }
                        }
                    }
                    if (areAllGeneratingIndsExisting) {
                        candidates.add(candidate);
                    }
                }
            }
        }
    }

    /**
     * Extracts the prefix of an IND. For instance, the prefix of ABC < DEF is ADBE
     *
     * @param ind is the IND from that the prefix should be extracted
     * @return the prefix for the IND
     */
    protected IntList extractAprioriPrefix(InclusionDependency ind) {
        int prefixSize = (ind.getArity() - 1) * 2;
        IntArrayList prefix = new IntArrayList(prefixSize);
        for (int i = 0; i < prefixSize / 2; i++) {
            prefix.add(ind.getTargetReference().getDependentColumns()[i]);
            prefix.add(ind.getTargetReference().getReferencedColumns()[i]);
        }
        return prefix;
    }

    @Override
    public void consolidate(Collection<InclusionDependency> existingInds, Collection<InclusionDependency> newInds) {
        // We only need to remove "direct sub-INDs" for the new INDs.
        final Set<InclusionDependency> generatorInds = new HashSet<>();
        for (InclusionDependency newInd : newInds) {
            InclusionDependencies.generateImpliedInds(newInd, newInd.getArity() - 1, generatorInds);
        }
        existingInds.removeAll(generatorInds);
    }
}
