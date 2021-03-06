package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.sindy.util.IND;
import de.hpi.isg.sindy.util.INDs;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;

/**
 * Generates {@link IND} candidates in an Apriori manner (as described for the MIND algorithm).
 * In addition,
 */
public class AprioriCandidateGenerator implements CandidateGenerator, SindyCandidateGenerator {

    /**
     * Logger.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void generate(Collection<IND> newInds,
                         Collection<IND> consolidatedInds,
                         int nextArity,
                         NaryIndRestrictions naryIndRestrictions,
                         boolean isExcludeVoidIndsFromCandidateGeneration,
                         Predicate<int[]> emptyColumnTest,
                         int columnBitMask,
                         Collection<IND> collector) {

        Map<IndSubspaceKey, SortedSet<IND>> groupedInds = INDs.groupIntoSubspaces(newInds, columnBitMask);
        for (Map.Entry<IndSubspaceKey, SortedSet<IND>> entry : groupedInds.entrySet()) {
            int oldIndCandidatesSize = collector.size();
            this.generate(
                    entry.getValue(), entry.getKey(),
                    naryIndRestrictions,
                    isExcludeVoidIndsFromCandidateGeneration ?
                            ind -> !emptyColumnTest.test(ind.getDependentColumns()) :
                            null,
                    nextArity,
                    collector
            );
            this.logger.debug("Generated {} candidates for {}.", collector.size() - oldIndCandidatesSize, entry.getKey());
        }

    }

    @Override
    public void generate(Collection<IND> inds,
                         IndSubspaceKey indSubspaceKey,
                         NaryIndRestrictions naryIndRestrictions,
                         Predicate<IND> inputPredicate,
                         int nextArity,
                         Collection<IND> candidates) {

        // Put all INDs into a prefix map.
        Map<IntList, List<IND>> prefixMap = new HashMap<>();
        PrefixGrouping:
        for (IND ind : inds) {
            // Test whether the IND may be used for candidate generation.
            if (inputPredicate != null && !inputPredicate.test(ind)) continue;

            if (ind.getArity() == nextArity - 1) {
                IntList prefix = this.extractAprioriPrefix(ind);
                List<IND> prefixGroup = prefixMap.computeIfAbsent(prefix, k -> new ArrayList<>());
                prefixGroup.add(ind);
            }
        }

        // Process each prefix group independently.
        for (Map.Entry<IntList, List<IND>> entry : prefixMap.entrySet()) {
            IntList prefix = entry.getKey();
            List<IND> prefixGroup = entry.getValue();

            if (prefixGroup.size() < 2) continue;

            // Create IND candidates within each group.
            prefixGroup.sort(INDs.COMPARATOR);
            for (int i = 0; i < prefixGroup.size() - 1; i++) {
                IND ind1 = prefixGroup.get(i);
                int newDep1 = ind1.getDependentColumns()[ind1.getArity() - 1];
                int newRef1 = ind1.getReferencedColumns()[ind1.getArity() - 1];

                for (int j = i + 1; j < prefixGroup.size(); j++) {
                    IND ind2 = prefixGroup.get(j);
                    int newDep2 = ind2.getDependentColumns()[ind2.getArity() - 1];
                    int newRef2 = ind2.getReferencedColumns()[ind2.getArity() - 1];

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
                        System.arraycopy(ind1.getDependentColumns(), 0, newDep, 0, prefixPerSideSize);
                        System.arraycopy(ind1.getReferencedColumns(), 0, newRef, 0, prefixPerSideSize);
                    }
                    newDep[prefixPerSideSize] = newDep1;
                    newDep[prefixPerSideSize + 1] = newDep2;
                    newRef[prefixPerSideSize] = newRef1;
                    newRef[prefixPerSideSize + 1] = newRef2;
                    IND candidate = new IND(newDep, newRef);

                    // Check that all generating INDs really exist.
                    boolean areAllGeneratingIndsValid = true;
                    if (prefixPerSideSize > 0) {
                        List<IND> generatingInds = new LinkedList<>();
                        INDs.generateImpliedInds(candidate, prefixPerSideSize + 1, generatingInds);
                        Iterator<IND> iterator = generatingInds.iterator();
                        iterator.next();
                        iterator.next(); // Skip ind1 and ind2.
                        boolean isAllowTrivialInds = naryIndRestrictions.isAllowTrivialInds();
                        while (iterator.hasNext()) {
                            IND generatingInd = iterator.next();
                            // It might be when we allow to embed trivial INDs (e.g., R[ABB] < R[ABC])
                            if (!(isAllowTrivialInds && generatingInd.isTrivial()) && !inds.contains(generatingInd)) {
                                areAllGeneratingIndsValid = false;
                                break;
                            }
                            // Make sure that the input predicate holds for all generating INDs.
                            if (inputPredicate != null && !inputPredicate.test(generatingInd)) {
                                areAllGeneratingIndsValid = false;
                                break;
                            }
                        }
                    }
                    if (areAllGeneratingIndsValid) {
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
    protected IntList extractAprioriPrefix(IND ind) {
        int prefixSize = (ind.getArity() - 1) * 2;
        IntArrayList prefix = new IntArrayList(prefixSize);
        for (int i = 0; i < prefixSize / 2; i++) {
            prefix.add(ind.getDependentColumns()[i]);
            prefix.add(ind.getReferencedColumns()[i]);
        }
        return prefix;
    }

    @Override
    public void consolidate(Collection<IND> existingInds, Collection<IND> newInds) {
        // We only need to remove "direct sub-INDs" for the new INDs.
        final Set<IND> generatorInds = new HashSet<>();
        for (IND newInd : newInds) {
            INDs.generateImpliedInds(newInd, newInd.getArity() - 1, generatorInds);
        }
        existingInds.removeAll(generatorInds);
    }
}
