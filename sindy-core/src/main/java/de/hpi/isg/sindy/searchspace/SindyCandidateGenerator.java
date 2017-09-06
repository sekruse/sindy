package de.hpi.isg.sindy.searchspace;


import de.hpi.isg.sindy.util.IND;

import java.util.Collection;
import java.util.function.Predicate;

/**
 * Generates {@link IND} for {@link de.hpi.isg.sindy.core.Sindy}.
 */
public interface SindyCandidateGenerator {

    /**
     * Generates {@link IND} candidates.
     *
     * @param newInds                                  the most recently discovered {@link IND}s
     * @param consolidatedInds                         all discovered, consolidated {@link IND}s (see {@link #consolidate(Collection, Collection)})
     * @param nextArity                                the arity of the {@link IND} candidates to generate
     * @param naryIndRestrictions                      restrictions on admitted {@link IND} candidates
     * @param isExcludeVoidIndsFromCandidateGeneration whether {@link IND} candidates with empty columns should be neglected
     * @param columnBitMask                            which bits in column IDs represent the column index
     * @param emptyColumnCombinationTest               tests column combinations if they are empty
     * @param collector                                collects the generated {@link IND} candidates
     */
    void generate(Collection<IND> newInds,
                  Collection<IND> consolidatedInds,
                  int nextArity,
                  NaryIndRestrictions naryIndRestrictions,
                  boolean isExcludeVoidIndsFromCandidateGeneration,
                  Predicate<int[]> emptyColumnCombinationTest,
                  int columnBitMask,
                  Collection<IND> collector);

    /**
     * Removes {@link IND}s from the {@code existingInds} that are implied by the {@code newInds}.
     *
     * @param existingInds {@link IND}s that were used to generate {@link IND} candidates
     * @param newInds      candidates that turned out to be actual {@link IND}s
     */
    void consolidate(Collection<IND> existingInds, Collection<IND> newInds);

}