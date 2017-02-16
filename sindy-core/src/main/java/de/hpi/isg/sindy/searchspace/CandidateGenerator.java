package de.hpi.isg.sindy.searchspace;


import de.hpi.isg.sindy.util.IND;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Generates {@link IND} candidates based on given {@link IND}s by some strategy.
 */
public interface CandidateGenerator {


    /**
     * Generates {@link IND} candidates.
     *
     * @param inds                is a collection of valid INDs of a single IND subspace
     * @param indSubspaceKey      describes this IND subspace
     * @param naryIndRestrictions that should be applied to the candidate generation
     * @return all generated IND candidates
     */
    default Collection<IND> generate(Collection<IND> inds,
                                     IndSubspaceKey indSubspaceKey,
                                     NaryIndRestrictions naryIndRestrictions,
                                     int maxArity) {
        Collection<IND> collector = new LinkedList<>();
        this.generate(inds, indSubspaceKey, naryIndRestrictions, maxArity, collector);
        return collector;
    }

    /**
     * Generates {@link IND} candidates.
     *
     * @param inds                is a collection of valid INDs of a single IND subspace
     * @param indSubspaceKey      describes this IND subspace
     * @param naryIndRestrictions that should be applied to the candidate generation
     * @@param collector collects all generated IND candidates
     */
    void generate(Collection<IND> inds,
                  IndSubspaceKey indSubspaceKey,
                  NaryIndRestrictions naryIndRestrictions,
                  int maxArity,
                  Collection<IND> collector);

    /**
     * Removes {@link IND}s from the {@code existingInds} that are implied by the {@code newInds}.
     *
     * @param existingInds {@link IND}s that were used to generate {@link IND} candidates
     * @param newInds      candidates that turned out to be actual {@link IND}s
     */
    void consolidate(Collection<IND> existingInds, Collection<IND> newInds);

}