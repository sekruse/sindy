package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Generates {@link InclusionDependency} candidates based on given {@link InclusionDependency}s by some strategy.
 */
public interface CandidateGenerator {


    /**
     * Generates {@link InclusionDependency} candidates.
     *
     * @param inds                is a collection of valid INDs of a single IND subspace
     * @param indSubspaceKey      describes this IND subspace
     * @param naryIndRestrictions that should be applied to the candidate generation
     * @return all generated IND candidates
     */
    default Collection<InclusionDependency> generate(Collection<InclusionDependency> inds,
                                             IndSubspaceKey indSubspaceKey,
                                             NaryIndRestrictions naryIndRestrictions,
                                             int maxArity) {
        Collection<InclusionDependency> collector = new LinkedList<>();
        this.generate(inds, indSubspaceKey, naryIndRestrictions, maxArity, collector);
        return collector;
    }

    /**
     * Generates {@link InclusionDependency} candidates.
     *
     * @param inds                is a collection of valid INDs of a single IND subspace
     * @param indSubspaceKey      describes this IND subspace
     * @param naryIndRestrictions that should be applied to the candidate generation
     * @@param collector collects all generated IND candidates
     */
    void generate(Collection<InclusionDependency> inds,
                                             IndSubspaceKey indSubspaceKey,
                                             NaryIndRestrictions naryIndRestrictions,
                                             int maxArity,
                                             Collection<InclusionDependency> collector);

    /**
     * Removes {@link InclusionDependency}s from the {@code existingInds} that are implied by the {@code newInds}.
     *
     * @param existingInds {@link InclusionDependency}s that were used to generate {@link InclusionDependency} candidates
     * @param newInds      candidates that turned out to be actual {@link InclusionDependency}s
     */
    void consolidate(Collection<InclusionDependency> existingInds, Collection<InclusionDependency> newInds);

}