package de.hpi.isg.sindy.searchspace;


import de.hpi.isg.sindy.util.IND;

import java.util.Collection;

/**
 * This interface describes algorithms to calculate the most specific IND candidates for a given table space.:
 * <p>
 * <p>
 * Created by basti on 8/17/15.
 */
public interface OptimisticCandidateGenerator<TSubspace> {

    /**
     * Adapts the subspace for this generator to work, e.g., by adding INDs or NINDs.
     *
     * @param indSubspaceKey      describes the current subspace
     * @param subspace            is the subspace
     * @param naryIndRestrictions to be considered in the generation
     */
    void prepareSubspace(IndSubspaceKey indSubspaceKey, TSubspace subspace,
                         NaryIndRestrictions naryIndRestrictions);

    /**
     * Generates the optimistic candidates for the given subspace.
     *
     * @param indSubspaceKey      describes the current subspace
     * @param subspace            is the IND subspace for that the IND candidates should be generated
     * @param naryIndRestrictions to be considered in the generation
     * @param minArity            is the minimum arity of candidates to be created
     * @param collector           collects the IND candidates
     */
    void generateOptimisticCandidates(IndSubspaceKey indSubspaceKey, TSubspace subspace,
                                      NaryIndRestrictions naryIndRestrictions, int minArity,
                                      Collection<IND> collector);


}
