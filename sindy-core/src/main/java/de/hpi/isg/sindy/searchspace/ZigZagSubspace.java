package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.sindy.util.IND;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Describes INDs and NINDs between two tables. It contains:
 * <ul>
 * <li>positive border (non-redundant IND set w.r.t. projection and permutation)</li>
 * <li>stripped negative border (non-redundant non-IND set w.r.t. projection and permuation and w/o unary non-INDs)</li>
 * <li>unary INDs</li>
 * </ul>
 *
 * @author sebastian.kruse
 * @since 06.08.2015
 */
public class ZigZagSubspace {

    /**
     * Descriptor for this subspace.
     */
    public final IndSubspaceKey key;

    /**
     * Contains all unary INDs.
     */
    public final Set<IND> unaryInds = new HashSet<>();

    /**
     * Contains all INDs from the positive border.
     */
    public final Set<IND> positiveBorder = new HashSet<>();

    /**
     * Contains all non-INDs from the negative border except for unary non-INDs.
     */
    public Set<IND> strippedNegativeBorder = new HashSet<>();

    public ZigZagSubspace(IndSubspaceKey key) {
        this.key = key;
    }

    /**
     * Collects all INDs in the {@link #positiveBorder} of the given arity.
     * @param arity the arity of the INDs to collect
     * @return the matching INDs
     */
    public Collection<IND> getPositiveBorderInds(int arity) {
        Collection<IND> inds = new LinkedList<>();
        for (IND inclusionDependency : positiveBorder) {
            if (inclusionDependency.getArity() == arity) {
                inds.add(inclusionDependency);
            }
        }
        return inds;
    }

}
