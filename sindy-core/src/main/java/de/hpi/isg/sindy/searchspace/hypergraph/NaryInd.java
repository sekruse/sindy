package de.hpi.isg.sindy.searchspace.hypergraph;

import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.util.IND;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.*;

/**
 * This class represents n-ary INDs as set of {@link UnaryInd}s.
 */
public class NaryInd {

    public static final Comparator<NaryInd> ARITY_COMPARATOR =
            Comparator.comparingInt(ind -> ind.getUnaryInds().size());

    private final Set<UnaryInd> unaryInds;

    /**
     * Converts a regular {@link IND} into this set-based representation.
     *
     * @param ind is the original IND to be imitated
     */
    public NaryInd(IND ind) {
        this.unaryInds = new HashSet<>(ind.getArity());
        for (int i = 0; i < ind.getArity(); i++) {
            this.unaryInds.add(new UnaryInd(ind.getDependentColumns()[i],
                    ind.getReferencedColumns()[i]));
        }
    }

    /**
     * Creates a n-ary from the given unary INDs.
     *
     * @param unaryInds are the unary INDs that form up the n-ary IND.
     */
    public NaryInd(Set<UnaryInd> unaryInds) {
        this.unaryInds = unaryInds;
    }

    public NaryInd(NaryInd naryInd, UnaryInd unaryInd) {
        this(new HashSet<>(naryInd.getUnaryInds()));
        this.unaryInds.add(unaryInd);
    }

    public NaryInd(NaryInd ind) {
        this(new HashSet<>(ind.getUnaryInds()));
    }

    public Set<UnaryInd> getUnaryInds() {
        return unaryInds;
    }

    /**
     * Checks if the to INDs have no unary IND in common.
     *
     * @param that is the other IND
     * @return whether the two INDs are disjoint
     */
    public boolean isDisjointWith(NaryInd that) {
        for (UnaryInd unaryInd : this.unaryInds) {
            if (that.unaryInds.contains(unaryInd)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether this IND is specializing another IND, i.e., it is embedding a subset of unary INDs
     * in comparison with the other IND.
     *
     * @param that is the allegedly specialized IND
     * @return whether this IND is specializing the other IND
     */
    public boolean isSpecializing(NaryInd that) {
        return that.unaryInds.size() < this.unaryInds.size() && this.unaryInds.containsAll(that.unaryInds);
    }

    /**
     * Tests whether this IND satisfies the given IND restrictions.
     *
     * @param restrictions restrictions to satisfy
     * @return whether the IND satisfies the restrictions
     */
    public boolean satisfies(NaryIndRestrictions restrictions) {
        // Look for intra-repetitions.
        if (!restrictions.isAllowIntraRepetitions()) {
            IntSet depSet = new IntOpenHashSet(this.unaryInds.size());
            IntSet refSet = new IntOpenHashSet(this.unaryInds.size());
            for (UnaryInd unaryInd : this.unaryInds) {
                if (!depSet.add(unaryInd.depId)) {
                    return false;
                }
                if (!refSet.add(unaryInd.refId)) {
                    return false;
                }
            }
        }

        // Look for inter-repetitions.
        if (!restrictions.isAllowInterRepetitions()) {
            IntSet depSet = new IntOpenHashSet(this.unaryInds.size());
            IntSet refSet = new IntOpenHashSet(this.unaryInds.size());
            for (UnaryInd unaryInd : this.unaryInds) {
                depSet.add(unaryInd.depId);
                refSet.add(unaryInd.refId);
            }
            depSet.retainAll(refSet);
            if (!depSet.isEmpty()) {
                return false;
            }
        }

        // Look for trivial INDs.
        else if (!restrictions.isAllowTrivialInds()) {
            for (UnaryInd unaryInd : this.unaryInds) {
                if (unaryInd.depId == unaryInd.refId) {
                    return false;
                }
            }
        }

        // If all tests passed, the IND satisfies the restrictions.
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NaryInd naryInd = (NaryInd) o;

        return this.unaryInds.equals(naryInd.unaryInds);

    }

    @Override
    public int hashCode() {
        return this.unaryInds.hashCode();
    }

    @Override
    public String toString() {
        return this.unaryInds.toString();
    }

    /**
     * Converts this IND to a {@link IND}.
     *
     * @return an {@link IND} logically equal to this IND
     */
    public IND toInclusionDependency() {
        List<UnaryInd> unaryInds = new ArrayList<>(this.getUnaryInds());
        Collections.sort(unaryInds);
        int[] dep = new int[unaryInds.size()];
        int[] ref = new int[unaryInds.size()];
        int i = 0;
        for (UnaryInd unaryInd : unaryInds) {
            dep[i] = unaryInd.depId;
            ref[i] = unaryInd.refId;
            i++;
        }
        return new IND(dep, ref);
    }

    /**
     * Checks whether this IND has no repeating attributes within the dependent and referenced side.
     *
     * @param isCheckSidesIndividually is {@code true} if repeating attributes should only be found within the
     *                                 referenced and dependent side individually
     * @return whether the IND is free of repetitions
     */
    public boolean isNonRepeating(boolean isCheckSidesIndividually) {
        IntSet usedAttributes = new IntOpenHashSet(this.unaryInds.size() * 2);
        for (UnaryInd unaryInd : this.unaryInds) {
            if (!usedAttributes.add(unaryInd.depId)) {
                return false;
            }
        }
        if (isCheckSidesIndividually) {
            usedAttributes.clear();
        }
        for (UnaryInd unaryInd : this.unaryInds) {
            if (!usedAttributes.add(unaryInd.refId)) {
                return false;
            }
        }
        return true;
    }
}
