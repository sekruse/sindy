package de.hpi.isg.sindy.searchspace.hypergraph;

import de.hpi.isg.sindy.util.IND;

import java.util.HashSet;
import java.util.Set;

/**
 * This class describes a unary IND between two columns represented via {@code int} IDs.
 */
public class UnaryInd implements Comparable<UnaryInd> {

    public final int depId;
    public final int refId;

    /**
     * Creates a new unary IND.
     *
     * @param depId is the ID of the dependent column
     * @param refId is the ID of the referenced column
     */
    public UnaryInd(int depId, int refId) {
        this.depId = depId;
        this.refId = refId;
    }

    /**
     * Creates a new instance from an {@link IND}.
     *
     * @param ind is a unary {@link IND}
     */
    public UnaryInd(IND ind) {
        if (ind.getArity() != 1) {
            throw new IllegalArgumentException("Allegedly unary IND has an arity of " + ind.getArity());
        }
        this.depId = ind.getDependentColumns()[0];
        this.refId = ind.getReferencedColumns()[0];
    }

    /**
     * Converts this unary IND.
     *
     * @return a {@link NaryInd} that is logically equal to this unary IND
     */
    public NaryInd toNaryInd() {
        Set<UnaryInd> singletonIndSet = new HashSet<>();
        singletonIndSet.add(this);
        return new NaryInd(singletonIndSet);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UnaryInd unaryInd = (UnaryInd) o;

        if (this.depId != unaryInd.depId) return false;
        return this.refId == unaryInd.refId;

    }

    @Override
    public int hashCode() {
        int result = this.depId;
        result = 31 * result + this.refId;
        return result;
    }

    @Override
    public int compareTo(UnaryInd that) {
        int result = Integer.compare(this.depId, that.depId);
        return result == 0 ? Integer.compare(this.refId, that.refId) : result;
    }

    @Override
    public String toString() {
        return String.format("%d \u2286 %d", this.depId, this.refId);
    }

}
