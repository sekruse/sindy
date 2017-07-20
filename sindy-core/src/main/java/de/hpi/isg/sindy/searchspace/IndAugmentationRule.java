package de.hpi.isg.sindy.searchspace;


import de.hpi.isg.sindy.util.IND;

/**
 * An IND augmentation rule reflects the fact that an n-ary inclusion dependency contains an FD
 * among its referenced attributes. This FD must then be found in the dependent attributes as well.
 * This circumstance allows us to define a syntactic rule expressing that INDs can be "augmented" to INDs
 * of higher arity.
 */
public class IndAugmentationRule {

    private final IND lhs;

    private final IND rhs;

    public IndAugmentationRule(IND lhs, IND rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public IND getLhs() {
        return this.lhs;
    }

    public IND getRhs() {
        return this.rhs;
    }

    @Override
    public String toString() {
        return String.format("\u3008%s\u3009 \u21C9 \u3008%s\u3009", this.lhs, this.rhs);
    }
}

