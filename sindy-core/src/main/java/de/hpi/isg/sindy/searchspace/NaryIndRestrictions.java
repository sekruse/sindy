package de.hpi.isg.sindy.searchspace;

/**
 * Possible restrictions to the n-ary IND search space. The following is possible:
 * <ol>
 * <li>allow/deny repetitions of attributes among the dependent and referenced side of INDs</li>
 * <li>allow/deny to embed trivial INDs in n-ary IND candidates (requires 1.)</li>
 * <li>allow/deny repetition within the dependent and referenced side</li>
 * </ol>
 */
public enum NaryIndRestrictions {
    NO_REPETITIONS(false, false, false), // 0
    NO_INTRA_REPETITIONS_NO_TRIVIAL(true, false, false), // 1
    NO_INTRA_REPETITIONS(true, false, true), // 2
    NO_TRIVIAL(true, true, false), // 3
    NO_INTER_REPETITIONS(false, true, false), // 4
    ANY_REPETITION(true, true, true); // 5

    public static String overviewString() {
        StringBuilder sb = new StringBuilder();
        String separator = "";
        for (NaryIndRestrictions naryIndRestriction : NaryIndRestrictions.values()) {
            sb.append(separator).append(naryIndRestriction.ordinal()).append("=").append(naryIndRestriction.name());
            separator = ", ";
        }
        return sb.toString();
    }

    private final boolean isAllowInterRepetitions;
    private final boolean isAllowIntraRepetitions;
    private final boolean isAllowTrivialInds;

    NaryIndRestrictions(boolean isAllowInterRepetitions, boolean isAllowIntraRepetitions, boolean isAllowTrivialInds) {
        this.isAllowInterRepetitions = isAllowInterRepetitions;
        this.isAllowIntraRepetitions = isAllowIntraRepetitions;
        this.isAllowTrivialInds = isAllowTrivialInds;
    }

    public boolean isAllowInterRepetitions() {
        return isAllowInterRepetitions;
    }

    public boolean isAllowIntraRepetitions() {
        return isAllowIntraRepetitions;
    }

    public boolean isAllowTrivialInds() {
        return isAllowTrivialInds;
    }
}
