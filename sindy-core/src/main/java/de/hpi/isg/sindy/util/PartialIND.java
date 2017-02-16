package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Arrays;
import java.util.Objects;

/**
 * Simple implementation of an inclusion dependency via column IDs.
 */
public class PartialIND extends IND {

    /**
     * Expresses by some metric the overlap of the dependent with the referenced columns (w.r.t. the dependent columns)
     */
    private final double overlap;

    /**
     * Expresses some metric of the size of the dependent columns, e.g., the number of distinct values.
     */
    private final int dependentSize;

    public PartialIND(int[] dependentColumns, int[] referencedColumns, int dependentSize, double overlap) {
        super(dependentColumns, referencedColumns);
        this.dependentSize = dependentSize;
        this.overlap = overlap;
    }

    /**
     * Creates a new unary partial IND.
     *
     * @param dependentId  the dependent column ID
     * @param referencedId the referenced column ID
     * @param overlap      the overlap of the dependent with the referenced column
     */
    public PartialIND(int dependentId, int referencedId, int dependentSize, double overlap) {
        super(dependentId, referencedId);
        this.dependentSize = dependentSize;
        this.overlap = overlap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        final PartialIND ind = (PartialIND) o;
        return this.dependentSize == ind.dependentSize &&
                this.overlap == ind.overlap &&
                Arrays.equals(this.dependentColumns, ind.dependentColumns) &&
                Arrays.equals(this.referencedColumns, ind.referencedColumns);
    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + Arrays.hashCode(this.dependentColumns);
        result = 31 * result + Arrays.hashCode(this.referencedColumns);
        result = 31 * result + Objects.hash(this.dependentSize, this.overlap);
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s \u2286 %s (overlap=%,.2f)",
                Arrays.toString(this.dependentColumns), Arrays.toString(this.referencedColumns), this.overlap
        );
    }

    /**
     * Nicer version of {@link #toString()} that resolves table names and column indices.
     *
     * @param tables        the indexed tables
     * @param numColumnBits the number of bits used to encode columns
     * @return a {@link String} representation of this instance
     */
    public String toString(Int2ObjectMap<String> tables, int numColumnBits) {
        if (this.getArity() == 0) return "[] \u2286 []";
        int columnBitMask = -1 >>> (Integer.SIZE - numColumnBits);

        int depMinColumnId = this.dependentColumns[0] & ~columnBitMask;
        int depTableId = depMinColumnId | columnBitMask;
        int refMinColumnId = this.referencedColumns[0] & ~columnBitMask;
        int refTableId = refMinColumnId | columnBitMask;

        StringBuilder sb = new StringBuilder();
        sb.append(tables.get(depTableId)).append('[');
        String separator = "";
        for (int dependentColumn : this.dependentColumns) {
            sb.append(separator).append(dependentColumn - depMinColumnId);
            separator = ", ";
        }
        sb.append("] \u2286 ").append(tables.get(refTableId)).append("[");
        separator = "";
        for (int referencedColumn : this.referencedColumns) {
            sb.append(separator).append(referencedColumn - refMinColumnId);
            separator = ", ";
        }
        sb.append("] (overlap=").append(this.overlap).append(")");
        return sb.toString();
    }

}
