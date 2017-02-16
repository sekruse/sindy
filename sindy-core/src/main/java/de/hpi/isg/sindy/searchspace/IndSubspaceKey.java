package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.sindy.util.IND;

/**
 * This class identifies a subspace of the n-ary IND search space. Two subspaces are completely independent of each other.
 * The basic observation is that a combination of two tables, a dependent and a referenced one, form their own IND
 * search space.
 */
public class IndSubspaceKey {

    /**
     * The ID of the dependent table of the subspace.
     * <p>Note: table IDs are such IDs where all column bits are set to {@code 1}. E.g., assume a column ID {@code 00001 0001},
     * where the first four bits mark the table and the last four bits mark the table. The corresponding table ID is
     * {@code 0001 1111}.</p>
     */
    private final int dependentTableId;

    /**
     * The ID of the referenced table of the subspace.
     * <p>Note: table IDs are such IDs where all column bits are set to {@code 1}. E.g., assume a column ID {@code 00001 0001},
     * where the first four bits mark the table and the last four bits mark the table. The corresponding table ID is
     * {@code 0001 1111}.</p>
     */
    private final int referencedTableId;

    /**
     * Creates a key for the tables that contain the given columns.
     *
     * @param dependentColumnId  is the ID of the dependent column
     * @param referencedColumnId is the ID of the referenced column
     * @param columnBitMask      marks those bits in the column IDs that represent the column
     * @return the {@link IndSubspaceKey}
     */
    public static IndSubspaceKey createFromColumns(int dependentColumnId, int referencedColumnId, int columnBitMask) {
        int dependentTableId = dependentColumnId | columnBitMask;
        int referencedTableId = referencedColumnId | columnBitMask;
        return new IndSubspaceKey(dependentTableId, referencedTableId);
    }

    /**
     * Creates a key for the tables that contain the given columns.
     *
     * @param ind           is an inclusion dependency for that the ID space is sought
     * @param columnBitMask marks those bits in the column IDs that represent the column
     * @return the {@link IndSubspaceKey}
     */
    public static IndSubspaceKey createFromInd(IND ind, int columnBitMask) {
        return createFromColumns(ind.getDependentColumns()[0],
                ind.getReferencedColumns()[0],
                columnBitMask);
    }

    /**
     * Creates a new instance for the given tables.
     *
     * @param dependentTableId  is the ID of the dependent table
     * @param referencedTableId is the ID of the referenced table
     */
    public IndSubspaceKey(int dependentTableId, int referencedTableId) {
        this.dependentTableId = dependentTableId;
        this.referencedTableId = referencedTableId;
    }

    public int getReferencedTableId() {
        return referencedTableId;
    }

    public int getDependentTableId() {
        return dependentTableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndSubspaceKey that = (IndSubspaceKey) o;

        if (dependentTableId != that.dependentTableId) return false;
        return referencedTableId == that.referencedTableId;

    }

    @Override
    public int hashCode() {
        int result = dependentTableId;
        result = 31 * result + referencedTableId;
        return result;
    }

    @Override
    public String toString() {
        return "IndSubspace[" + dependentTableId + " < " + referencedTableId + ']';
    }

}
