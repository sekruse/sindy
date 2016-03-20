package de.hpi.isg.sindy.searchspace;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.model.util.BasicPrettyPrinter;
import de.hpi.isg.mdms.model.util.IdUtils;

/**
 * This class identifies a subspace of the n-ary IND search space. Two subspaces are completely independent of each other.
 * The basic observation is that a combination of two tables, a dependent and a referenced one, form their own IND
 * search space.
 */
public class IndSubspaceKey {

  /**
   * The ID of the dependent table of the subspace.
   */
  private final int dependentTableId;

  /**
   * The ID of the referenced table of the subspace.
   */
  private final int referencedTableId;

  /**
   * Creates a key for the tables that contain the given columns.
   *
   * @param dependentColumnId  is the ID of the dependent column
   * @param referencedColumnId is the ID of the referenced column
   * @param idUtils            that manage the ID space in which the column and table ID reside
   * @return the {@link IndSubspaceKey}
   */
  public static IndSubspaceKey createFromColumns(int dependentColumnId, int referencedColumnId, IdUtils idUtils) {
    int dependentTableId = idUtils.getTableId(dependentColumnId);
    int referencedTableId = idUtils.getTableId(referencedColumnId);
    return new IndSubspaceKey(dependentTableId, referencedTableId);
  }

  /**
   * Creates a key for the tables that contain the given columns.
   *
   * @param ind is an inclusion dependency for that the ID space is sought
   * @param idUtils            that manage the ID space in which the column and table ID reside
   * @return the {@link IndSubspaceKey}
   */
  public static IndSubspaceKey createFromInd(InclusionDependency ind, IdUtils idUtils) {
    return createFromColumns(ind.getTargetReference().getDependentColumns()[0],
            ind.getTargetReference().getReferencedColumns()[0],
            idUtils);
  }

  /**
   * Creates a new instance for the given tables.
   * @param dependentTableId is the ID of the dependent table
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

  public String toString(BasicPrettyPrinter prettyPrinter) {
    return "IndSubspace[" + prettyPrinter.prettyPrint(this.dependentTableId) + " < " + prettyPrinter.prettyPrint(this.referencedTableId)+ ']';
  }
}
