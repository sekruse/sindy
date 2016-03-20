package de.hpi.isg.sindy.util;

import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Collection;
import java.util.Comparator;

/**
 * Utilities for handling {@link InclusionDependency} objects.
 */
public class InclusionDependencies {

    private InclusionDependencies() {
    }

    /**
     * {@link Comparator} for {@link InclusionDependency}s. Orders by
     * <ol>
     * <li>arity,</li>
     * <li>dependent column IDs (lexicographically), and</li>
     * <li>referenced column IDs (lexicographically).</li>
     * </ol>
     */
    public static final Comparator<InclusionDependency> COMPARATOR =
            (ind1, ind2) -> {
                int result = ind1.getArity() - ind2.getArity();
                if (result != 0) return result;

                final int[] depCols1 = ind1.getTargetReference().getDependentColumns();
                final int[] depCols2 = ind2.getTargetReference().getDependentColumns();
                for (int i = 0; i < depCols1.length; i++) {
                    if ((result = depCols1[i] - depCols2[i]) != 0) return result;
                }

                final int[] refCols1 = ind1.getTargetReference().getReferencedColumns();
                final int[] refCols2 = ind2.getTargetReference().getReferencedColumns();
                for (int i = 0; i < refCols1.length; i++) {
                    if ((result = refCols1[i] - refCols2[i]) != 0) return result;
                }

                return 0;
            };

    /**
     * Generates all INDs that are implied by the given {@code ind} of the desired {@code arity} in lexicographical order.
     *
     * @param ind       whose implied INDs should be generated
     * @param arity     of the generated {@link InclusionDependency}s
     * @param collector collects the results
     */
    public static void generateImpliedInds(InclusionDependency ind, int arity, Collection<InclusionDependency> collector) {
        generateImpliedIndsAux(new IntArrayList(arity), ind, collector, arity);
    }

    /**
     * Generates all INDs that are implied by the given {@code ind} of the desired {@code arity} in lexicographical order
     * by recursion. Should be used via {@link #generateImpliedInds(InclusionDependency, int, Collection)}
     *
     * @param selectedUindPos are positions of unary INDs in {@code ind} that are already selected
     * @param ind             is the IND whose generalizing INDs should be collected
     * @param arity           is number unary INDs embedded in the collected INDs
     * @param collector       collects the results
     */
    private static void generateImpliedIndsAux(IntList selectedUindPos,
                                               InclusionDependency ind,
                                               Collection<InclusionDependency> collector,
                                               int arity) {
        if (arity > 0) {
            int minPos = selectedUindPos.isEmpty() ? 0 : selectedUindPos.getInt(selectedUindPos.size() - 1) + 1;
            int maxPos = ind.getArity() - arity;
            for (int pos = minPos; pos <= maxPos; pos++) {
                selectedUindPos.add(pos);
                generateImpliedIndsAux(selectedUindPos, ind, collector, arity - 1);
                selectedUindPos.remove(selectedUindPos.size() - 1);
            }
        } else {
            int[] dep = new int[selectedUindPos.size()];
            int[] ref = new int[selectedUindPos.size()];
            int index = 0;
            for (IntIterator iter = selectedUindPos.iterator(); iter.hasNext(); ) {
                int pos = iter.nextInt();
                dep[index] = ind.getTargetReference().getDependentColumns()[pos];
                ref[index] = ind.getTargetReference().getReferencedColumns()[pos];
                index++;
            }
            collector.add(new InclusionDependency(new InclusionDependency.Reference(dep, ref)));
        }
    }

}
