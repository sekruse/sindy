package de.hpi.isg.sindy.util;

import de.hpi.isg.sindy.searchspace.IndSubspaceKey;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.*;

/**
 * Utilities for handling {@link IND} objects.
 */
public class INDs {

    private INDs() {
    }

    /**
     * {@link Comparator} for {@link IND}s. Orders by
     * <ol>
     * <li>arity,</li>
     * <li>dependent column IDs (lexicographically), and</li>
     * <li>referenced column IDs (lexicographically).</li>
     * </ol>
     */
    public static final Comparator<IND> COMPARATOR =
            (ind1, ind2) -> {
                int result = ind1.getArity() - ind2.getArity();
                if (result != 0) return result;

                final int[] depCols1 = ind1.getDependentColumns();
                final int[] depCols2 = ind2.getDependentColumns();
                for (int i = 0; i < depCols1.length; i++) {
                    if ((result = depCols1[i] - depCols2[i]) != 0) return result;
                }

                final int[] refCols1 = ind1.getReferencedColumns();
                final int[] refCols2 = ind2.getReferencedColumns();
                for (int i = 0; i < refCols1.length; i++) {
                    if ((result = refCols1[i] - refCols2[i]) != 0) return result;
                }

                return 0;
            };

    /**
     * Generates all INDs that are implied by the given {@code ind} of the desired {@code arity} in lexicographical order.
     *
     * @param ind       whose implied INDs should be generated
     * @param arity     of the generated {@link IND}s
     * @param collector collects the results
     */
    public static void generateImpliedInds(IND ind, int arity, Collection<IND> collector) {
        generateImpliedIndsAux(new IntArrayList(arity), ind, collector, arity);
    }

    /**
     * Generates all INDs that are implied by the given {@code ind} of the desired {@code arity} in lexicographical order
     * by recursion. Should be used via {@link #generateImpliedInds(IND, int, Collection)}
     *
     * @param selectedUindPos are positions of unary INDs in {@code ind} that are already selected
     * @param ind             is the IND whose generalizing INDs should be collected
     * @param arity           is number unary INDs embedded in the collected INDs
     * @param collector       collects the results
     */
    private static void generateImpliedIndsAux(IntList selectedUindPos,
                                               IND ind,
                                               Collection<IND> collector,
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
                dep[index] = ind.getDependentColumns()[pos];
                ref[index] = ind.getReferencedColumns()[pos];
                index++;
            }
            collector.add(new IND(dep, ref));
        }
    }

    /**
     * Groups {@link IND}s by their IND subspace and sorts the IND groups.
     *
     * @param inds          is a collection of INDs to be grouped
     * @param columnBitMask marks the bits in the column IDs that encode the column index
     * @return a map from IND subspace descriptions to sorted IND groups
     */
    public static Map<IndSubspaceKey, SortedSet<IND>> groupIntoSubspaces(
            final Collection<IND> inds,
            final int columnBitMask) {
        final Map<IndSubspaceKey, SortedSet<IND>> indGroups = new HashMap<>();
        for (IND ind : inds) {
            final IndSubspaceKey key = IndSubspaceKey.createFromInd(ind, columnBitMask);
            final SortedSet<IND> indGroup = indGroups.computeIfAbsent(key, k -> new TreeSet<>(COMPARATOR));
            indGroup.add(ind);
        }
        return indGroups;
    }

}
