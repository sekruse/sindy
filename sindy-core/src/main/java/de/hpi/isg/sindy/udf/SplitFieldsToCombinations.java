package de.hpi.isg.sindy.udf;

import de.hpi.isg.sindy.data.IntObjectTuple;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function splits a given input tuples into column combinations. The given first integer field will be offset by the field
 * index.
 *
 * @author Sebastian Kruse
 */
public class SplitFieldsToCombinations extends RichFlatMapFunction<IntObjectTuple<String[]>, IntObjectTuple<String>> {

    private static final long serialVersionUID = 1377116120504051734L;

    private static final Logger LOG = LoggerFactory.getLogger(SplitFieldsToCombinations.class);

    /**
     * Reusable output object.
     */
    private final IntObjectTuple<String> outputTuple = new IntObjectTuple<>();

    /**
     * Maintains a mapping from TODO.
     */
    private final Int2ObjectMap<Object2IntMap<IntList>> offsetsWithIdByMinColumnId;

    /**
     * Used to build up combined values.
     */
    private final StringBuilder sb = new StringBuilder();

    /**
     * Whether column combinations that contain {@code null} values should be considered.
     */
    private final boolean isOutputCombinationsWithNulls;

    /**
     * Create a new instance.
     *
     * @param columnCombinationIds          a mapping from minimum column IDs to lists of column IDs; each such list is
     *                                      a column combination to be formed
     * @param columnBitMask                 marks the column bits in the IDs
     * @param isOutputCombinationsWithNulls whether to consider column combinations that contain {@code null}s
     */
    public SplitFieldsToCombinations(final Object2IntMap<IntList> columnCombinationIds,
                                     final int columnBitMask,
                                     final boolean isOutputCombinationsWithNulls) {

        this.isOutputCombinationsWithNulls = isOutputCombinationsWithNulls;

        this.offsetsWithIdByMinColumnId = new Int2ObjectOpenHashMap<>();
        for (final Object2IntMap.Entry<IntList> entry : columnCombinationIds.object2IntEntrySet()) {
            final IntList columnList = entry.getKey();
            final int columnCombinationId = entry.getIntValue();

            // extract the base column ID
            final int anyColumnId = columnList.getInt(0);
            final int minColumnId = anyColumnId & ~columnBitMask; // set all column bits to zero -> first column

            // FIXME: It should rather be .. by table ID (see PlanBuildingUtils#createTupleDataSet)
            Object2IntMap<IntList> offsetsWithId = this.offsetsWithIdByMinColumnId.get(minColumnId);
            if (offsetsWithId == null) {
                offsetsWithId = new Object2IntArrayMap<>();
                this.offsetsWithIdByMinColumnId.put(minColumnId, offsetsWithId);
            }
            final IntList columnIndices = new IntArrayList(columnList.size());
            for (final int column : columnList) {
                final int columnIndex = column & columnBitMask; // i.e., retain only the column bits
                columnIndices.add(columnIndex);
            }
            offsetsWithId.put(columnIndices, columnCombinationId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add column combination {} with ID {} to file that starts with column ID {}.\n",
                        columnIndices, columnCombinationId, Integer.toHexString(minColumnId));
            }
        }
    }

    @Override
    public void flatMap(final IntObjectTuple<String[]> tuple, final Collector<IntObjectTuple<String>> out)
            throws Exception {

        final int minColumnId = tuple.a;
        final Object2IntMap<IntList> offsetsWithId = this.offsetsWithIdByMinColumnId.get(minColumnId);
        if (offsetsWithId == null || offsetsWithId.isEmpty()) {
            return;
        }

        ColumnCombinations:
        for (final Object2IntArrayMap.Entry<IntList> entry : offsetsWithId.object2IntEntrySet()) {
            final IntList columnOffsets = entry.getKey();
            final int combinationId = entry.getIntValue();

            String[] fields = tuple.b;

            this.sb.setLength(0);
            String separator = "";
            for (final IntListIterator i = columnOffsets.iterator(); i.hasNext(); ) {
                final int offset = i.nextInt();
                String field = fields[offset];
                if (field == null) {
                    if (this.isOutputCombinationsWithNulls) {
                        field = "\1"; // marker for null values
                    } else {
                        continue ColumnCombinations;
                    }
                }
                this.sb.append(separator).append(field);
                separator = "\0"; // field delimiter
            }

            this.outputTuple.a = combinationId;
            this.outputTuple.b = this.sb.toString();
            out.collect(this.outputTuple);
        }

    }
}
