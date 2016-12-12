package de.hpi.isg.sindy.udf;

import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.IdUtils;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This function splits a given {@link Tuple} into column combinations. The given first integer field will be offset by the field
 * index.
 *
 * @author Sebastian Kruse
 */
public class SplitFieldsToCombinations extends RichFlatMapFunction<Tuple, Tuple2<Integer, String>> {

    private static final long serialVersionUID = 1377116120504051734L;

    private static final Logger LOG = LoggerFactory.getLogger(SplitFieldsToCombinations.class);

    /**
     * Reusable output object.
     */
    private final Tuple2<Integer, String> outputTuple = new Tuple2<Integer, String>();

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
     * @param columnCombinationIds          a mapping from {@link Table} IDs to lists of {@link Column} IDs, that form the requested
     *                                      column combinations
     * @param idUtils                       to manipulate those IDs
     * @param isOutputCombinationsWithNulls whether to consider column combinations that contain {@code null}s
     */
    public SplitFieldsToCombinations(final Object2IntMap<IntList> columnCombinationIds,
                                     final IdUtils idUtils,
                                     final boolean isOutputCombinationsWithNulls) {

        this.isOutputCombinationsWithNulls = isOutputCombinationsWithNulls;

        this.offsetsWithIdByMinColumnId = new Int2ObjectOpenHashMap<>();
        for (final Object2IntMap.Entry<IntList> entry : columnCombinationIds.object2IntEntrySet()) {
            final IntList columnList = entry.getKey();
            final int columnCombinationId = entry.getIntValue();

            // extract the base column ID
            final int anyColumnId = columnList.getInt(0);
            final int minColumnId = idUtils.createGlobalId(idUtils.getLocalSchemaId(anyColumnId),
                    idUtils.getLocalTableId(anyColumnId), idUtils.getMinColumnNumber());

            // FIXME: It should rather be .. by table ID (see PlanBuildingUtils#createTupleDataSet)
            Object2IntMap<IntList> offsetsWithId = this.offsetsWithIdByMinColumnId.get(minColumnId);
            if (offsetsWithId == null) {
                offsetsWithId = new Object2IntArrayMap<>();
                this.offsetsWithIdByMinColumnId.put(minColumnId, offsetsWithId);
            }
            final IntList columnOffsets = new IntArrayList(columnList.size());
            for (final int column : columnList) {
                columnOffsets.add(idUtils.getLocalColumnId(column) - idUtils.getMinColumnNumber());
            }
            offsetsWithId.put(columnOffsets, columnCombinationId);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add column combination {} with ID {} to file that starts with column ID {}.\n",
                        columnOffsets, columnCombinationId, Integer.toHexString(minColumnId));
            }
        }
    }

    @Override
    public void flatMap(final Tuple tuple, final Collector<Tuple2<Integer, String>> out)
            throws Exception {

        // FIXME: This looks wrong, but I am not sure, if it is.
        final Object2IntMap<IntList> offsetsWithId = this.offsetsWithIdByMinColumnId.get(tuple.getMinColumnId());
        if (offsetsWithId == null || offsetsWithId.isEmpty()) {
            return;
        }

        ColumnCombinations:
        for (final Object2IntArrayMap.Entry<IntList> entry : offsetsWithId.object2IntEntrySet()) {
            final IntList columnOffsets = entry.getKey();
            final int combinationId = entry.getIntValue();

            String[] fields = tuple.getFields();

            this.sb.setLength(0);
            String separator = "";
            for (final IntListIterator i = columnOffsets.iterator(); i.hasNext(); ) {
                final int offset = i.nextInt();
                String field = fields[offset];
                if (field == null) {
                    if (this.isOutputCombinationsWithNulls) {
                        field = "\1";
                    } else {
                        continue ColumnCombinations;
                    }
                }
                this.sb.append(separator).append(field);
                separator = "\0";
            }

            this.outputTuple.f0 = combinationId;
            this.outputTuple.f1 = this.sb.toString();
            out.collect(this.outputTuple);
        }

    }
}
