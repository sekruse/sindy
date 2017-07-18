package de.hpi.isg.sindy.util;

import de.hpi.isg.sindy.data.IntObjectTuple;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of columns in the different tables.
 */
public class TableWidthAccumulator implements Accumulator<IntObjectTuple<Integer>, Int2IntOpenHashMap> {

    /**
     * Default {@link Accumulator} key.
     */
    public static final String DEFAULT_KEY = "table-widths";

    /**
     * Maps table IDs to the number of columns in this table.
     */
    private final Int2IntOpenHashMap numColumnsByTableId;

    /**
     * Creates a new instance.
     */
    public TableWidthAccumulator() {
        this(new Int2IntOpenHashMap());
    }

    /**
     * Creates a new instances with the given data.
     * @param numColumnsByTableId the data
     */
    private TableWidthAccumulator(Int2IntOpenHashMap numColumnsByTableId) {
        this.numColumnsByTableId = numColumnsByTableId;
    }

    @Override
    public void add(IntObjectTuple<Integer> tableIdAndNumColumns) {
        this.numColumnsByTableId.put(tableIdAndNumColumns.a, tableIdAndNumColumns.b.intValue());
    }

    @Override
    public Int2IntOpenHashMap getLocalValue() {
        return this.numColumnsByTableId;
    }

    @Override
    public void resetLocal() {
        this.numColumnsByTableId.clear();
    }

    @Override
    public void merge(Accumulator<IntObjectTuple<Integer>, Int2IntOpenHashMap> accumulator) {
        this.numColumnsByTableId.putAll(accumulator.getLocalValue());
    }

    @Override
    public Accumulator<IntObjectTuple<Integer>, Int2IntOpenHashMap> clone() {
        return new TableWidthAccumulator(this.numColumnsByTableId);
    }
}
