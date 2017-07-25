package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of tuples in the different tables.
 */
public class TableHeightAccumulator extends IntCounter {

    /**
     * Default {@link Accumulator} key.
     */
    public static final String DEFAULT_KEY = "table-heights";

    public TableHeightAccumulator() {
        super();
    }

    public TableHeightAccumulator(Int2IntOpenHashMap counts) {
        super(counts);
    }

    @Override
    public Accumulator<Integer, Int2IntOpenHashMap> clone() {
        return new TableHeightAccumulator(this.counts);
    }
}
