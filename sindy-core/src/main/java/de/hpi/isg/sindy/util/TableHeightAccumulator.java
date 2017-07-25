package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of tuples in the different tables.
 */
public class TableHeightAccumulator extends LongCounter {

    /**
     * Default {@link Accumulator} key.
     */
    public static final String DEFAULT_KEY = "table-heights";

    public TableHeightAccumulator() {
        super();
    }

    public TableHeightAccumulator(Int2LongOpenHashMap counts) {
        super(counts);
    }

    @Override
    public Accumulator<Integer, Int2LongOpenHashMap> clone() {
        return new TableHeightAccumulator(this.counts);
    }
}
