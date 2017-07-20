package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of values in columns / column combinations.
 */
public class DistinctValueCounter extends IntCounter {

    /**
     * Default {@link Accumulator} key.
     */
    public static final String DEFAULT_KEY = "distinct-values";

    public DistinctValueCounter() {
        super();
    }
    public DistinctValueCounter(Int2IntOpenHashMap counts) {
        super(counts);
    }

    @Override
    public Accumulator<Integer, Int2IntOpenHashMap> clone() {
        return new DistinctValueCounter(this.counts);
    }
}
