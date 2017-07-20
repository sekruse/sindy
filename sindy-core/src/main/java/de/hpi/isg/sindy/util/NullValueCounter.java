package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of values in columns / column combinations.
 */
public class NullValueCounter extends IntCounter {

    /**
     * Default {@link Accumulator} key.
     */
    public static final String DEFAULT_KEY = "null-values";

    public NullValueCounter() {
        super();
    }
    public NullValueCounter(Int2IntOpenHashMap counts) {
        super(counts);
    }

    @Override
    public Accumulator<Integer, Int2IntOpenHashMap> clone() {
        return new NullValueCounter(this.counts);
    }

}
