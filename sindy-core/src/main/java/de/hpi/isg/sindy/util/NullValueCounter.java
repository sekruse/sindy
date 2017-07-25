package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of values in columns / column combinations.
 */
public class NullValueCounter extends LongCounter {

    /**
     * Default {@link Accumulator} key.
     */
    public static final String DEFAULT_KEY = "null-values";

    public NullValueCounter() {
        super();
    }
    public NullValueCounter(Int2LongOpenHashMap counts) {
        super(counts);
    }

    @Override
    public Accumulator<Integer, Int2LongOpenHashMap> clone() {
        return new NullValueCounter(this.counts);
    }

}
