package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of {@code int}s.
 */
public class LongCounter implements Accumulator<Integer, Int2LongOpenHashMap> {

    /**
     * Maps table IDs to the number of columns in this table.
     */
    protected final Int2LongOpenHashMap counts;

    /**
     * Creates a new instance.
     */
    public LongCounter() {
        this(new Int2LongOpenHashMap());
    }

    /**
     * Creates a new instances with the given data.
     *
     * @param counts the data
     */
    LongCounter(Int2LongOpenHashMap counts) {
        this.counts = counts;
        this.counts.defaultReturnValue(0);
    }

    @Override
    public void add(Integer columnId) {
        this.counts.addTo(columnId, 1);
    }

    public void add(int columnId) {
        this.counts.addTo(columnId, 1);
    }

    @Override
    public Int2LongOpenHashMap getLocalValue() {
        return this.counts;
    }

    @Override
    public void resetLocal() {
        this.counts.clear();
    }

    @Override
    public void merge(Accumulator<Integer, Int2LongOpenHashMap> accumulator) {
        for (ObjectIterator<Int2LongMap.Entry> iter = accumulator.getLocalValue().int2LongEntrySet().fastIterator(); iter.hasNext(); ) {
            Int2LongMap.Entry entry = iter.next();
            this.counts.addTo(entry.getIntKey(), entry.getLongValue());
        }
    }

    @Override
    public Accumulator<Integer, Int2LongOpenHashMap> clone() {
        return new LongCounter(new Int2LongOpenHashMap(this.counts));
    }
}
