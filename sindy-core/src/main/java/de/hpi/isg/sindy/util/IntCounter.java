package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to count the number of {@code int}s.
 */
public class IntCounter implements Accumulator<Integer, Int2IntOpenHashMap> {

    /**
     * Maps table IDs to the number of columns in this table.
     */
    protected final Int2IntOpenHashMap counts;

    /**
     * Creates a new instance.
     */
    public IntCounter() {
        this(new Int2IntOpenHashMap());
    }

    /**
     * Creates a new instances with the given data.
     * @param counts the data
     */
    IntCounter(Int2IntOpenHashMap counts) {
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
    public Int2IntOpenHashMap getLocalValue() {
        return this.counts;
    }

    @Override
    public void resetLocal() {
        this.counts.clear();
    }

    @Override
    public void merge(Accumulator<Integer, Int2IntOpenHashMap> accumulator) {
        for (ObjectIterator<Int2IntMap.Entry> iter = accumulator.getLocalValue().int2IntEntrySet().fastIterator(); iter.hasNext();) {
            Int2IntMap.Entry entry = iter.next();
            this.counts.addTo(entry.getIntKey(), entry.getIntValue());
        }
    }

    @Override
    public Accumulator<Integer, Int2IntOpenHashMap> clone() {
        return new IntCounter(this.counts);
    }
}
