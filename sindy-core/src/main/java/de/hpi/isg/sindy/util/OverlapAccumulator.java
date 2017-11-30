package de.hpi.isg.sindy.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.flink.api.common.accumulators.Accumulator;

/**
 * An {@link Accumulator} to create a histogram of relative overlaps.
 */
public class OverlapAccumulator implements Accumulator<Double, IntArrayList> {

    /**
     * Default {@link Accumulator} key.
     */
    public static final String DEFAULT_KEY = "overlap-counter";

    public static final String[] KEYS = new String[]{"[0, 0]", "[0, 0.1)", "[0.1, 0.9)", "[0.9, 0.99)", "[0.99, 1)", "[1.0, 1.0]"};

    /**
     * The number of buckets. We have
     * <ol>
     * <li>= 0,</li>
     * <li>< 0.1,</li>
     * <li>< 0.9,</li>
     * <li>< 0.99</li>
     * <li>< 1, and</li>
     * <li>= 1.0.</li>
     * </ol>
     */
    public static final int NUM_BUCKETS = KEYS.length;

    /**
     * Stores buckets.
     */
    private final IntArrayList buckets;

    /**
     * Create a new {@link #buckets} structure.
     *
     * @return an {@link IntArrayList} for the buckets
     */
    private static final IntArrayList createBuckets() {
        IntArrayList buckets = new IntArrayList(NUM_BUCKETS);
        for (int i = 0; i < NUM_BUCKETS; i++) {
            buckets.add(0);
        }
        return buckets;
    }

    public OverlapAccumulator() {
        this(createBuckets());
    }

    public OverlapAccumulator(IntArrayList buckets) {
        this.buckets = buckets;
    }

    @Override
    public void add(Double value) {
        this.add(value.doubleValue());
    }

    public void add(double value) {
        int bucket;
        if (value == 0d) bucket = 0;
        if (value < 0.1) bucket = 1;
        else if (value < 0.9) bucket = 2;
        else if (value < 0.99) bucket = 3;
        else if (value < 1) bucket = 4;
        else bucket = 5;
        this.buckets.set(bucket, this.buckets.getInt(bucket) + 1);
    }

    @Override
    public IntArrayList getLocalValue() {
        return this.buckets;
    }

    @Override
    public void resetLocal() {
        for (int i = 0; i < NUM_BUCKETS; i++) {
            this.buckets.set(i, 0);
        }
    }

    @Override
    public void merge(Accumulator<Double, IntArrayList> other) {
        IntArrayList thatBuckets = other.getLocalValue();
        for (int i = 0; i < NUM_BUCKETS; i++) {
            this.buckets.set(i, this.buckets.getInt(i) + thatBuckets.getInt(i));
        }
    }

    @Override
    public OverlapAccumulator clone() {
        return new OverlapAccumulator(this.buckets.clone());
    }
}
