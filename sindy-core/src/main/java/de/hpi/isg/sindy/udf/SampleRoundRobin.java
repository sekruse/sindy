package de.hpi.isg.sindy.udf;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

/**
 * This filter UDF samples values in a round-robin fashion.
 */
public class SampleRoundRobin<T> extends RichFilterFunction<T> {

    private final int samplingPercentage;

    private int counter;

    private boolean[] selectionMask;

    /**
     * Creates a new instance.
     *
     * @param samplingPercentage the percentage ({@code 0 < x < 100}) of elements to be sampled
     */
    public SampleRoundRobin(int samplingPercentage) {
        if (samplingPercentage <= 0 || samplingPercentage >= 100) {
            throw new IllegalArgumentException("Illegal sampling percentage.");
        }
        this.samplingPercentage = samplingPercentage;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.selectionMask = new boolean[100];
        double delta = 100d / this.samplingPercentage;
        for (int i = 0; i < this.samplingPercentage; i++) {
            this.selectionMask[(int) (i * delta)] = true;
        }
        this.counter = -1;
    }

    @Override
    public boolean filter(T t) throws Exception {
        if (++this.counter == 100) this.counter = 0;
        return this.selectionMask[this.counter];
    }
}
