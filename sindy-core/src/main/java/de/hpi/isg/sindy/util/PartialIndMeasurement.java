package de.hpi.isg.sindy.util;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link Measurement} that captures the number of {@link IND}s and candidates..
 */
@Type("partial-inds")
public class PartialIndMeasurement extends Measurement {

    /**
     * Stores a histogram of the encountered overlap ratios.
     */
    private Map<String, Integer> overlapRatioHistogram = new HashMap<>();


    /**
     * Serialization constructor.
     */
    @SuppressWarnings("unused")
    private PartialIndMeasurement() {
        super();
    }

    /**
     * Creates a new instance.
     *
     * @param id the ID of the new instance
     */
    public PartialIndMeasurement(String id) {
        super(id);
    }

    /**
     * Add a new entry into the {@link #overlapRatioHistogram}.
     *
     * @param key   for the bucket
     * @param count of the bucket
     */
    public void add(String key, int count) {
        this.overlapRatioHistogram.put(key, count);
    }

    /**
     * Return the histogram of overlap ratios.
     *
     * @return the histogram
     */
    public Map<String, Integer> getOverlapRatioHistogram() {
        return this.overlapRatioHistogram;
    }

    @Override
    public String toString() {
        return String.format("%s[%s, %s]", this.getClass().getSimpleName(), this.getId(), this.overlapRatioHistogram);
    }
}
