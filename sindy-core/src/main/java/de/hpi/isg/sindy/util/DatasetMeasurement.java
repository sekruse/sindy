package de.hpi.isg.sindy.util;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;

/**
 * A {@link Measurement} that captures the size of a dataset.
 */
@Type("dataset")
public class DatasetMeasurement extends Measurement {

    /**
     * The number of columns.
     */
    private int numColumns = -1;

    /**
     * The number of tuples.
     */
    private long numTuples = -1L;


    /**
     * Serialization constructor.
     */
    @SuppressWarnings("unused")
    private DatasetMeasurement() {
        super();
    }

    /**
     * Creates a new instance.
     *
     * @param id the ID of the new instance
     */
    public DatasetMeasurement(String id) {
        super(id);
    }


    public int getNumColumns() {
        return this.numColumns;
    }

    public void setNumColumns(int numColumns) {
        this.numColumns = numColumns;
    }

    public long getNumTuples() {
        return this.numTuples;
    }

    public void setNumTuples(long numTuples) {
        this.numTuples = numTuples;
    }

    @Override
    public String toString() {
        return String.format("%s[%s, %,d x %,d]", this.getClass().getSimpleName(), this.getId(), this.numColumns, this.numTuples);
    }
}
