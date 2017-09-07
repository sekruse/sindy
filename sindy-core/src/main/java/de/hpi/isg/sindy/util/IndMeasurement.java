package de.hpi.isg.sindy.util;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link Measurement} that captures the number of {@link IND}s and candidates..
 */
@Type("inds")
public class IndMeasurement extends Measurement {

    private Collection<Entry> entries = new ArrayList<>();

    /**
     * An entry in a {@link IndMeasurement}.
     */
    public static class Entry {

        private int arity, numInds, numCandidates, numArs;

        public Entry(int arity, int numCandidates, int numInds, int numArs) {
            this.arity = arity;
            this.numInds = numInds;
            this.numCandidates = numCandidates;
            this.numArs = numArs;
        }

        private Entry() {
        }

        public int getArity() {
            return this.arity;
        }

        public int getNumInds() {
            return this.numInds;
        }

        public int getNumCandidates() {
            return this.numCandidates;
        }
    }

    /**
     * Serialization constructor.
     */
    @SuppressWarnings("unused")
    private IndMeasurement() {
        super();
    }

    /**
     * Creates a new instance.
     *
     * @param id the ID of the new instance
     */
    public IndMeasurement(String id) {
        super(id);
    }

    /**
     * Add a new {@link Entry} to this instance.
     *
     * @param arity         the arity of the {@link IND}s
     * @param numCandidates the number of {@link IND} candidates of the given {@code arity}
     * @param numInds       the number of {@link IND}s of the given {@code arity}
     * @param numArs        the number of {@link de.hpi.isg.sindy.searchspace.IndAugmentationRule}s of the given {@code arity}
     */
    public void add(int arity, int numCandidates, int numInds, int numArs) {
        this.entries.add(new Entry(arity, numCandidates, numInds, numArs));
    }

    /**
     * Returns the {@link Entry}s stored in this instance.
     *
     * @return the {@link Entry}s
     */
    public Collection<Entry> getEntries() {
        return this.entries;
    }

    @Override
    public String toString() {
        return String.format("%s[%s, %,d entries]", this.getClass().getSimpleName(), this.getId(), this.entries.size());
    }
}
