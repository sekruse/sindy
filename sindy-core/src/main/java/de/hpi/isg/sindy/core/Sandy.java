package de.hpi.isg.sindy.core;

import de.hpi.isg.sindy.io.RemoteCollectorImpl;
import de.hpi.isg.sindy.util.IND;
import de.hpi.isg.sindy.util.PartialIND;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.function.Consumer;

/**
 * This class implements an adaption of the distributed IND algorithm SINDY, which finds (unary) partial INDs.
 * <p>The current partiality measure is as follows: Let {@code a} be the number of distinct values that appear
 * both in the dependent and referenced column; and let {@code b} be the number of distinct values that appear only
 * in the dependent column. Then the <i>overlap</i> is defined as {@code a/b}.</p>
 *
 * @author Sebastian Kruse
 */
public class Sandy extends AbstractSindy implements Runnable {

    // TODO: With a different partiality metric that is monotonously increasing, we could also go for n-ary partial INDs.
    // Distinct-value overlap does not satisfy that.
    // Number of "dependent" tuples could, though.

    /**
     * Counts the collected INDs.
     */
    private int numDiscoveredInds;

    /**
     * Accepts INDs as soon as they are available.
     */
    private Consumer<PartialIND> indCollector;

    /**
     * Collects INDs so as to generate candidates of higher arity.
     */
    private Collection<PartialIND> newInds, allInds;

    private double minRelativeOverlap = 0.9d;

    /**
     * Creates a new instance.
     *
     * @param inputFiles           input files with their table IDs; see {@link #inputFiles}
     * @param numColumnBits        the number of column bits in the table IDs
     * @param executionEnvironment Flink {@link ExecutionEnvironment} to use
     * @param indCollector         collects INDs as soon as they are discovered
     */
    public Sandy(Int2ObjectMap<String> inputFiles, int numColumnBits,
                 ExecutionEnvironment executionEnvironment,
                 Consumer<PartialIND> indCollector) {
        super(inputFiles, numColumnBits, executionEnvironment);
        this.indCollector = indCollector;
    }


    @Override
    public void run() {
        this.initializeRun();
        try {
            try {
                this.executeRun();
            } finally {
                this.cleanUp();
            }
        } catch (Throwable t) {
            this.logger.error("SINDY failed.", t);
            throw new RuntimeException("SINDY failed.", t);
        }
    }

    private void initializeRun() {
        if (this.isOnlyCountInds && this.maxArity != 1) {
            this.logger.warn("Counting only, will process only unary INDs.");
            this.maxArity = 1;
        }

        if (this.maxArity > 1) {
            if (this.naryIndRestrictions == null || this.candidateGenerator == null) {
                throw new IllegalStateException("No n-ary IND restrictions and/or candidate generator set up.");
            }
        }

        this.numDiscoveredInds = 0;
    }

    protected void executeRun() throws Exception {
        // Take care of the unary IND detection.
        DataSet<Tuple4<Integer, Integer, int[], int[]>> overlaps = this.buildUnaryOverlapDetectionPlan(this.inputFiles.keySet());
        AbstractSindy.AddCommandFactory<Tuple4<Integer, Integer, int[], int[]>> handleResultCommandFactory =
                overlapSet ->
                        (Runnable) (() -> {
                            // Handle the distinct value count.
                            int dependentId = overlapSet.f0;
                            int dependentCount = overlapSet.f1;

                            // Handle the overlaps.
                            for (int i = 0; i < overlapSet.f2.length; i++) {
                                int referencedId = overlapSet.f2[i];
                                int overlap = overlapSet.f3[i];
                                Sandy.this.collectUnaryPartialInd(dependentId, referencedId, dependentCount, overlap);
                            }
                        });
        String jobName = String.format("SANDY on %d tables (%s)", this.inputFiles.size(), new Date());
        this.newInds = new ArrayList<>();
        this.collectAsync(handleResultCommandFactory, overlaps, jobName);

        this.allInds = this.newInds;
    }


    protected void cleanUp() throws Exception {
        RemoteCollectorImpl.shutdownAll();
    }

    /**
     * Collects a unary {@link IND}.
     *
     * @param dependentId  is the ID of the dependent column
     * @param referencedId is the ID of the referenced column
     * @param dependentCount         the number of distinct values in the dependent columns
     * @param overlap                the number of common distinct values of the dependent and referenced column
     */
    protected void collectUnaryPartialInd(final int dependentId, final int referencedId, final int dependentCount, final int overlap) {
        this.collectPartialInd(dependentId, referencedId, null, null, dependentCount, overlap);
    }

    /**
     * Adds an {@link PartialIND}.
     *
     * @param dependentId            is either the ID of the dependent column or a column combination ID
     * @param referencedId           is either the ID of the referenced column or a column combination ID
     * @param columnCombinationsById is {@code null} if a unary IND is added or a map that assigns column combinations to IDs.
     * @param indCandidates          is {@code null} or a set of IND candidates
     * @param dependentCount         the number of distinct values in the dependent columns
     * @param overlap                the number of common distinct values of the dependent and referenced column
     */
    protected void collectPartialInd(final int dependentId, final int referencedId,
                                     Int2ObjectMap<IntList> columnCombinationsById, final Set<IND> indCandidates,
                                     int dependentCount, int overlap) {

        // Resolve the column combination IDs.
        PartialIND ind;
        double partiality = dependentCount == 0 ? 0d : overlap / (double) dependentCount;
        if (partiality < this.minRelativeOverlap) return;

        if (columnCombinationsById == null) {
            // We have a unary IND.
            ind = new PartialIND(dependentId, referencedId, dependentCount, partiality);
        } else {
            // We have an n-ary IND.
            IntList dependentColumns = columnCombinationsById.get(dependentId);
            IntList referencedColumns = columnCombinationsById.get(referencedId);
            ind = new PartialIND(dependentColumns.toIntArray(), referencedColumns.toIntArray(), dependentCount, partiality);
            if (indCandidates != null && !indCandidates.contains(ind)) {
                this.logger.info("Rejected n-ary pseudo IND {}.", ind);
                return;
            }
        }

        // Check if the IND is also a valid candidate.
        if (indCandidates != null && !indCandidates.contains(ind)) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Reject pseudo-IND {}.", ind);
            }
            return;
        }


        if (this.indCollector != null) this.indCollector.accept(ind);
        this.newInds.add(ind);

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Discovered {} (might be non-maximal).", ind);
        }

        if (++this.numDiscoveredInds % 1000000 == 0) {
            this.logger.info("{} INDs added so far.", this.numDiscoveredInds);
        }
    }

    public double getMinRelativeOverlap() {
        return this.minRelativeOverlap;
    }

    public void setMinRelativeOverlap(double minRelativeOverlap) {
        this.minRelativeOverlap = minRelativeOverlap;
    }

    /**
     * @return the number of {@link IND}s discovered in the last run
     */
    public int getNumDiscoveredInds() {
        return this.numDiscoveredInds;
    }

    /**
     * In contrast to the {@link #indCollector}, which immediately forwards all {@link IND}s, this method can be
     * called after {@link #run()} and provides a consolidated view on the {@link IND}s with any non-maximal
     * results removed.
     *
     * @return the maximal {@link IND}s from the last invocation of {@link #run()}
     */
    public Collection<PartialIND> getAllInds() {
        return this.allInds;
    }
}