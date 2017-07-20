package de.hpi.isg.sindy.core;

import de.hpi.isg.sindy.io.RemoteCollectorImpl;
import de.hpi.isg.sindy.searchspace.IndAugmentationRule;
import de.hpi.isg.sindy.searchspace.IndSubspaceKey;
import de.hpi.isg.sindy.util.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.function.Consumer;

/**
 * This class implements the distributed IND algorithm SINDY. In addition, it supports n-ary IND detection.
 *
 * @author Sebastian Kruse
 */
public class Andy extends AbstractSindy implements Runnable {

    /**
     * Counts the collected INDs.
     */
    private int numDiscoveredInds;

    /**
     * Accepts INDs as soon as they are available.
     */
    private final Consumer<IND> indCollector;

    /**
     * Collects INDs so as to generate candidates of higher arity.
     */
    private Collection<IND> newInds, allInds;

    /**
     * Keeps track of {@link IndAugmentationRule}s. The keys are the LHS of the respected {@link IndAugmentationRule}.
     */
    private Collection<IndAugmentationRule> augmentationRules;

    /**
     * Creates a new instance.
     *
     * @param inputFiles           input files with their table IDs; see {@link #inputFiles}
     * @param numColumnBits        the number of column bits in the table IDs
     * @param executionEnvironment Flink {@link ExecutionEnvironment} to use
     * @param indCollector         collects INDs as soon as they are discovered
     */
    public Andy(Int2ObjectMap<String> inputFiles, int numColumnBits,
                ExecutionEnvironment executionEnvironment,
                Consumer<IND> indCollector) {
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

        if (this.maxArity > 1 || this.maxArity == -1) {
            if (this.naryIndRestrictions == null || this.candidateGenerator == null) {
                throw new IllegalStateException("No n-ary IND restrictions and/or candidate generator set up.");
            }
        }

        this.numDiscoveredInds = 0;
    }

    protected void executeRun() throws Exception {
        // Take care of the unary IND detection.
        DataSet<Tuple2<Integer, int[]>> unaryInds = this.buildUnaryIndDetectionPlan(this.inputFiles.keySet()).project(0, 2);
        if (this.isOnlyCountInds) {
            int numInds = this.count(unaryInds);
            this.numDiscoveredInds = numInds;
            this.logger.info("Discovered {} unary INDs.", numInds);
            return;
        }

        this.newInds = new ArrayList<>();
        this.augmentationRules = new ArrayList<>();
        AbstractSindy.AddCommandFactory<Tuple2<Integer, int[]>> addUnaryIndCommandFactory = indSet ->
                (Runnable) (() -> {
                    for (int referencedId : indSet.f1) {
                        int dependentId = indSet.f0;
                        Andy.this.collectUnaryInd(dependentId, referencedId);
                    }
                });
        String jobName = String.format("ANDY on %d tables (unary, %s)", this.inputFiles.size(), new Date());
        this.collectAsync(addUnaryIndCommandFactory, unaryInds, jobName);

        // Retrieve the number of null values per column.
        JobExecutionResult result = this.getJobMeasurements().get(0).getFlinkResults();
        Int2IntOpenHashMap simpleNullValueCounts = result.getAccumulatorResult(NullValueCounter.DEFAULT_KEY);
        List<Object2IntMap<IntList>> nullValueCountsByArity = new ArrayList<>();
        {
            Object2IntMap<IntList> nullValueCounts = new Object2IntOpenHashMap<>(simpleNullValueCounts.size());
            for (ObjectIterator<Int2IntMap.Entry> iter = simpleNullValueCounts.int2IntEntrySet().fastIterator(); iter.hasNext(); ) {
                Int2IntMap.Entry entry = iter.next();
                nullValueCounts.put(new IntArrayList(new int[]{entry.getIntKey()}), entry.getIntValue());
            }
            nullValueCountsByArity.add(0, nullValueCounts);
        }

        // Store the distinct value counts of non-empty columns.
        Int2IntOpenHashMap simpleDistinctValueCounts = result.getAccumulatorResult(DistinctValueCounter.DEFAULT_KEY);
        List<Object2IntMap<IntList>> distinctValueCountsByArity = new ArrayList<>();
        {
            Object2IntMap<IntList> distinctValueCounts = new Object2IntOpenHashMap<>(simpleDistinctValueCounts.size());
            for (ObjectIterator<Int2IntMap.Entry> iter = simpleDistinctValueCounts.int2IntEntrySet().fastIterator(); iter.hasNext(); ) {
                Int2IntMap.Entry entry = iter.next();
                distinctValueCounts.put(new IntArrayList(new int[]{entry.getIntKey()}), entry.getIntValue());
            }
            distinctValueCountsByArity.add(0, distinctValueCounts);
        }

        // Detect empty columns and add appropriate INDs.
        Int2IntOpenHashMap numColumnsByTableId = result.getAccumulatorResult(TableWidthAccumulator.DEFAULT_KEY);
        IntList allColumnIds = new IntArrayList();
        for (Map.Entry<Integer, Integer> entry : numColumnsByTableId.int2IntEntrySet()) {
            int columnId = entry.getKey();
            for (int i = 0; i < entry.getValue(); i++) {
                allColumnIds.add(columnId + i);
            }
        }
        for (IntIterator depIter = allColumnIds.iterator(); depIter.hasNext(); ) {
            int depId = depIter.nextInt();
            if (simpleDistinctValueCounts.get(depId) == 0) {
                for (IntIterator refIter = allColumnIds.iterator(); refIter.hasNext(); ) {
                    int refId = refIter.nextInt();
                    if (depId != refId) this.collectUnaryInd(depId, refId);
                }
            }
        }

        // Go over the INDs to find 0-ary ARs. Promote all other INDs.
        for (Iterator<IND> iter = this.newInds.iterator(); iter.hasNext(); ) {
            IND ind = iter.next();

            // Determine distinct value count of dependent and referenced column.
            int depColumnId = ind.getDependentColumns()[0];
            int refColumnId = ind.getReferencedColumns()[0];
            if (simpleDistinctValueCounts.get(depColumnId) == 0 && simpleDistinctValueCounts.get(refColumnId) == 1) {
                IndAugmentationRule iar = new IndAugmentationRule(IND.emptyInstance, ind);
                this.augmentationRules.add(iar);
                System.out.printf("Discovered %s.\n", iar);
                iter.remove();
            }
        }


        // Now perform n-ary IND detection using the Apriori candidate generation.
        this.allInds = this.newInds;
        int newArity = 2;
        while (this.newInds != null && !this.newInds.isEmpty() && (newArity <= this.maxArity || this.maxArity == -1)) {
            this.logger.info("{} INDs for n-ary IND generation.", this.newInds.size());
            if (this.logger.isDebugEnabled()) {
                List<IND> temp = new ArrayList<>(this.newInds);
                temp.sort(INDs.COMPARATOR);
                if (this.logger.isDebugEnabled()) {
                    for (IND candidate : temp) {
                        this.logger.debug("-> IND {}", candidate);
                    }
                }
            }

            // Generate n-ary IND candidates.
            final Set<IND> indCandidates = this.generateCandidates(this.newInds);
            if (indCandidates.isEmpty()) {
                break;
            }

            // For any column combination in the n-ary IND candidates, create an ID.
            final Object2IntMap<IntList> columnCombinationIds = this.createIdsForColumnCombinations2(indCandidates);
            final Int2ObjectMap<IntList> columnCombinationsById = this.invert(columnCombinationIds);

            // Build and execute the appropriate Flink job.
            this.newInds = new ArrayList<>();
            DataSet<Tuple2<Integer, int[]>> indSets = this.buildNaryIndDetectionPlan(columnCombinationIds).project(0, 2);
            AbstractSindy.AddCommandFactory<Tuple2<Integer, int[]>> addNaryIndCommandFactory =
                    indSet ->
                            (Runnable) () -> {
                                int dependentId = indSet.f0;
                                for (int referencedId : indSet.f1) {
                                    Andy.this.collectInd(dependentId, referencedId, columnCombinationsById, indCandidates);
                                }
                            };
            jobName = String.format("SINDY on %d tables (%d-ary, %s)", this.inputFiles.size(), newArity, new Date());
            this.collectAsync(addNaryIndCommandFactory, indSets, jobName);

            // Collect the number of distinct values and null values per column combination.
            result = this.getJobMeasurements().get(newArity - 1).getFlinkResults();
            Int2IntOpenHashMap encodedNullValueCounts = result.getAccumulatorResult(NullValueCounter.DEFAULT_KEY);
            Object2IntMap<IntList> nullValueCounts = new Object2IntOpenHashMap<>(simpleNullValueCounts.size());
            for (ObjectIterator<Int2IntMap.Entry> iter = encodedNullValueCounts.int2IntEntrySet().fastIterator(); iter.hasNext(); ) {
                Int2IntMap.Entry entry = iter.next();
                int columnCombinationId = entry.getIntKey();
                IntList columnCombination = sorted(columnCombinationsById.get(columnCombinationId));
                nullValueCounts.put(columnCombination, entry.getIntValue());
            }
            nullValueCountsByArity.add(newArity - 1, nullValueCounts);

            Int2IntOpenHashMap encodedDistinctValueCounts = result.getAccumulatorResult(DistinctValueCounter.DEFAULT_KEY);
            Object2IntMap<IntList> distinctValueCounts = new Object2IntOpenHashMap<>(simpleDistinctValueCounts.size());
            for (ObjectIterator<Int2IntMap.Entry> iter = encodedDistinctValueCounts.int2IntEntrySet().fastIterator(); iter.hasNext(); ) {
                Int2IntMap.Entry entry = iter.next();
                int columnCombinationId = entry.getIntKey();
                IntList columnCombination = columnCombinationsById.get(columnCombinationId);
                distinctValueCounts.put(columnCombination, entry.getIntValue());
            }
            distinctValueCountsByArity.add(newArity - 1, distinctValueCounts);


            // Go over the INDs, determine column combination metadata, and determine IND ARs.
            Object2IntMap<IntList> prevDistinctValueCounts = distinctValueCountsByArity.get(newArity - 2);
            Object2IntMap<IntList> curDistinctValueCounts = distinctValueCountsByArity.get(newArity - 1);
            Object2IntMap<IntList> prevNullValueCounts = nullValueCountsByArity.get(newArity - 2);
            Object2IntMap<IntList> curNullValueCounts = nullValueCountsByArity.get(newArity - 1);
            for (Iterator<IND> iter = this.newInds.iterator(); iter.hasNext(); ) {
                IND newInd = iter.next();

                // First check, whether the new IND is void.
                IntList sortedDepColumnIds = toSortedIntList(newInd.getDependentColumns());
                int numDepDistinctValues = curDistinctValueCounts.getInt(sortedDepColumnIds);
                if (numDepDistinctValues == 0) continue;

                // Otherwise, check for possible IND ARs.
                boolean isIarEmbedded = false;
                IntList sortedRefColumnIds = toSortedIntList(newInd.getReferencedColumns());
                int numRefDistinctValues = curDistinctValueCounts.getInt(sortedRefColumnIds);
                int numRefNullValues = curNullValueCounts.getInt(sortedRefColumnIds);
                for (int i = 0; i < newInd.getArity(); i++) {
                    IND generalization = newInd.coproject(i);
                    IntList sortedGenRefColumnIds = toSortedIntList(generalization.getReferencedColumns());
                    int numGenRefDistinctValues = prevDistinctValueCounts.getInt(sortedGenRefColumnIds);
                    int numGenRefNullValues = prevNullValueCounts.getInt(sortedGenRefColumnIds);
                    if (numRefDistinctValues == numGenRefDistinctValues && numRefNullValues == numGenRefNullValues) {
                        IndAugmentationRule iar = new IndAugmentationRule(generalization, newInd.project(i));
                        this.augmentationRules.add(iar);
                        System.out.printf("Discovered %s.\n", iar);
                        isIarEmbedded = true;
                    }
                }

                if (isIarEmbedded) iter.remove();
            }

            // Consolidate the newly discovered INDs with the existing INDs.
            this.candidateGenerator.consolidate(this.allInds, this.newInds);
            this.allInds.addAll(this.newInds);

            // Prepare for the next iteration.
            newArity++;
        }
    }

    /**
     * Returns a sorted version of the given {@link IntList}.
     * @param intList that should be sorted
     * @return the given {@link IntList} if it was sorted already; otherwise a sorted copy of the same
     */
    private static final IntList sorted(IntList intList) {
        // First, test if the list is already sorted.
        boolean isSorted = true;
        for (int i = 1; i < intList.size(); i++) {
            if (intList.getInt(i - 1) > intList.getInt(i)) {
                isSorted = false;
                break;
            }
        }
        if (isSorted) return intList;

        // Otherwise, sort.
        int[] vals = intList.toIntArray();
        Arrays.sort(vals);
        return IntArrayList.wrap(vals);
    }

    /**
     * Returns a sorted {@link IntList} of the given {@code int} array.
     * @param intArray that should be sorted and converted
     * @return the given {@link IntList} if it was sorted already; otherwise a sorted copy of the same
     */
    private static final IntList toSortedIntList(int[] intArray) {
        Arrays.sort(intArray);
        return IntArrayList.wrap(intArray);
    }

    /**
     * Generates n-ary {@link IND} candidates based on a set of known {@link IND}.
     *
     * @param knownInds {@link IND}s that have been found since the last candidate generation (TODO: revise?)
     * @return the generated {@link IND} candidates
     */
    private Set<IND> generateCandidates(Collection<IND> knownInds) {
        Map<IndSubspaceKey, SortedSet<IND>> groupedInds = INDs.groupIntoSubspaces(knownInds, this.columnBitMask);
        final Set<IND> indCandidates = new HashSet<>();
        for (Map.Entry<IndSubspaceKey, SortedSet<IND>> entry : groupedInds.entrySet()) {
            int oldIndCandidatesSize = indCandidates.size();
            this.candidateGenerator.generate(
                    entry.getValue(), entry.getKey(),
                    this.naryIndRestrictions,
                    null, // TODO
                    this.maxArity,
                    indCandidates
            );
            this.logger.debug("Generated {} candidates for {}.", indCandidates.size() - oldIndCandidatesSize, entry.getKey());
        }
        this.logger.info("Generated {} IND candidates.", indCandidates.size());
        if (this.logger.isDebugEnabled()) {
            List<IND> temp = new ArrayList<>(indCandidates);
            temp.sort(INDs.COMPARATOR);
            for (IND candidate : temp) {
                this.logger.debug("-> Candidate {}", candidate);
            }
        }
        return indCandidates;
    }

    protected void cleanUp() throws Exception {
        RemoteCollectorImpl.shutdownAll();
    }


    /**
     * Collects a unary {@link IND}.
     *
     * @param dependentId  is the ID of the dependent column
     * @param referencedId is the ID of the referenced column
     */
    protected void collectUnaryInd(final int dependentId, final int referencedId) {
        this.collectInd(dependentId, referencedId, null, null);
    }

    /**
     * Adds an {@link IND}. If the {@link IND} is not rejected for any reason, it will be given to the
     * {@link #indCollector} (if any) and added to the {@link #newInds}.
     *
     * @param dependentId            is either the ID of the dependent column or a column combination ID
     * @param referencedId           is either the ID of the referenced column or a column combination ID
     * @param columnCombinationsById is {@code null} if a unary IND is added or a map that assigns column combinations to IDs.
     * @param indCandidates          is {@code null} or a set of IND candidates
     */
    protected void collectInd(final int dependentId, final int referencedId,
                              Int2ObjectMap<IntList> columnCombinationsById,
                              final Set<IND> indCandidates) {

        // Resolve the column combination IDs.
        IND ind;

        if (columnCombinationsById == null) {
            // We have a unary IND.
            ind = new IND(dependentId, referencedId);
        } else {
            // We have an n-ary IND.
            IntList dependentColumns = columnCombinationsById.get(dependentId);
            IntList referencedColumns = columnCombinationsById.get(referencedId);
            ind = new IND(dependentColumns.toIntArray(), referencedColumns.toIntArray());
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

        this.collectInd(ind);
    }

    /**
     * Collect an {@link IND}. The {@link IND} will be given to the {@link #indCollector} (if any) and added to the
     * {@link #newInds}.
     *
     * @param ind the {@link IND}
     */
    private void collectInd(IND ind) {
        if (this.indCollector != null) this.indCollector.accept(ind);
        this.newInds.add(ind);

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Discovered {} (might be non-maximal).", ind);
        }

        if (++this.numDiscoveredInds % 1000000 == 0) {
            this.logger.info("{} INDs added so far.", this.numDiscoveredInds);
        }
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
    public Collection<IND> getConsolidatedINDs() {
        return this.allInds;
    }

}