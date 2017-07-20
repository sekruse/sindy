package de.hpi.isg.sindy.core;

import de.hpi.isg.sindy.io.RemoteCollectorImpl;
import de.hpi.isg.sindy.searchspace.IndSubspaceKey;
import de.hpi.isg.sindy.searchspace.OptimisticCandidateGenerator;
import de.hpi.isg.sindy.searchspace.ZigZagSubspace;
import de.hpi.isg.sindy.searchspace.hypergraph.NaryInd;
import de.hpi.isg.sindy.searchspace.hypergraph.UnaryInd;
import de.hpi.isg.sindy.util.IND;
import de.hpi.isg.sindy.util.INDs;
import de.hpi.isg.sindy.util.TableWidthAccumulator;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
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
public class Sindy extends AbstractSindy implements Runnable {

    /**
     * Counts the collected INDs.
     */
    private int numDiscoveredInds;

    /**
     * Accepts INDs as soon as they are available.
     */
    private Consumer<IND> indCollector;

    /**
     * Keeps track of seen dependent columns or column combinations. This helps to detect "empty" column or column combinations.
     */
    private IntSet seenDependentIds;

    /**
     * Keeps track of columns that do not contain any values (apart from {@code null} values).
     */
    private IntSet emptyColumnIds;

    /**
     * Collects INDs so as to generate candidates of higher arity.
     */
    private Collection<IND> newInds, allInds;

    /**
     * The maximum arity of IND candidates to be checked with a breadth-first search strategy.
     */
    private int maxBfsArity = -1;

    /**
     * Whether to exclude void {@link IND}s where the dependent side does not contain any values from candidate generation.
     * <p>By default {@code false} because it impairs the result completeness when the user is not aware of this restriction.</p>
     */
    private boolean isExcludeVoidIndsFromCandidateGeneration;

    /**
     * Candidate generator to use when proceeding optimistically through the search space by leaping "over" several
     * IND candidates.
     */
    private OptimisticCandidateGenerator<ZigZagSubspace> optimisticCandidateGenerator;

    /**
     * Creates a new instance.
     *
     * @param inputFiles           input files with their table IDs; see {@link #inputFiles}
     * @param numColumnBits        the number of column bits in the table IDs
     * @param executionEnvironment Flink {@link ExecutionEnvironment} to use
     * @param indCollector         collects INDs as soon as they are discovered
     */
    public Sindy(Int2ObjectMap<String> inputFiles, int numColumnBits,
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

        if (this.maxArity >= 1) {
            if (this.maxBfsArity == -1 || this.maxBfsArity > this.maxArity) {
                this.maxBfsArity = this.maxArity;
                this.logger.info("Setting maximum BFS arity to {}.", this.maxBfsArity);
            }
        }

        if (this.isConfiguredForMixedTraversal()) {
            if (this.optimisticCandidateGenerator == null) {
                throw new IllegalStateException(
                        "Should process some levels optimistically, but an optimistic candidate generator has not been configured."
                );
            }
            if (this.columnBitMask == 0x0) {
                throw new IllegalStateException(
                        "Should process some levels optimistically, but a column bit mask has not been configured."
                );
            }
        }

        this.numDiscoveredInds = 0;
    }

    /**
     * Tells whether this instance is configured in such a way that it should traverse the IND search space both using
     * pessimistic and optimistic traversal strategies.
     *
     * @return whether said condition holds
     */
    private boolean isConfiguredForMixedTraversal() {
        return this.maxBfsArity > 1 && (this.maxArity == -1 || this.maxArity > this.maxBfsArity);
    }

    protected void executeRun() throws Exception {
        // Take care of the unary IND detection.
        DataSet<Tuple2<Integer, int[]>> unaryInds = this.buildUnaryIndDetectionPlan(this.inputFiles.keySet()).project(0, 2);
        if (this.isOnlyCountInds) {
            int numInds = this.count(unaryInds);
            this.numDiscoveredInds = numInds;
            this.logger.info("Discovered {} unary INDs.", numInds);
        } else {
            this.newInds = new ArrayList<>();
            this.seenDependentIds = new IntOpenHashSet();
            AbstractSindy.AddCommandFactory<Tuple2<Integer, int[]>> addUnaryIndCommandFactory = indSet ->
                    (Runnable) (() -> {
                        for (int referencedId : indSet.f1) {
                            int dependentId = indSet.f0;
                            Sindy.this.collectUnaryInd(dependentId, referencedId);
                        }
                        this.seenDependentIds.add(indSet.f0);
                    });
            String jobName = String.format("SINDY on %d tables (unary, %s)", this.inputFiles.size(), new Date());
            this.collectAsync(addUnaryIndCommandFactory, unaryInds, jobName);

            // Detect empty columns and add appropriate INDs.
            this.emptyColumnIds = new IntOpenHashSet();
            JobExecutionResult result = this.getJobMeasurements().get(0).getFlinkResults();
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
                if (!this.seenDependentIds.contains(depId)) {
                    this.emptyColumnIds.add(depId);
                    for (IntIterator refIter = allColumnIds.iterator(); refIter.hasNext(); ) {
                        int refId = refIter.nextInt();
                        if (depId != refId) this.collectUnaryInd(depId, refId);
                    }
                }
            }
        }

        // Now perform n-ary IND detection using the Apriori candidate generation.
        this.allInds = this.newInds;
        int newArity = 2;
        while (this.newInds != null && !this.newInds.isEmpty() && (newArity <= this.maxBfsArity || this.maxBfsArity == -1)) {
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
            this.seenDependentIds.clear();
            DataSet<Tuple2<Integer, int[]>> indSets = this.buildNaryIndDetectionPlan(columnCombinationIds).project(0, 2);
            AbstractSindy.AddCommandFactory<Tuple2<Integer, int[]>> addNaryIndCommandFactory =
                    indSet ->
                            (Runnable) () -> {
                                int dependentId = indSet.f0;
                                for (int referencedId : indSet.f1) {
                                    Sindy.this.collectInd(dependentId, referencedId, columnCombinationsById, indCandidates);
                                }
                                this.seenDependentIds.add(dependentId);
                            };
            String jobName = String.format("SINDY on %d tables (%d-ary, %s)", this.inputFiles.size(), newArity, new Date());
            this.collectAsync(addNaryIndCommandFactory, indSets, jobName);

            // Detect empty columns and add appropriate INDs.
            // Index the INDs by their dependent columns.
            Int2ObjectMap<Collection<IND>> indCandidatesByDepId = new Int2ObjectOpenHashMap<>();
            for (IND indCandidate : indCandidates) {
                IntList depColumns = IntArrayList.wrap(indCandidate.getDependentColumns());
                int depId = columnCombinationIds.getInt(depColumns);
                Collection<IND> indCandidatesForDep = indCandidatesByDepId.get(depId);
                if (indCandidatesForDep == null) {
                    indCandidatesForDep = new ArrayList<>();
                    indCandidatesByDepId.put(depId, indCandidatesForDep);
                }
                indCandidatesForDep.add(indCandidate);
            }
            // Create INDs for all unseen dependent column INDs.
            for (Int2ObjectMap.Entry<Collection<IND>> entry : indCandidatesByDepId.int2ObjectEntrySet()) {
                int depId = entry.getIntKey();
                if (!this.seenDependentIds.contains(depId)) {
                    for (IND indCandidate : entry.getValue()) {
                        this.collectInd(indCandidate);
                    }
                }
            }

            // Consolidate the newly discovered INDs with the existing INDs.
            this.candidateGenerator.consolidate(this.allInds, this.newInds);
            this.allInds.addAll(this.newInds);

            // Prepare for the next iteration.
            newArity++;
        }


        // Check if we dropped out of above loop because of the maxBfsArity parameter.
        if (this.maxBfsArity == -1 || this.maxBfsArity >= newArity) return;

        // If so, check if we should proceed further anyway.
        if (this.maxArity != -1 && newArity > this.maxArity) return;

        // Initialize the data structures for optimistic traversal.
        Map<IndSubspaceKey, ZigZagSubspace> indSubspaces = new HashMap<>();
        for (IND ind : this.allInds) {
            // Determine the IND subspace for the IND.
            ZigZagSubspace indSubspace = indSubspaces.computeIfAbsent(
                    IndSubspaceKey.createFromInd(ind, this.columnBitMask),
                    ZigZagSubspace::new
            );
            // Make use of the knowledge that the INDs are already consolidated.
            indSubspace.positiveBorder.add(ind);
            NaryInd naryInd = new NaryInd(ind);
            for (UnaryInd unaryInd : naryInd.getUnaryInds()) {
                indSubspace.unaryInds.add(unaryInd.toNaryInd().toInclusionDependency());
            }
            // TODO: Maintain the stripped negative border.
            throw new RuntimeException("TODO: Maintain the stripped negative border.");
        }
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
                    this.isExcludeVoidIndsFromCandidateGeneration ?
                            ind -> ind.getArity() == 1 && !this.emptyColumnIds.contains(ind.getDependentColumns()[0]) :
                            null,
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
     * Adds an {@link IND}.
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
     * Collect an {@link IND}.
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

    public boolean isExcludeVoidIndsFromCandidateGeneration() {
        return this.isExcludeVoidIndsFromCandidateGeneration;
    }

    public void setExcludeVoidIndsFromCandidateGeneration(boolean excludeVoidIndsFromCandidateGeneration) {
        this.isExcludeVoidIndsFromCandidateGeneration = excludeVoidIndsFromCandidateGeneration;
    }

    public int getMaxBfsArity() {
        return this.maxBfsArity;
    }

    public void setMaxBfsArity(int maxBfsArity) {
        this.maxBfsArity = maxBfsArity;
    }

    public OptimisticCandidateGenerator<ZigZagSubspace> getOptimisticCandidateGenerator() {
        return this.optimisticCandidateGenerator;
    }

    public void setOptimisticCandidateGenerator(OptimisticCandidateGenerator<ZigZagSubspace> optimisticCandidateGenerator) {
        this.optimisticCandidateGenerator = optimisticCandidateGenerator;
    }

}