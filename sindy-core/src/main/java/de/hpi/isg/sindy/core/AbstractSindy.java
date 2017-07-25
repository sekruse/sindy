package de.hpi.isg.sindy.core;

import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.sindy.data.IntObjectTuple;
import de.hpi.isg.sindy.io.MultiFileTextInputFormat;
import de.hpi.isg.sindy.io.RemoteCollectorImpl;
import de.hpi.isg.sindy.searchspace.AprioriCandidateGenerator;
import de.hpi.isg.sindy.searchspace.CandidateGenerator;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.udf.*;
import de.hpi.isg.sindy.util.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class gather basic functionalities used by SINDY and its derivates.
 */
public abstract class AbstractSindy {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Configuration of the IND search.                                                                               //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Maintains a mapping from table IDs to input file paths. Table IDs are {@code int} values where the
     * first {@code n} bits decode the table and the last {@code 32-n} bits are all set to {@code 1}. Column IDs,
     * in contrast, encode the column index in these last bits.
     */
    protected final Int2ObjectMap<String> inputFiles = new Int2ObjectOpenHashMap<>();

    /**
     * Marks the last bits in column/table IDs that represent the column index.
     *
     * @see #inputFiles
     */
    protected final int columnBitMask;

    /**
     * Maximum number of columns to consider in each table. A value of {@code -1} indicates that no such restriction
     * should be applied.
     */
    protected int maxColumns = -1;

    /**
     * Percentage of rows to sample and compute inclusion dependencies on. A value of {@code -1} indicates that no
     * such restriction applies.
     */
    protected int sampleRows = -1;

    /**
     * Treat values/value combinations with {@code NULL} values as non-existent.
     */
    protected boolean isDropNulls;

    /**
     * Do not use pre-grouping in global aggregations.
     */
    protected boolean isNotUseGroupOperators;

    /**
     * Defines on which edge-case inclusion dependencies should be considered.
     */
    protected NaryIndRestrictions naryIndRestrictions = NaryIndRestrictions.NO_REPETITIONS;

    /**
     * The maximum arity of inclusion dependencies to test. A value of {@code -1} implies no such restriction.
     */
    protected int maxArity = -1;

    /**
     * {@link CandidateGenerator} for n-ary IND discovery.
     */
    protected CandidateGenerator candidateGenerator = new AprioriCandidateGenerator();
    /**
     * Whether to exclude void {@link IND}s where the dependent side does not contain any values from candidate generation.
     * <p>By default {@code false} because it impairs the result completeness when the user is not aware of this restriction.</p>
     */
    protected boolean isExcludeVoidIndsFromCandidateGeneration;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Parsing configuration.                                                                                         //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * CSV field separator.
     */
    private char fieldSeparator = ';';

    /**
     * CSV quote char.
     */
    private char quoteChar;

    /**
     * CSV escape character (used in some dialects).
     */
    private char escapeChar;

    /**
     * Whether to trim (leading) content outside of quotes when parsing CSV rows.
     */
    private boolean isUseStrictQuotes;

    /**
     * Whether to ignore leading whitespace when parsing CSV rows.
     */
    private boolean isIgnoreLeadingWhiteSpace;

    /**
     * Whether to ignore lines that cannot be parsed or do not have the expected number of fields (otherwise fail).
     */
    private boolean isDropDifferingLines;

    /**
     * Representation of {@code NULL} values. If set to {@code null}, then this setting does not apply.
     */
    private String nullString;

    /**
     * The encoding of the CSV files.
     */
    private Encoding encoding = Encoding.DEFAULT_ENCODING;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Flink configuration.                                                                                           //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * A Flink {@link ExecutionEnvironment} to run Flink jobs.
     */
    private final ExecutionEnvironment executionEnvironment;

    /**
     * Keeps track of executed Flink jobs.
     */
    private final List<JobMeasurement> jobMeasurements = new LinkedList<>();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscellaneous configuration.                                                                                   //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Logger.
     */
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Whether inclusion dependencies should only be counted rather than collected.
     */
    protected boolean isOnlyCountInds;

    /**
     * Optional {@link Experiment} to store experimental data.
     */
    protected Experiment experiment;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Code.                                                                                                          //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Creates a new instance.
     *
     * @param inputFiles           input files with their table IDs; see {@link #inputFiles}
     * @param executionEnvironment Flink {@link ExecutionEnvironment} to use
     */
    protected AbstractSindy(Int2ObjectMap<String> inputFiles, int numColumnBits, ExecutionEnvironment executionEnvironment) {
        this.inputFiles.putAll(inputFiles);
        this.columnBitMask = -1 >>> (Integer.SIZE - numColumnBits);
        this.executionEnvironment = executionEnvironment;
    }

    /**
     * Builds a {@link DataSet} that comprises all cells of the given input tables. A cell consists of a
     * column ID a value in that respective column.
     *
     * @param tableIds the IDs of the tables to be considered
     * @return the cell {@link DataSet}
     */
    protected DataSet<IntObjectTuple<String>> buildCellDataSet(IntCollection tableIds) {
        // Build the datasets for the different tables.
        List<DataSet<IntObjectTuple<String>>> tableDataSets = new ArrayList<>();
        Object2IntMap<String> paths2minColumnId = new Object2IntOpenHashMap<>(tableIds.size());
        for (IntIterator i = tableIds.iterator(); i.hasNext(); ) {
            int tableId = i.nextInt();
            String path = this.inputFiles.get(tableId);
            if (path == null) throw new IllegalArgumentException("Unknown table ID: " + tableId);
            int minColumnId = tableId & ~this.columnBitMask;
            paths2minColumnId.put(path, minColumnId);
        }

        // TODO: Check if any of the files has a compression suffix.
        String inputPath = FileUtils.findCommonParent(paths2minColumnId.keySet());
        final MultiFileTextInputFormat.ListBasedFileIdRetriever fileIdRetriever =
                new MultiFileTextInputFormat.ListBasedFileIdRetriever(paths2minColumnId);
        MultiFileTextInputFormat inputFormat;
        inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever, null);
        inputFormat.setEncoding(this.encoding);
        inputFormat.setFilePath(inputPath);
        // TODO: Enable if needed.
//                inputFormat.setRecordDetector(new CsvRecordStateMachine(csvParameters.getFieldSeparatorChar(),
//                        csvParameters.getQuoteChar(), '\n', encoding.getCharset()));
        DataSet<IntObjectTuple<String>> source = this.executionEnvironment
                .createInput(inputFormat)
                .name(String.format("CSV files (%d tables)", paths2minColumnId.size()));

        // Filter rows, if requested.
        if (this.sampleRows > 0 && this.sampleRows < 100) {
            source = source.filter(new SampleRoundRobin<>(this.sampleRows));
        }

        // Split the lines into pivot elements.
        SplitCsvRowsWithOpenCsv splitRowsFunction = new SplitCsvRowsWithOpenCsv(
                this.fieldSeparator,
                this.quoteChar,
                this.escapeChar,
                this.isUseStrictQuotes,
                this.isIgnoreLeadingWhiteSpace,
                this.isDropDifferingLines,
                null,
                this.maxColumns,
                this.nullString,
                this.isDropNulls
        );
        final DataSet<IntObjectTuple<String>> cells = source.flatMap(splitRowsFunction).name("Split rows");
        tableDataSets.add(cells);

        // Union all different datasets.
        return union(tableDataSets, "Union cells");
    }

    /**
     * Builds a {@link DataSet} that comprises all tuples of the given input tables. A tuple consists of the
     * column ID of the first column in the respective table and all values in this tuple, order by the columns.
     *
     * @param tableIds the IDs of the tables to be considered
     * @return the tuple {@link DataSet}
     */
    protected DataSet<IntObjectTuple<String[]>> buildTupleDataSet(IntCollection tableIds) {
        // Build the datasets for the different tables.
        List<DataSet<IntObjectTuple<String[]>>> tableDataSets = new ArrayList<>();
        Object2IntMap<String> paths2minColumnId = new Object2IntOpenHashMap<>(tableIds.size());
        for (IntIterator i = tableIds.iterator(); i.hasNext(); ) {
            int tableId = i.nextInt();
            String path = this.inputFiles.get(tableId);
            if (path == null) throw new IllegalArgumentException("Unknown table ID: " + tableId);
            int minColumnId = tableId & ~this.columnBitMask;
            paths2minColumnId.put(path, minColumnId);
        }

        // TODO: Check if any of the files has a compression suffix.
        String inputPath = FileUtils.findCommonParent(paths2minColumnId.keySet());
        final MultiFileTextInputFormat.ListBasedFileIdRetriever fileIdRetriever =
                new MultiFileTextInputFormat.ListBasedFileIdRetriever(paths2minColumnId);
        MultiFileTextInputFormat inputFormat;
        inputFormat = new MultiFileTextInputFormat(fileIdRetriever, fileIdRetriever, null);
        inputFormat.setEncoding(this.encoding);
        inputFormat.setFilePath(inputPath);
        // TODO: Enable if needed.
//                inputFormat.setRecordDetector(new CsvRecordStateMachine(csvParameters.getFieldSeparatorChar(),
//                        csvParameters.getQuoteChar(), '\n', encoding.getCharset()));
        DataSet<IntObjectTuple<String>> source = this.executionEnvironment
                .createInput(inputFormat)
                .name(String.format("CSV files (%d tables)", paths2minColumnId.size()));

        // Filter rows, if requested.
        if (this.sampleRows > 0 && this.sampleRows < 100) {
            source = source.filter(new SampleRoundRobin<>(this.sampleRows));
        }

        // NB: We don't do column sampling because we omitted respective columns already in the unary IND discovery
        // process. In consequence, n-ary candidates should not contain any of the excluded columns anyway.

        // Transform the CSV data into fields.
        ParseCsvRowsWithOpenCsv parseCsvRows = new ParseCsvRowsWithOpenCsv(
                this.fieldSeparator,
                this.quoteChar,
                this.escapeChar,
                this.isUseStrictQuotes,
                this.isIgnoreLeadingWhiteSpace,
                this.isDropDifferingLines,
                null,
                this.nullString
        );
        final DataSet<IntObjectTuple<String[]>> tuples = source.flatMap(parseCsvRows).name("Parse rows");
        tableDataSets.add(tuples);

        // Union all different datasets.
        return union(tableDataSets, "Union tuples");
    }

    /**
     * Union all given {@link DataSet}s.
     *
     * @param dataSets that should be unioned
     * @param name     name for the union operations
     * @return the union of the {@code dataSets}
     */
    protected static <T> DataSet<T> union(final Collection<DataSet<T>> dataSets, String name) {
        Validate.notNull(dataSets);
        Validate.notEmpty(dataSets);
        Validate.noNullElements(dataSets);

        DataSet<T> unionedDataSets = null;
        for (final DataSet<T> dataSet : dataSets) {
            if (unionedDataSets == null) {
                unionedDataSets = dataSet;
            } else {
                final UnionOperator<T> union = unionedDataSets.union(dataSet);
                if (name != null) {
                    union.name(name);
                }
                unionedDataSets = union;
            }
        }
        return unionedDataSets;
    }


    /**
     * Count the INDs in a given {@link DataSet}. Note that this command causes execution of the Flink job.
     *
     * @param inclusionDependencies is a {@link DataSet} with IND sets
     * @return the number of INDs
     */
    protected int count(DataSet<Tuple2<Integer, int[]>> inclusionDependencies) throws Exception {
        // Count the INDs.
        return inclusionDependencies.map(new AbstractSindy.CountInds()).sum(0).collect().get(0).f0;
    }


    /**
     * Collects INDs asynchronysly from a {@link DataSet}. Causes job execution.
     *
     * @param addCommandFactory creates add-commands for recieved dependencies
     * @param dependencies      is the {@link DataSet} that should be collected
     * @param jobName           is the name of the Flink job that will be executed
     * @throws Exception
     */
    protected <T> void collectAsync(final AbstractSindy.AddCommandFactory<T> addCommandFactory, DataSet<T> dependencies, String jobName) throws Exception {
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        RemoteCollectorImpl.collectLocal(
                dependencies,
                resultElement -> executorService.execute(addCommandFactory.create(resultElement))
        );

        this.executePlan(jobName);
        this.logger.debug("Shutting down dependency collector.");
        executorService.shutdown();
        this.logger.debug("Awaiting termination of IND store executor.");
        executorService.awaitTermination(365, TimeUnit.DAYS);
        this.logger.debug("Shutting down RemoteCollectorImpl.");
        RemoteCollectorImpl.shutdownAll();
    }

    /**
     * Builds a Flink plan to detect unary INDs.
     *
     * @param tables among that INDs are to be detected
     * @return a {@link DataSet} with the unary INDs (grouped by the dependent column)
     */
    protected DataSet<Tuple3<Integer, Integer, int[]>> buildUnaryIndDetectionPlan(IntCollection tables) {
        // Read the sources.
        DataSet<IntObjectTuple<String>> source = this.buildCellDataSet(tables);

        // For each distinct value, find the set of attributes that share this value.
        final DataSet<int[]> attributeGroups = this.createAttributeGroups(source);

        // Create IND candidates based on each attribute group.
        final DataSet<Tuple3<Integer, Integer, int[]>> inclusionLists = this.createInclusionLists(attributeGroups);

        // Intersect all IND candidates with the same dependent column.
        return this.intersectCandidates(inclusionLists);
    }


    /**
     * Builds a Flink plan to detect unary overlaps.
     *
     * @param tableIds among that overlaps are to be detected
     * @return a {@link DataSet} with the unary INDs (grouped by the dependent column)
     */
    protected DataSet<Tuple4<Integer, Integer, int[], int[]>> buildUnaryOverlapDetectionPlan(IntCollection tableIds) {
        // Read the sources.
        DataSet<IntObjectTuple<String>> source = this.buildCellDataSet(tableIds);

        // For each distinct value, find the set of attributes that share this value.
        final DataSet<int[]> attributeGroups = this.createAttributeGroups(source);

        return attributeGroups
                .flatMap(new OverlapListCreator()).name("Create overlap lists")
                .groupBy(0)
                .reduceGroup(new GroupMultiUnionOverlapLists()).name("Multi-union overlap lists");
    }

    /**
     * Creates a Flink plan to verify n-ary IND candidates in a batch.
     *
     * @param columnCombinationIds is a mapping of IDs to column combinations
     * @return a {@link DataSet} with n-ary INDs (grouped by dependent column combination)
     */
    protected DataSet<Tuple3<Integer, Integer, int[]>> buildNaryIndDetectionPlan(Object2IntMap<IntList> columnCombinationIds) {
        // Find all relevant files, i.e., those that contain a column combination of the IND candidates.
        final Set<String> relevantFiles = new HashSet<>();
        final IntSet relevantTables = new IntOpenHashSet();
        for (final IntList columnCombination : columnCombinationIds.keySet()) {
            int tableId = columnCombination.getInt(0) | this.columnBitMask;
            relevantTables.add(tableId);
            relevantFiles.add(this.inputFiles.get(tableId));
        }
        final Object2IntMap<String> pathIds = this.mapPathsToMinColumnId(relevantTables);
        pathIds.keySet().retainAll(relevantFiles);

        // Configure the input format.
        DataSet<IntObjectTuple<String[]>> tupleDataSet = this.buildTupleDataSet(relevantTables);

        // Split the lines into pivot elements.
        final SplitFieldsToCombinations splitFields = new SplitFieldsToCombinations(
                columnCombinationIds, this.columnBitMask, !this.isDropNulls
        );
        final DataSet<IntObjectTuple<String>> pivotElements = tupleDataSet.flatMap(splitFields).name("Create column combinations");

        // For each distinct value, find the set of attributes that share this value.
        final DataSet<int[]> attributeGroups = this.createAttributeGroups(pivotElements);

        // Create IND candidates based on each attribute group.
        final DataSet<Tuple3<Integer, Integer, int[]>> inclusionLists = this.createInclusionLists(attributeGroups);

        // Intersect all IND candidates with the same dependent column.
        return this.intersectCandidates(inclusionLists);
    }

    /**
     * Collect minimum column IDs for each given table.
     *
     * @return a map that assigns to the path of each table the minimum column ID
     */
    private Object2IntMap<String> mapPathsToMinColumnId(final IntCollection tableIds) {
        final Object2IntMap<String> paths2MinColumnId = new Object2IntOpenHashMap<>();

        for (IntIterator i = tableIds.iterator(); i.hasNext(); ) {
            final int tableId = i.nextInt();
            final int minColumnId = tableId & ~this.columnBitMask;
            final String path = this.inputFiles.get(tableId);
            paths2MinColumnId.put(path, minColumnId);
        }
        return paths2MinColumnId;
    }


    /**
     * Groups all pivot elements by their value and creates the set of all attributes that contain that values. The
     * value is discarded.
     */
    private DataSet<int[]> createAttributeGroups(final DataSet<IntObjectTuple<String>> pivotElements) {
        final DataSet<int[]> attributeGroups;
        if (!this.isNotUseGroupOperators) {
            attributeGroups = pivotElements
                    .map(new CreateCells()).name("Prepare cell merging")
                    .groupBy(0)
                    .reduceGroup(new GroupMergeCells()).name("Merge cells")
                    .map(new ExtractAttributeGroupsFromCells()).name("Extract attribute groups");
        } else {
            attributeGroups = pivotElements
                    .groupBy(1)
                    .reduceGroup(new UnionAttributes()).name("Create attribute groups");
        }

        return attributeGroups;
    }

    /**
     * Creates IND candidates based on the given attribute groups.
     */
    private DataSet<Tuple3<Integer, Integer, int[]>> createInclusionLists(final DataSet<int[]> attributeGroups) {
        return attributeGroups.flatMap(new InclusionListCreator()).name("Create IND candidate sets");
    }

    /**
     * Intersects the referenced columns of inclusion lists with the same dependent column.
     */
    private DataSet<Tuple3<Integer, Integer, int[]>> intersectCandidates(
            final DataSet<Tuple3<Integer, Integer, int[]>> inclusionLists) {
        return inclusionLists
                .groupBy(0)
                .reduceGroup(new GroupIntersectCandidates()).name("Intersect IND candidate sets");
    }

    /**
     * Collects all n-ary INDs and groups them by their dependent column combination.
     *
     * @param inds is a collection of constraints that contains the {@code n}-ary INDs
     * @param n    is the size of the INDs
     */
    protected Map<IntList, List<IND>> groupNaryIndsByDccs(Collection<? extends IND> inds,
                                                          final int n) {

        final Map<IntList, List<IND>> indsByDependentColumn = new HashMap<>();
        for (final IND ind : inds) {
            if (ind.getArity() == n) {
                List<IND> indGroup = indsByDependentColumn.computeIfAbsent(
                        new IntArrayList(ind.getDependentColumns()),
                        k -> new ArrayList<>()
                );
                indGroup.add(ind);
            }
        }
        return indsByDependentColumn;
    }

    /**
     * Assigns IDs to all column combinations that are found within the given IND candidates.
     *
     * @param indCandidatesByDcc are the IND candidates
     * @return a map that assigns an ID to each column combination
     */
    protected Object2IntMap<IntList> createIdsForColumnCombinations2(
            final Map<IntList, List<IND>> indCandidatesByDcc) {

        int columnCombinationId = 0;
        final Object2IntMap<IntList> columnCombinationIds = new Object2IntOpenHashMap<>();
        columnCombinationIds.defaultReturnValue(-1);
        for (final List<IND> indCandidates : indCandidatesByDcc.values()) {
            columnCombinationId = this.createIdsForColumnCombinations2(columnCombinationId, columnCombinationIds, indCandidates);
        }
        return columnCombinationIds;
    }

    /**
     * Assigns IDs to all column combinations that are found within the given IND candidates.
     *
     * @param indCandidates are the IND candidates
     * @return a map that assigns an ID to each column combination
     */
    protected Object2IntMap<IntList> createIdsForColumnCombinations2(Collection<IND> indCandidates) {
        final Object2IntMap<IntList> columnCombinationIds = new Object2IntOpenHashMap<>();
        columnCombinationIds.defaultReturnValue(-1);
        this.createIdsForColumnCombinations2(0, columnCombinationIds, indCandidates);
        return columnCombinationIds;
    }

    /**
     * Assigns IDs to all column combinations that are found within the given IND candidates.
     *
     * @param nextId               is the next free ID that can be assigned to a new column combination
     * @param columnCombinationIds is an already existing mapping from column combinations to IDs
     * @param indCandidates        are the IND candidates
     * @return the new next free ID
     */
    private int createIdsForColumnCombinations2(int nextId, Object2IntMap<IntList> columnCombinationIds, Collection<IND> indCandidates) {
        for (final IND indCandidate : indCandidates) {
            IntArrayList columns = new IntArrayList(indCandidate.getDependentColumns());
            if (!columnCombinationIds.containsKey(columns)) {
                columnCombinationIds.put(columns, nextId);
                nextId++;
            }
            columns = new IntArrayList(indCandidate.getReferencedColumns());
            if (!columnCombinationIds.containsKey(columns)) {
                columnCombinationIds.put(columns, nextId);
                nextId++;
            }
        }
        return nextId;
    }

    /**
     * Inverts all key-value pairs in the given map. Requires unique values.
     *
     * @param originalMap is the map that should be inverted
     * @return the inverted map
     */
    protected <T> Int2ObjectMap<T> invert(final Object2IntMap<T> originalMap) {
        final Int2ObjectMap<T> invertedMap = new Int2ObjectOpenHashMap<>();
        for (final Object2IntMap.Entry<T> entry : originalMap.object2IntEntrySet()) {
            invertedMap.put(entry.getIntValue(), entry.getKey());
        }
        return invertedMap;
    }

    /**
     * Executes the plan that was created on the {@link #executionEnvironment} and also prints out some measurement
     * informations. The job measurements will be kept track of in {@link #jobMeasurements}.
     *
     * @param planName is the name of the plan to be executed
     * @throws Exception
     */
    protected void executePlan(final String planName) throws Exception {
        this.logger.info("Execute plan \"{}\".", planName);

        final long startTime = System.currentTimeMillis();
        final JobExecutionResult result = this.executionEnvironment.execute(planName);
        final long endTime = System.currentTimeMillis();
        final JobMeasurement jobMeasurement = new JobMeasurement(planName, startTime, endTime, result);

        this.logger.info("Finished plan {}.", planName);
        this.logger.info("Plan runtime: {} ms (net {} ms)", jobMeasurement.getDuration(), result.getNetRuntime());

        this.jobMeasurements.add(jobMeasurement);
    }

    /**
     * Update the {@link #experiment} with data about the number of columns and tuples from the analyzed dataset.
     *
     * @param result of the Flink job that analyzed a dataset
     */
    protected void updateExperimentWithDatasetSize(JobExecutionResult result) {
        if (this.experiment != null) {
            Int2IntMap tableWidths = result.getAccumulatorResult(TableWidthAccumulator.DEFAULT_KEY);
            Int2LongOpenHashMap tableHeights = result.getAccumulatorResult(TableHeightAccumulator.DEFAULT_KEY);
            if (tableWidths != null && tableHeights != null) {
                int numColumns = 0;
                long numTuples = 0L;
                for (IntIterator iter = tableWidths.values().iterator(); iter.hasNext(); ) {
                    numColumns += iter.nextInt();
                }
                for (LongIterator iter = tableHeights.values().iterator(); iter.hasNext(); ) {
                    numTuples += iter.nextLong();
                }
                DatasetMeasurement datasetMeasurement = new DatasetMeasurement("dataset");
                datasetMeasurement.setNumColumns(numColumns);
                datasetMeasurement.setNumTuples(numTuples);
                this.experiment.addMeasurement(datasetMeasurement);
            }
        }
    }

    public Int2ObjectMap<String> getInputFiles() {
        return this.inputFiles;
    }

    public int getMaxColumns() {
        return this.maxColumns;
    }

    public void setMaxColumns(int maxColumns) {
        this.maxColumns = maxColumns;
    }

    public int getSampleRows() {
        return this.sampleRows;
    }

    public void setSampleRows(int sampleRows) {
        this.sampleRows = sampleRows;
    }

    public boolean isDropNulls() {
        return this.isDropNulls;
    }

    public void setDropNulls(boolean dropNulls) {
        this.isDropNulls = dropNulls;
    }

    public boolean isNotUseGroupOperators() {
        return this.isNotUseGroupOperators;
    }

    public void setNotUseGroupOperators(boolean notUseGroupOperators) {
        this.isNotUseGroupOperators = notUseGroupOperators;
    }

    public NaryIndRestrictions getNaryIndRestrictions() {
        return this.naryIndRestrictions;
    }

    public void setNaryIndRestrictions(NaryIndRestrictions naryIndRestrictions) {
        this.naryIndRestrictions = naryIndRestrictions;
    }

    public int getMaxArity() {
        return this.maxArity;
    }

    public void setMaxArity(int maxArity) {
        this.maxArity = maxArity;
    }

    public CandidateGenerator getCandidateGenerator() {
        return this.candidateGenerator;
    }

    public void setCandidateGenerator(CandidateGenerator candidateGenerator) {
        this.candidateGenerator = candidateGenerator;
    }

    public char getFieldSeparator() {
        return this.fieldSeparator;
    }

    public void setFieldSeparator(char fieldSeparator) {
        this.fieldSeparator = fieldSeparator;
    }

    public char getQuoteChar() {
        return this.quoteChar;
    }

    public void setQuoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
    }

    public String getNullString() {
        return this.nullString;
    }

    public void setNullString(String nullString) {
        this.nullString = nullString;
    }

    public char getEscapeChar() {
        return this.escapeChar;
    }

    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public boolean isUseStrictQuotes() {
        return this.isUseStrictQuotes;
    }

    public void setUseStrictQuotes(boolean useStrictQuotes) {
        this.isUseStrictQuotes = useStrictQuotes;
    }

    public boolean isIgnoreLeadingWhiteSpace() {
        return this.isIgnoreLeadingWhiteSpace;
    }

    public void setIgnoreLeadingWhiteSpace(boolean ignoreLeadingWhiteSpace) {
        this.isIgnoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
    }

    public boolean isDropDifferingLines() {
        return this.isDropDifferingLines;
    }

    public void setDropDifferingLines(boolean dropDifferingLines) {
        this.isDropDifferingLines = dropDifferingLines;
    }

    public Encoding getEncoding() {
        return this.encoding;
    }

    public void setEncoding(Encoding encoding) {
        this.encoding = encoding;
    }

    public ExecutionEnvironment getExecutionEnvironment() {
        return this.executionEnvironment;
    }

    public List<JobMeasurement> getJobMeasurements() {
        return this.jobMeasurements;
    }

    public boolean isOnlyCountInds() {
        return this.isOnlyCountInds;
    }

    public void setOnlyCountInds(boolean onlyCountInds) {
        this.isOnlyCountInds = onlyCountInds;
    }

    /**
     * If you are confused on how to index your input files, you can use this function to do so.
     *
     * @param inputFiles    that should be indexed
     * @param numColumnBits the number of column bits in the table IDs; e.g. use 16 to share bits evenly among tables and columns
     * @return the indexed input files
     */
    public static Int2ObjectMap<String> indexInputFiles(Collection<String> inputFiles, int numColumnBits) {
        Int2ObjectMap<String> index = new Int2ObjectOpenHashMap<>();
        int bitmask = -1 >>> (Integer.SIZE - numColumnBits);
        int tableIdDelta = bitmask + 1;
        int tableId = bitmask;
        for (String inputFile : inputFiles) {
            index.put(tableId, inputFile);
            tableId += tableIdDelta;
        }
        return index;
    }

    public boolean isExcludeVoidIndsFromCandidateGeneration() {
        return this.isExcludeVoidIndsFromCandidateGeneration;
    }

    public void setExcludeVoidIndsFromCandidateGeneration(boolean excludeVoidIndsFromCandidateGeneration) {
        this.isExcludeVoidIndsFromCandidateGeneration = excludeVoidIndsFromCandidateGeneration;
    }

    public Experiment getExperiment() {
        return this.experiment;
    }

    public void setExperiment(Experiment experiment) {
        this.experiment = experiment;
    }

    @SuppressWarnings("serial")
    static class CountInds implements MapFunction<Tuple2<Integer, int[]>, Tuple1<Integer>> {

        private final Tuple1<Integer> outputTuple = new Tuple1<>();

        @Override
        public Tuple1<Integer> map(Tuple2<Integer, int[]> indList) throws Exception {
            this.outputTuple.f0 = indList.f1.length;
            return this.outputTuple;
        }

    }

    /**
     * Factory to create an add-command for certain elements.
     *
     * @param <T> is the type of the elements to be added
     */
    public interface AddCommandFactory<T> {

        /**
         * Create the add-command.
         *
         * @param element for that the add-command should be created
         * @return the add-command
         */
        Runnable create(T element);

    }

}
