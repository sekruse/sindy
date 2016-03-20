package de.hpi.isg.sindy.apps;

import com.beust.jcommander.Parameter;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.flink.apps.FlinkAppTemplate;
import de.hpi.isg.mdms.flink.data.Tuple;
import de.hpi.isg.mdms.flink.location.CsvFileLocation;
import de.hpi.isg.mdms.flink.readwrite.RemoteCollectorImpl;
import de.hpi.isg.mdms.flink.util.PlanBuildingUtils;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.hpi.isg.mdms.util.CollectionUtils;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.udf.*;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.lang3.Validate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This class gather basic functionalities used by SINDY and its derivates.
 */
public abstract class AbstractSindy<TParameters> extends FlinkAppTemplate<TParameters> {

    /**
     * Creates a new instance.
     *
     * @param parameters parameters for the app
     */
    public AbstractSindy(TParameters parameters) {
        super(parameters);
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
     * Return the {@link AbstractSindy.Parameters} extracted from the command-line
     *
     * @return the {@link AbstractSindy.Parameters} extracted from the command-line
     */
    abstract AbstractSindy.Parameters getBasicSindyParameters();


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
        this.getLogger().debug("Shutting down dependency collector.");
        executorService.shutdown();
        this.getLogger().debug("Awaiting termination of IND store executor.");
        executorService.awaitTermination(365, TimeUnit.DAYS);
        this.getLogger().debug("Shutting down RemoteCollectorImpl.");
        RemoteCollectorImpl.shutdownAll();
    }

    /**
     * Builds a Flink plan to detect unary INDs.
     *
     * @param tables among that INDs are to be detected
     * @return a {@link DataSet} with the unary INDs (grouped by the dependent column)
     */
    protected DataSet<Tuple3<Integer, Integer, int[]>> buildUnaryIndDetectionPlan(Collection<Table> tables) {
        // Read the sources.
        DataSet<Tuple2<Integer, String>> source = PlanBuildingUtils.buildCellDataSet(this.executionEnvironment,
                tables, this.metadataStore, !this.getBasicSindyParameters().isSuppressEmptyValues);

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
     * @param tables among that overlaps are to be detected
     * @return a {@link DataSet} with the unary INDs (grouped by the dependent column)
     */
    protected DataSet<Tuple4<Integer, Integer, int[], int[]>> buildUnaryOverlapDetectionPlan(Collection<Table> tables) {
        // Read the sources.
        DataSet<Tuple2<Integer, String>> source = PlanBuildingUtils.buildCellDataSet(this.executionEnvironment,
                tables, this.metadataStore, !this.getBasicSindyParameters().isSuppressEmptyValues);

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
     * @param schema               in which the columns reside
     * @return a {@link DataSet} with n-ary INDs (grouped by dependent column combination)
     */
    protected DataSet<Tuple3<Integer, Integer, int[]>> buildNaryIndDetectionPlan(Object2IntMap<IntList> columnCombinationIds, Schema schema) {
        // Find all relevant files, i.e., those that contain a column combination of the IND candidates.
        final Set<String> relevantFiles = new HashSet<>();
        final Set<Table> relevantTables = new HashSet<>();
        for (final IntList columnCombination : columnCombinationIds.keySet()) {
            int tableId = this.metadataStore.getIdUtils().getTableId(columnCombination.getInt(0));
            Table table = schema.getTableById(tableId);
            relevantFiles.add(this.getCsvFilePath(table).toString());
            relevantTables.add(table);
        }
        final Object2IntMap<String> pathIds = this.collectPathIds(schema);
        pathIds.keySet().retainAll(relevantFiles);

        // Configure the input format.
        DataSet<Tuple> tupleDataSet = PlanBuildingUtils.buildTupleDataSet(
                this.executionEnvironment, relevantTables, this.metadataStore, this.getBasicSindyParameters().isSuppressEmptyValues);

        // Split the lines into pivot elements.
        final SplitFieldsToCombinations splitFields = new SplitFieldsToCombinations(
                columnCombinationIds, this.metadataStore.getIdUtils(), this.getBasicSindyParameters().isDropNulls);
        final DataSet<Tuple2<Integer, String>> pivotElements = tupleDataSet.flatMap(splitFields);

        // For each distinct value, find the set of attributes that share this value.
        final DataSet<int[]> attributeGroups = this.createAttributeGroups(pivotElements);

        // Create IND candidates based on each attribute group.
        final DataSet<Tuple3<Integer, Integer, int[]>> inclusionLists = this.createInclusionLists(attributeGroups);

        // Intersect all IND candidates with the same dependent column.
        return this.intersectCandidates(inclusionLists);
    }

    /**
     * Finds the {@link Path} for a {@link Table}, which must reside within a CSV file.
     *
     * @param table whose {@link Path} should be determined
     * @return the {@link Path}
     */
    protected Path getCsvFilePath(Table table) {
        final Location location = table.getLocation();
        Validate.isInstanceOf(CsvFileLocation.class, location, "%s is not located in a CSV file as required.", table);
        return ((CsvFileLocation) location).getPath();
    }


    /**
     * Groups all pivot elements by their value and creates the set of all attributes that contain that values. The
     * value is discarded.
     */
    private DataSet<int[]> createAttributeGroups(final DataSet<Tuple2<Integer, String>> pivotElements) {
        final DataSet<int[]> attributeGroups;
        if (!this.getBasicSindyParameters().isNotUseGroupOperators) {
            attributeGroups = pivotElements
                    .map(new CreateCells())
                    .groupBy(0)
                    .reduceGroup(new GroupMergeCells())
                    .map(new ExtractAttributeGroupsFromCells());
        } else {
            attributeGroups = pivotElements
                    .groupBy(1)
                    .reduceGroup(new UnionAttributes());
        }

        return attributeGroups;
    }

    /**
     * Creates IND candidates based on the given attribute groups.
     */
    private DataSet<Tuple3<Integer, Integer, int[]>> createInclusionLists(final DataSet<int[]> attributeGroups) {
        return attributeGroups.flatMap(new InclusionListCreator());
    }

    /**
     * Intersects the referenced columns of inclusion lists with the same dependent column.
     */
    private DataSet<Tuple3<Integer, Integer, int[]>> intersectCandidates(
            final DataSet<Tuple3<Integer, Integer, int[]>> inclusionLists) {
        return inclusionLists
                .groupBy(0)
                .reduceGroup(new GroupIntersectCandidates());
    }

    /**
     * Collects all n-ary INDs and groups them by their dependent column combination.
     *
     * @param constraints is a collection of constraints that contains the {@code n}-ary INDs
     * @param n           is the size of the INDs
     */
    protected Map<IntList, List<InclusionDependency>> groupNaryIndsByDccs(Collection<? extends Constraint> constraints,
                                                                          final int n) {

        final Map<IntList, List<InclusionDependency>> indsByDependentColumn = new HashMap<>();
        for (final Constraint constraint : constraints) {
            if (!(constraint instanceof InclusionDependency)) {
                continue;
            }
            final InclusionDependency ind = (InclusionDependency) constraint;
            final int[] dependentColumns = ind.getTargetReference().getDependentColumns();
            if (dependentColumns.length == n) {
                CollectionUtils.putIntoList(indsByDependentColumn, new IntArrayList(dependentColumns), ind);
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
            final Map<IntList, List<InclusionDependency>> indCandidatesByDcc) {

        int columnCombinationId = 0;
        final Object2IntMap<IntList> columnCombinationIds = new Object2IntOpenHashMap<>();
        columnCombinationIds.defaultReturnValue(-1);
        for (final List<InclusionDependency> indCandidates : indCandidatesByDcc.values()) {
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
    protected Object2IntMap<IntList> createIdsForColumnCombinations2(Collection<InclusionDependency> indCandidates) {
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
    private int createIdsForColumnCombinations2(int nextId, Object2IntMap<IntList> columnCombinationIds, Collection<InclusionDependency> indCandidates) {
        for (final InclusionDependency indCandidate : indCandidates) {
            IntArrayList columns = new IntArrayList(indCandidate.getTargetReference().getDependentColumns());
            if (!columnCombinationIds.containsKey(columns)) {
                columnCombinationIds.put(columns, nextId);
                nextId++;
            }
            columns = new IntArrayList(indCandidate.getTargetReference().getReferencedColumns());
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
     * Collect minimum {@link Column#getId()} for each {@link Table} in the {@link Schema}.
     *
     * @param schema is the schema whose tables/columns are to be inspected
     * @return a map that assigns to the path of each table the minimum column ID
     */
    private Object2IntMap<String> collectPathIds(final Schema schema) {
        final Object2IntMap<String> pathIds = new Object2IntOpenHashMap<>();
        IdUtils idUtils = this.metadataStore.getIdUtils();

        final int schemaNumber = idUtils.getLocalSchemaId(schema.getId());
        for (final Table table : schema.getTables()) {
            // Skip empty tables.
            if (table.getColumns().isEmpty()) {
                continue;
            }
            final int tableNumber = idUtils.getLocalTableId(table.getId());
            final int minColumnId = idUtils.createGlobalId(schemaNumber, tableNumber, idUtils.getMinColumnNumber());
            pathIds.put(this.getCsvFilePath(table).toString(), minColumnId);
        }
        return pathIds;
    }


    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    public static class Parameters implements Serializable {

        private static final long serialVersionUID = 2936720486536771056L;

        @Parameter(names = {"--max-columns"}, description = "use only the first columns of each file", required = false)
        public int maxColumns = -1;

        @Parameter(names = {"--sample-rows"}, description = "how many percent of rows to take", required = false)
        public int sampleRows = -1;

        @Parameter(names = {"--no-nulls"}, description = "treat values/value combinations with null as non-existent", required = false)
        public boolean isDropNulls = false;

        @Parameter(names = {"--suppress-empty-values"},
                description = "treat empty fields as non-existent",
                required = false)
        public boolean isSuppressEmptyValues;

        @Parameter(names = {"--no-group-operators"},
                description = "use the old operators",
                required = false)
        public boolean isNotUseGroupOperators;

        @Parameter(names = {"--nary-restrictions"},
                description = "restricts n-ary edge case INDs")
        public int naryIndRestriction = NaryIndRestrictions.NO_REPETITIONS.ordinal();

        public NaryIndRestrictions getNaryIndRestriction() {
            for (NaryIndRestrictions naryIndRestrictions : NaryIndRestrictions.values()) {
                if (naryIndRestrictions.ordinal() == this.naryIndRestriction) {
                    return naryIndRestrictions;
                }
            }
            throw new RuntimeException(String.format("Illegal n-ary IND restriction, choose from %s.",
                    NaryIndRestrictions.overviewString()));
        }
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
