package de.hpi.isg.sindy.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.InMemoryConstraintCollection;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter;
import de.hpi.isg.mdms.flink.parameters.FlinkParameters;
import de.hpi.isg.mdms.flink.readwrite.RemoteCollectorImpl;
import de.hpi.isg.mdms.model.constraints.Constraint;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Column;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.sindy.searchspace.AprioriCandidateGenerator;
import de.hpi.isg.sindy.searchspace.CandidateGenerator;
import de.hpi.isg.sindy.searchspace.IndSubspaceKey;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.util.InclusionDependencies;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.*;

/**
 * This class implements a distributed IND algorithm that makes use of De Marchi's IND algorithm.
 *
 * @author Sebastian Kruse
 */
public class Sindy extends AbstractSindy<Sindy.Parameters> {

    /**
     * The schema, within which the INDs should be looked up.
     */
    private Schema schema;

    /**
     * Stores the constraints discovered by the algorithm.
     */
    private ConstraintCollection constraintCollection;

    /**
     * Counts the collected INDs.
     */
    private int numAddedInds;

    /**
     * The restrictions to apply to n-ary IND discovery.
     */
    protected NaryIndRestrictions naryIndRestrictions;

    /**
     * {@link CandidateGenerator} for n-ary IND discovery.
     */
    protected CandidateGenerator candidateGenerator;

    /**
     * Makes INDs readable.
     */
    protected DependencyPrettyPrinter prettyPrinter;

    private Collection<InclusionDependency> indCollector;

    public static void main(final String[] args) throws Exception {
        Sindy.Parameters parameters = new Sindy.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new Sindy(parameters).run();
    }

    /**
     * Creates a new instance.
     *
     * @param parameters define what to do
     */
    public Sindy(final Sindy.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();


        if (this.parameters.isCountOnly && this.parameters.maxN != 1) {
            this.getLogger().warn("Counting only, will process only unary INDs.");
            this.parameters.maxN = 1;
        }

        if (this.parameters.maxN > 1) {
            this.naryIndRestrictions = this.getBasicSindyParameters().getNaryIndRestriction();
            this.candidateGenerator = this.parameters.getCandidateGenerator();
        }

        // Load the schema that should be profiled.
        this.schema = this.getSchema(this.parameters.schemaId, this.parameters.schemaName);
        if (this.schema == null) {
            this.getLogger().error("Available schemas:");
            for (final Schema availableSchema : this.metadataStore.getSchemas()) {
                this.getLogger().error("{}:\t\"{}\"", availableSchema.getId(), availableSchema.getName());
            }
            throw new IllegalArgumentException(String.format("Could not find a schema for ID %s/name %s.",
                    this.parameters.schemaId, this.parameters.schemaName));
        }

        // Check if we should sample.
        int maxColumns = this.getBasicSindyParameters().maxColumns;
        if (maxColumns > 0) {
            int numUsedColumns = 0;
            for (final Table table : this.schema.getTables()) {
                numUsedColumns += Math.min(maxColumns, table.getColumns().size());
            }
            this.getLogger().info("Column restriction of {} leaves a total of {} columns to be considered.",
                    maxColumns, numUsedColumns);
        }

        // Create the constraint collection.
        if (!this.parameters.isCountOnly) {
            if (this.parameters.isDryRun) {
                this.constraintCollection = new InMemoryConstraintCollection(this.metadataStore, this.schema);
            } else {
                String constraintsDescription = String.format("INDs for %s (SINDY, n=%d, %s)",
                        this.schema.getName(), this.parameters.maxN, DateFormat.getInstance().format(new Date()));
                this.constraintCollection = this.metadataStore.createConstraintCollection(constraintsDescription, this.schema);
            }
        }
        this.numAddedInds = 0;

        if (this.logger.isDebugEnabled()) {
            for (Table table : this.schema.getTables()) {
                this.getLogger().info("Profiling {}->{}", table.getId(), table);
                for (Column column : table.getColumns()) {
                    this.getLogger().info(" with {}->{}", column.getId(), column);
                }
            }
        }

        this.prettyPrinter = new DependencyPrettyPrinter(this.metadataStore);
    }

    @Override
    protected void executeAppLogic() throws Exception {
        // Take care of the unary IND detection.
        DataSet<Tuple2<Integer, int[]>> unaryInds = this.buildUnaryIndDetectionPlan(this.schema.getTables()).project(0, 2);
        if (this.parameters.isCountOnly) {
            int numInds = this.count(unaryInds);
            System.out.printf("Discovered %d unary INDs.\n", numInds);
        } else {
            this.indCollector = new LinkedList<>();
            AbstractSindy.AddCommandFactory<Tuple2<Integer, int[]>> addUnaryIndCommandFactory =
                    indSet ->
                            (Runnable) (() -> {
                                for (int referencedId : indSet.f1) {
                                    int dependentId = indSet.f0;
                                    Sindy.this.addInclusionDependency(dependentId, referencedId);
                                }
                            });
            String jobName = String.format("SINDY on %s (%s)", this.schema.getName(), new Date());
            this.collectAsync(addUnaryIndCommandFactory, unaryInds, jobName);
        }

        Collection<InclusionDependency> allInds = this.indCollector;

        // Now perform n-ary IND detection using the Apriori candidate generation.
        if (this.parameters.maxN > 1) {
            Collection<InclusionDependency> newInds = allInds;
            while (!newInds.isEmpty()) {
                this.logger.info("{} INDs for n-ary IND generation.", newInds.size());
                if (this.logger.isDebugEnabled()) {
                    List<InclusionDependency> temp = new ArrayList<>(newInds);
                    Collections.sort(temp, InclusionDependencies.COMPARATOR);
                    for (InclusionDependency candidate : temp) {
                        this.logger.debug("-> IND {}", candidate);
                    }
                }

                // Generate n-ary IND candidates.
                final Set<InclusionDependency> indCandidates = this.generateCandidates(newInds);
                if (indCandidates.isEmpty()) {
                    break;
                }

                // For any column combination in the n-ary IND candidates, create an ID.
                final Object2IntMap<IntList> columnCombinationIds = this.createIdsForColumnCombinations2(indCandidates);
                final Int2ObjectMap<IntList> columnCombinationsById = this.invert(columnCombinationIds);

                // Build and execute the appropriate Flink job.
                this.indCollector = new LinkedList<>();
                DataSet<Tuple2<Integer, int[]>> indSets = this.buildNaryIndDetectionPlan(columnCombinationIds, this.schema).project(0, 2);
                AbstractSindy.AddCommandFactory<Tuple2<Integer, int[]>> addNaryIndCommandFactory =
                        indSet ->
                                (Runnable) () -> {
                                    for (int referencedId : indSet.f1) {
                                        int dependentId = indSet.f0;
                                        Sindy.this.addInclusionDependency(dependentId, referencedId, columnCombinationsById, indCandidates);
                                    }
                                };
                String jobName = String.format("SINDY (n-ary) on %s", this.schema.getName());
                this.collectAsync(addNaryIndCommandFactory, indSets, jobName);

                // Consolidate the newly discovered INDs with the existing INDs.
                this.candidateGenerator.consolidate(allInds, this.indCollector);
                allInds.addAll(this.indCollector);

                // Prepare for the next iteration.
                newInds = this.indCollector;
                this.indCollector = null;
            }
        }

        if (!this.parameters.isCountOnly) {
            if (this.parameters.isDryRun) {
                this.printInclusionDependencies(allInds);
            } else {
                allInds.forEach(this.constraintCollection::add);
                this.metadataStore.flush();
            }
        }

        this.logStatistics();
    }

    /**
     * Generates n-ary {@link InclusionDependency} candidates based on a set of known {@link InclusionDependency}.
     *
     * @param knownInds {@link InclusionDependency}s that have been found since the last candidate generation (TODO: revise?)
     * @return the generated {@link InclusionDependency} candidates
     */
    private Set<InclusionDependency> generateCandidates(Collection<InclusionDependency> knownInds) {
        Map<IndSubspaceKey, SortedSet<InclusionDependency>> groupedInds = InclusionDependencies.groupIntoSubspaces(
                knownInds, this.metadataStore.getIdUtils()
        );
        final Set<InclusionDependency> indCandidates = new HashSet<>();
        for (Map.Entry<IndSubspaceKey, SortedSet<InclusionDependency>> entry : groupedInds.entrySet()) {
            int oldIndCandidatesSize = indCandidates.size();
            this.candidateGenerator.generate(
                    entry.getValue(), entry.getKey(),
                    this.getBasicSindyParameters().getNaryIndRestriction(),
                    this.parameters.maxN,
                    indCandidates
            );
            this.logger.debug("Generated {} candidates for {}.", indCandidates.size() - oldIndCandidatesSize, entry.getKey());
        }
        this.logger.info("Generated {} IND candidates.", indCandidates.size());
        if (this.logger.isDebugEnabled()) {
            List<InclusionDependency> temp = new ArrayList<>(indCandidates);
            Collections.sort(temp, InclusionDependencies.COMPARATOR);
            for (InclusionDependency candidate : temp) {
                this.logger.debug("-> Candidate {}", candidate);
            }
        }
        return indCandidates;
    }

    /**
     * Prints the inclusion dependencies grouped by the dependent attribute.
     */
    private void printInclusionDependencies(Collection<InclusionDependency> inds) {
        ArrayList<InclusionDependency> sortedInds = new ArrayList<>(inds);
        sortedInds.sort(InclusionDependencies.COMPARATOR);
        for (InclusionDependency ind : sortedInds) {
            System.out.println(this.prettyPrinter.prettyPrint(ind));
        }
    }

    /**
     * Print the number of found INDs as CSV.
     */
    private void logStatistics() {
        // Print the number of INDs on each level.
        this.getLogger().info("IND count");
        this.getLogger().info("dataset;overall;n=1;n=2;...");
        final StringBuilder sb = new StringBuilder();
        // FIXME: This is rather a hotfix for the metadatastore bug.
        Collection<Constraint> foundConstraints;
        foundConstraints = this.constraintCollection.getConstraints();
        sb.append(this.schema.getName()).append(";").append(foundConstraints.size());
        int maxArity = 0;
        Int2IntMap indCounts = new Int2IntOpenHashMap();
        for (Constraint constraint : foundConstraints) {
            InclusionDependency ind = (InclusionDependency) constraint;
            int arity = ind.getArity();
            int numIndsForArity = indCounts.get(arity);
            indCounts.put(arity, numIndsForArity + 1);
            maxArity = Math.max(arity, maxArity);
        }
        for (int arity = 1; arity <= maxArity; arity++) {
            int numIndsForArity = indCounts.get(arity);
            sb.append(";").append(numIndsForArity);
        }
        this.getLogger().info(sb.toString());
    }

    @Override
    protected void cleanUp() throws Exception {
        super.cleanUp();
        RemoteCollectorImpl.shutdownAll();
    }

    @Override
    protected MetadataStoreParameters getMetadataStoreParameters() {
        return this.parameters.metadataStoreParameters;
    }

    @Override
    protected FlinkParameters getFlinkParameters() {
        return this.parameters.flinkParamters;
    }

    @Override
    AbstractSindy.Parameters getBasicSindyParameters() {
        return this.parameters.basicSindyParameters;
    }


    /**
     * Adds an IND to the {@link #constraintCollection}.
     *
     * @param dependentId  is the ID of the dependent column
     * @param referencedId is the ID of the referenced column
     */
    protected void addInclusionDependency(final int dependentId, final int referencedId) {
        this.addInclusionDependency(dependentId, referencedId, null, null);
    }

    /**
     * Adds an IND to the {@link #constraintCollection}.
     *
     * @param dependentId            is either the ID of the dependent column or a column combination ID
     * @param referencedId           is either the ID of the referenced column or a column combination ID
     * @param columnCombinationsById is {@code null} if a unary IND is added or a map that assigns column combinations to IDs.
     * @param indCandidates          is {@code null} or a set of IND candidates
     */
    protected void addInclusionDependency(final int dependentId, final int referencedId,
                                          Int2ObjectMap<IntList> columnCombinationsById,
                                          final Set<InclusionDependency> indCandidates) {

        // Resolve the column combination IDs.
        IntList dependentColumns, referencedColumns;
        if (columnCombinationsById == null) {
            dependentColumns = IntLists.singleton(dependentId);
            referencedColumns = IntLists.singleton(referencedId);
        } else {
            dependentColumns = columnCombinationsById.get(dependentId);
            referencedColumns = columnCombinationsById.get(referencedId);
        }
        if (dependentColumns == null || referencedColumns == null) {
            throw new IllegalStateException(String.format("Could not find the column (combination) with ID %d or %d.",
                    dependentId,
                    referencedId));
        }

        final InclusionDependency.Reference reference = new InclusionDependency.Reference(dependentColumns.toIntArray(), referencedColumns.toIntArray());
        InclusionDependency ind = new InclusionDependency(reference);

        // Check if the IND is also a valid candidate.
        if (indCandidates != null && !indCandidates.contains(ind)) {
            if (this.getLogger().isDebugEnabled()) {
                this.getLogger().debug("Reject pseudo-IND {}.", ind);
            }
            return;
        }


        this.indCollector.add(ind);
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Discovered {} (might be non-maximal).", this.prettyPrinter.prettyPrint(ind));
        }

        if (++this.numAddedInds % 1000000 == 0) {
            this.getLogger().info("{} INDs added so far.", this.numAddedInds);
        }

    }

    /**
     * Parameters for the execution of the surrounding class.
     *
     * @author Sebastian Kruse
     */
    public static class Parameters implements Serializable {

        private static final long serialVersionUID = 2936720486536771056L;

        @Parameter(names = {MetadataStoreParameters.SCHEMA_ID},
                description = MetadataStoreParameters.SCHEMA_ID_DESCRIPTION, required = false)
        public Integer schemaId;

        @Parameter(names = {MetadataStoreParameters.SCHEMA_NAME},
                description = MetadataStoreParameters.SCHEMA_NAME_DESCRIPTION, required = false)
        public String schemaName;

        @Parameter(names = {"--max-n"}, description = "search at max n-ary INDs", required = false)
        public int maxN = 1;

        @Parameter(names = "--nary-gen", description = "n-ary IND candidate generation strategy", required = false)
        public String candidateGenerator = "apriori";

        @Parameter(names = {"--count-only"},
                description = "count INDs within Flink instead of delivering them (only for unary INDs)")
        public boolean isCountOnly;

        @Parameter(names = {"--dry-run"},
                description = "print INDs instead of delivering them")
        public boolean isDryRun;

        @ParametersDelegate
        public final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @ParametersDelegate
        public final FlinkParameters flinkParamters = new FlinkParameters();

        @ParametersDelegate
        public final AbstractSindy.Parameters basicSindyParameters = new AbstractSindy.Parameters();

        public CandidateGenerator getCandidateGenerator() {
            switch (this.candidateGenerator) {
                case "apriori":
                case "mind":
                    return new AprioriCandidateGenerator();
                default:
                    throw new IllegalArgumentException(String.format(
                            "Unknown candidate generator strategy: \"%s\".", this.candidateGenerator
                    ));
            }
        }
    }

}