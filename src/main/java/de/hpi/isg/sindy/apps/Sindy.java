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
import de.hpi.isg.mdms.util.CollectionUtils;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import it.unimi.dsi.fastutil.ints.*;
import org.apache.commons.lang3.Validate;
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
     * The restrictions to apply during n-ary IND search.
     */
    protected NaryIndRestrictions naryIndRestrictions;

    /**
     * Makes INDs readable.
     */
    protected DependencyPrettyPrinter prettyPrinter;

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

        this.naryIndRestrictions = this.getBasicSindyParameters().getNaryIndRestriction();

        if (this.parameters.isCountOnly && this.parameters.maxN != 1) {
            this.getLogger().warn("Counting only, will process only unary INDs.");
            this.parameters.maxN = 1;
        }
        Validate.isTrue(this.parameters.maxN == 1, "Implementation currently allows for unary IND discovery only.");


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

        if (this.getBasicSindyParameters().isVerbose) {
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
            this.getLogger().info("Discovered {} unary INDs.", numInds);
        } else {
            AbstractSindy.AddCommandFactory<Tuple2<Integer, int[]>> addUnaryIndCommandFactory =
                    indSet ->
                            (Runnable) (() -> {
                                for (int referencedId : indSet.f1) {
                                    int dependentId = indSet.f0;
                                    Sindy.this.addInclusionDependency(dependentId, referencedId);
                                }
                            });
            String jobName = String.format("Sindy on %s (%s)", this.schema.getName(), new Date());
            this.collectAsync(addUnaryIndCommandFactory, unaryInds, jobName);
        }

        if (this.getBasicSindyParameters().isVerbose && !this.parameters.isCountOnly) {
            this.printInclusionDependencies();
            this.printIndsAsCsv();
        }

        this.metadataStore.flush();
    }


    /**
     * Prints the inclusion dependencies grouped by the dependent attribute.
     */
    private void printInclusionDependencies() {
        if (this.getLogger().isDebugEnabled()) {
            Map<String, SortedSet<String>> textualInds = new HashMap<>();
            for (Constraint constraint : this.constraintCollection.getConstraints()) {
                InclusionDependency ind = (InclusionDependency) constraint;
                InclusionDependency.Reference targetReference = ind.getTargetReference();
                String dependentAttributes = Arrays.toString(targetReference.getDependentColumns());
                String referencedAttributes = Arrays.toString(targetReference.getReferencedColumns());
                CollectionUtils.putIntoSortedSet(textualInds, dependentAttributes, referencedAttributes);
            }

            for (Map.Entry<String, SortedSet<String>> entry : textualInds.entrySet()) {
                this.getLogger().debug("{} < {}", entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Print the number of found INDs as CSV.
     */
    private void printIndsAsCsv() {
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
        this.getLogger().info("Checking alleged {}", ind);

        // Check if the IND is also a valid candidate.
        if (indCandidates != null && !indCandidates.contains(ind)) {
            if (this.getLogger().isDebugEnabled()) {
                this.getLogger().debug("Reject pseudo-IND {}.", ind);
            }
            return;
        }


        this.constraintCollection.add(ind);
        if (this.parameters.isDryRun) {
            System.out.println(this.prettyPrinter.prettyPrint(ind));
        } else {

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


    }

}