package de.hpi.isg.sindy.mdms;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.apps.MdmsAppTemplate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.InclusionDependency;
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter;
import de.hpi.isg.mdms.flink.location.AbstractCsvLocation;
import de.hpi.isg.mdms.flink.location.CsvFileLocation;
import de.hpi.isg.mdms.flink.parameters.FlinkParameters;
import de.hpi.isg.mdms.flink.readwrite.RemoteCollectorImpl;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Schema;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.searchspace.AprioriCandidateGenerator;
import de.hpi.isg.sindy.searchspace.CandidateGenerator;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.util.IND;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.Serializable;
import java.util.Date;

/**
 * This class implements a distributed IND algorithm that makes use of De Marchi's IND algorithm.
 *
 * @author Sebastian Kruse
 */
public class SINDY extends MdmsAppTemplate<SINDY.Parameters> {

    /**
     * The schema, within which the INDs should be looked up.
     */
    private Schema schema;

    /**
     * Makes INDs readable.
     */
    protected DependencyPrettyPrinter prettyPrinter;

    private Sindy sindy;

    public static void main(final String[] args) throws Exception {
        SINDY.Parameters parameters = new SINDY.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new SINDY(parameters).run();
    }

    /**
     * Creates a new instance.
     *
     * @param parameters define what to do
     */
    public SINDY(final SINDY.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();

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

        // Load and index the tables to be profiled.
        Int2ObjectMap<String> indexedInputFiles = new Int2ObjectOpenHashMap<>();
        AbstractCsvLocation csvFileLocation = null;
        for (Table table : this.schema.getTables()) {
            Location location = table.getLocation();
            if (location instanceof CsvFileLocation) {
                csvFileLocation = ((CsvFileLocation) location);
                indexedInputFiles.put(table.getId(), csvFileLocation.getPath().toString());
            } else {
                throw new IllegalArgumentException(String.format("Cannot load %s from %s.", table, location));
            }
        }
        if (indexedInputFiles.isEmpty()) {
            throw new IllegalArgumentException("No tables to be profiled.");
        }
        assert csvFileLocation != null;

        // Set up Flink.
        ExecutionEnvironment executionEnvironment = this.getFlinkParameters().createExecutionEnvironment();

        // Create the algorithm.
        this.sindy = new Sindy(
                indexedInputFiles,
                this.metadataStore.getIdUtils().getNumColumnBits(),
                executionEnvironment,
                ind -> {
                } // Discard INDs and collect only the final, consolidated INDs.
        );
        this.sindy.setMaxArity(this.parameters.maxN);
        this.sindy.setOnlyCountInds(this.parameters.isCountOnly);
        this.sindy.setNaryIndRestrictions(this.parameters.getNaryIndRestrictions());
        this.parameters.configureCandidateGenerator(this.sindy);
        this.sindy.setNotUseGroupOperators(this.getBasicSindyParameters().isNotUseGroupOperators);
        this.sindy.setMaxColumns(this.getBasicSindyParameters().maxColumns);
        this.sindy.setSampleRows(this.getBasicSindyParameters().sampleRows);

        // TODO: Use per-file encoding settings as before.
        this.sindy.setFieldSeparator(csvFileLocation.getFieldSeparator());
        this.sindy.setQuoteChar(csvFileLocation.getQuoteChar());
        this.sindy.setNullString(csvFileLocation.getNullString());
        this.sindy.setDropNulls(this.getBasicSindyParameters().isDropNulls);

        this.prettyPrinter = new DependencyPrettyPrinter(this.metadataStore);
    }

    @Override
    protected void executeAppLogic() throws Exception {
        this.sindy.run();

        if (this.sindy.isOnlyCountInds()) {
            System.out.printf("Counted %,d INDs.\n", this.sindy.getNumDiscoveredInds());

        } else if (this.parameters.isDryRun) {
            System.out.printf("Discovered INDs:\n");
            for (IND ind : this.sindy.getConsolidatedINDs()) {
                InclusionDependency inclusionDependency = new InclusionDependency(ind.getDependentColumns(), ind.getReferencedColumns());
                System.out.printf("* %s\n", this.prettyPrinter.prettyPrint(inclusionDependency));
            }

        } else {
            ConstraintCollection<InclusionDependency> constraintCollection = this.metadataStore.createConstraintCollection(
                    String.format("INDs from SINDY (%s)", new Date()),
                    this.metadataStore.createExperiment(
                            String.format("SINDY on %s", this.schema.getName()),
                            this.metadataStore.createAlgorithm(this.getClass().getCanonicalName())
                    ),
                    InclusionDependency.class,
                    this.schema
            );
            for (IND ind : this.sindy.getConsolidatedINDs()) {
                constraintCollection.add(new InclusionDependency(ind.getDependentColumns(), ind.getReferencedColumns()));
            }

            this.metadataStore.flush();
        }
    }

    @Override
    protected boolean isCleanUpRequested() {
        return true;
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

    private FlinkParameters getFlinkParameters() {
        return this.parameters.flinkParamters;
    }

    private BasicParameters getBasicSindyParameters() {
        return this.parameters.basicSindyParameters;
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

        public void configureCandidateGenerator(Sindy sindy) {
            switch (this.candidateGenerator) {
                case "apriori":
                case "mind":
                    sindy.setExcludeVoidIndsFromCandidateGeneration(false);
                    break;
                case "binder":
                    sindy.setExcludeVoidIndsFromCandidateGeneration(true);
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                            "Unknown candidate generator strategy: \"%s\".", this.candidateGenerator
                    ));
            }
        }

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
        public final BasicParameters basicSindyParameters = new BasicParameters();

        @Parameter(names = {"--nary-restrictions"},
                description = "restricts n-ary edge case INDs")
        public String naryIndRestrictions = NaryIndRestrictions.NO_REPETITIONS.name();

        public NaryIndRestrictions getNaryIndRestrictions() {
            for (NaryIndRestrictions naryIndRestrictions : NaryIndRestrictions.values()) {
                if (naryIndRestrictions.name().equalsIgnoreCase(this.naryIndRestrictions)) {
                    return naryIndRestrictions;
                }
            }
            throw new IllegalStateException("Illegal n-ary IND restriction type: " + this.naryIndRestrictions);
        }

    }

}