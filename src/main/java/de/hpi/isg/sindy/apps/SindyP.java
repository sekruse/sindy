package de.hpi.isg.sindy.apps;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.parameters.JCommanderParser;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.domain.constraints.*;
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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple4;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.*;

/**
 * This class implements a distributed partial IND algorithm. That is, in contrast to
 * {@link Sindy}, it detects the overlap of columns rather than exact INDs.
 *
 * @author Sebastian Kruse
 */
public class SindyP extends AbstractSindy<SindyP.Parameters> {

    /**
     * The schema, within which the INDs should be looked up.
     */
    private Schema schema;

    /**
     * Stores the constraints discovered by the algorithm.
     */
    private ConstraintCollection dvoConstraintCollection, dvcConstraintCollection;

    /**
     * Counts the collected INDs.
     */
    private int numAddedDvos;

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

    public static void main(final String[] args) throws Exception {
        SindyP.Parameters parameters = new SindyP.Parameters();
        JCommanderParser.parseCommandLineAndExitOnError(parameters, args);
        new SindyP(parameters).run();
    }

    /**
     * Creates a new instance.
     *
     * @param parameters define what to do
     */
    public SindyP(final SindyP.Parameters parameters) {
        super(parameters);
    }

    @Override
    protected void prepareAppLogic() throws Exception {
        super.prepareAppLogic();


        if (this.parameters.maxN != 1) {
            this.getLogger().warn("N-ary partial IND discovery is not yet supported.");
            this.parameters.maxN = 1;
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
        if (this.parameters.isDryRun) {
            this.dvoConstraintCollection = new InMemoryConstraintCollection(this.metadataStore, this.schema);
            this.dvcConstraintCollection = new InMemoryConstraintCollection(this.metadataStore, this.schema);
        } else {
            this.dvoConstraintCollection = this.metadataStore.createConstraintCollection(
                    String.format("DVOs for %s (SINDY-P, %s)",
                            this.schema.getName(), DateFormat.getInstance().format(new Date())
                    ),
                    this.schema
            );
            if (this.parameters.isStoreDVC) {
                this.dvcConstraintCollection = this.metadataStore.createConstraintCollection(
                        String.format("DVCs for %s (SINDY-P, %s)",
                                this.schema.getName(), DateFormat.getInstance().format(new Date())
                        ),
                        this.schema
                );
            }
        }
        this.numAddedDvos = 0;

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
        DataSet<Tuple4<Integer, Integer, int[], int[]>> overlaps = this.buildUnaryOverlapDetectionPlan(this.schema.getTables());
        AbstractSindy.AddCommandFactory<Tuple4<Integer, Integer, int[], int[]>> handleResultCommandFactory =
                overlapSet ->
                        (Runnable) (() -> {
                            // Handle the distinct value count.
                            int dependentId = overlapSet.f0;
                            int dependentCount = overlapSet.f1;
                            SindyP.this.addDistinctValueCount(dependentId, dependentCount);

                            // Handle the overlaps.
                            for (int i = 0; i < overlapSet.f2.length; i++) {
                                int referencedId = overlapSet.f2[i];
                                int overlap = overlapSet.f3[i];
                                SindyP.this.addDistinctValueOverlap(dependentId, referencedId, overlap);
                            }
                        });
        String jobName = String.format("SINDY-P on %s (%s)", this.schema.getName(), new Date());
        this.collectAsync(handleResultCommandFactory, overlaps, jobName);

        if (this.parameters.isDryRun) {
            this.printDistinctValueOverlaps();
        } else {
            this.metadataStore.flush();
        }
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
    private void printDistinctValueOverlaps() {
        Collection<Constraint> constraints = this.dvoConstraintCollection.getConstraints();
        for (Constraint constraint : constraints) {
            System.out.println(this.prettyPrinter.prettyPrint((DistinctValueOverlap) constraint));
        }
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
     * Adds a {@link DistinctValueCount} to the {@link #dvcConstraintCollection}.
     *
     * @param columnId           is the ID of the column
     * @param distinctValueCount number of  distinct values in the column
     */
    protected void addDistinctValueCount(final int columnId, int distinctValueCount) {
        if (this.dvcConstraintCollection != null) {
            DistinctValueCount.buildAndAddToCollection(
                    new SingleTargetReference(columnId),
                    this.dvcConstraintCollection,
                    distinctValueCount
            );
        }
    }

    /**
     * Adds a {@link DistinctValueOverlap} to the {@link #dvoConstraintCollection}.
     *
     * @param dependentId  is the ID of the dependent column
     * @param referencedId is the ID of the referenced column
     * @param overlap      number of shared distinct values of the two columns
     */
    protected void addDistinctValueOverlap(final int dependentId, final int referencedId, int overlap) {
        DistinctValueOverlap dvo = DistinctValueOverlap.buildAndAddToCollection(
                overlap,
                new DistinctValueOverlap.Reference(dependentId, referencedId),
                this.dvoConstraintCollection
        );

        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Discovered {} (might be non-maximal).", this.prettyPrinter.prettyPrint(dvo));
        }

        if (++this.numAddedDvos % 1000000 == 0) {
            this.getLogger().info("{} INDs added so far.", this.numAddedDvos);
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
                description = MetadataStoreParameters.SCHEMA_ID_DESCRIPTION)
        public Integer schemaId;

        @Parameter(names = {MetadataStoreParameters.SCHEMA_NAME},
                description = MetadataStoreParameters.SCHEMA_NAME_DESCRIPTION)
        public String schemaName;

        @Parameter(names = {"--max-n"}, description = "search at max n-ary INDs")
        public int maxN = 1;

        @Parameter(names = "--nary-gen", description = "n-ary IND candidate generation strategy")
        public String candidateGenerator = "apriori";

        @Parameter(names = "--store-dvc", description = "store the distinct value counts of each column")
        public boolean isStoreDVC;

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