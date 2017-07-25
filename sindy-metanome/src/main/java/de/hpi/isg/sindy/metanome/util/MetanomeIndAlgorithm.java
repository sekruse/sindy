package de.hpi.isg.sindy.metanome.util;

import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.sindy.core.AbstractSindy;
import de.hpi.isg.sindy.core.Andy;
import de.hpi.isg.sindy.metanome.properties.MetanomeProperty;
import de.hpi.isg.sindy.metanome.properties.MetanomePropertyLedger;
import de.hpi.isg.sindy.searchspace.IndAugmentationRule;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.util.IND;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementFileInput;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.cli.ExperimentParameterAlgorithm;
import de.metanome.cli.HdfsInputGenerator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.*;

/**
 * Metanome interface for our IND algorithms.
 */
public abstract class MetanomeIndAlgorithm implements InclusionDependencyAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, BooleanParameterAlgorithm,
        ExperimentParameterAlgorithm, RelationalInputParameterAlgorithm {

    protected List<RelationalInputGenerator> inputGenerators = new ArrayList<>();

    protected InclusionDependencyResultReceiver resultReceiver;

    /**
     * @see AbstractSindy#maxColumns
     */
    @MetanomeProperty
    protected int maxColumns = -1;

    /**
     * @see AbstractSindy#sampleRows
     */
    @MetanomeProperty
    protected int sampleRows = -1;

    /**
     * @see AbstractSindy#isDropNulls
     */
    @MetanomeProperty
    protected boolean isDropNulls = true;

    /**
     * @see AbstractSindy#isNotUseGroupOperators
     */
    @MetanomeProperty
    protected boolean isNotUseGroupOperators;

    /**
     * @see AbstractSindy#maxArity
     */
    @MetanomeProperty
    protected int maxArity = -1;

    /**
     * @see AbstractSindy#naryIndRestrictions
     */
    @MetanomeProperty
    protected String naryIndRestrictions = NaryIndRestrictions.NO_REPETITIONS.name();

    /**
     * The number of bits used to encode columns.
     */
    @MetanomeProperty
    protected int numColumnBits = 16;

    /**
     * The parallelism to use for Flink jobs. {@code -1} indicates default parallelism.
     */
    @MetanomeProperty
    protected int parallelism = -1;

    /**
     * An optional {@code host:port} specification of a remote Flink master.
     */
    @MetanomeProperty
    protected String flinkMaster;

    /**
     * An optional Flink configuration file.
     */
    @MetanomeProperty
    protected String flinkConfig;


    /**
     * Keeps track of the configuration of this algorithm.
     */
    protected MetanomePropertyLedger propertyLedger;

    /**
     * Optional {@link Experiment} to store experimental data on.
     */
    protected Experiment experiment;

    @Override
    public void setResultReceiver(InclusionDependencyResultReceiver inclusionDependencyResultReceiver) {
        this.resultReceiver = inclusionDependencyResultReceiver;
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> configurationRequirements = new ArrayList<>();
        this.getPropertyLedger().contributeConfigurationRequirements(configurationRequirements);
        ConfigurationRequirementFileInput inputFiles = new ConfigurationRequirementFileInput("inputFiles");
        inputFiles.setRequired(true);
        configurationRequirements.add(inputFiles);
        return configurationRequirements;
    }

    protected MetanomePropertyLedger getPropertyLedger() {
        if (this.propertyLedger == null) {
            try {
                this.propertyLedger = MetanomePropertyLedger.createFor(this);
            } catch (AlgorithmConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return this.propertyLedger;
    }

    /**
     * Detects the {@link ConfigurationSettingFileInput} specified by Metanome for the current input.
     * <p>TODO: Check whether the {@link ConfigurationSettingFileInput} are the same across all inputs.</p>
     *
     * @return the {@link ConfigurationSettingFileInput}
     * @throws AlgorithmConfigurationException if no {@link ConfigurationSettingFileInput} could be determined
     */
    protected ConfigurationSettingFileInput getConfigurationSettingFileInput() throws AlgorithmConfigurationException {
        if (this.inputGenerators.isEmpty()) {
            throw new AlgorithmConfigurationException("Could not determine CSV settings: No inputs.");
        }
        RelationalInputGenerator inputGenerator = this.inputGenerators.get(0);
        if (inputGenerator instanceof DefaultFileInputGenerator) {
            return ((DefaultFileInputGenerator) inputGenerator).getSetting();
        } else if (inputGenerator instanceof HdfsInputGenerator) {
            return ((HdfsInputGenerator) inputGenerator).getSettings();
        }
        throw new AlgorithmConfigurationException("Could not determine CSV settings: Unknown input generator type.");
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        // Index the input files.
        Int2ObjectMap<MetanomeIndAlgorithm.Table> indexedInputTables = indexTables(this.inputGenerators, this.numColumnBits);
        Int2ObjectMap<String> indexedInputFiles = new Int2ObjectOpenHashMap<>();
        for (Map.Entry<Integer, MetanomeIndAlgorithm.Table> entry : indexedInputTables.entrySet()) {
            indexedInputFiles.put(entry.getKey(), entry.getValue().url);
        }

        // Set up Flink.
        ExecutionEnvironment executionEnvironment = FlinkUtils.createExecutionEnvironment(
                this.flinkMaster, this.parallelism, this.flinkConfig
        );
        this.execute(indexedInputTables, indexedInputFiles, executionEnvironment);
    }

    /**
     * Carry out the actual execution of the algorithm.
     */
    abstract protected void execute(Int2ObjectMap<Table> indexedInputTables,
                                    Int2ObjectMap<String> indexedInputFiles,
                                    ExecutionEnvironment executionEnvironment)
            throws AlgorithmExecutionException;

    /**
     * Translates an {@link IND} to a {@link InclusionDependency}.
     *
     * @param ind                that should be translated
     * @param indexedInputTables the indexed tables
     * @param columnBitMask      marks the column bits in the column IDs
     * @return the {@link InclusionDependency}
     */
    protected InclusionDependency translate(IND ind, Int2ObjectMap<MetanomeIndAlgorithm.Table> indexedInputTables, int columnBitMask) {
        InclusionDependency inclusionDependency;
        if (ind.getArity() == 0) {
            inclusionDependency = new InclusionDependency(
                    new ColumnPermutation(), new ColumnPermutation()
            );
        } else {
            int depMinColumnId = ind.getDependentColumns()[0] & ~columnBitMask;
            int depTableId = depMinColumnId | columnBitMask;
            MetanomeIndAlgorithm.Table depTable = indexedInputTables.get(depTableId);
            ColumnPermutation dep = new ColumnPermutation();
            for (int depColumnId : ind.getDependentColumns()) {
                int columnIndex = depColumnId - depMinColumnId;
                dep.getColumnIdentifiers().add(new ColumnIdentifier(
                        depTable.name,
                        depTable.columnNames.get(columnIndex)
                ));
            }

            int refMinColumnId = ind.getReferencedColumns()[0] & ~columnBitMask;
            int refTableId = refMinColumnId | columnBitMask;
            MetanomeIndAlgorithm.Table refTable = indexedInputTables.get(refTableId);
            ColumnPermutation ref = new ColumnPermutation();
            for (int refColumnId : ind.getReferencedColumns()) {
                int columnIndex = refColumnId - refMinColumnId;
                ref.getColumnIdentifiers().add(new ColumnIdentifier(
                        refTable.name,
                        refTable.columnNames.get(columnIndex)
                ));
            }
            inclusionDependency = new InclusionDependency(dep, ref);
        }
        return inclusionDependency;
    }

    /**
     * Formats an {@link IndAugmentationRule}.
     *
     * @param iar                that should be translated
     * @param indexedInputTables the indexed tables
     * @param columnBitMask      marks the column bits in the column IDs
     * @return the formatted {@link String}
     */
    protected String format(IndAugmentationRule iar, Int2ObjectMap<MetanomeIndAlgorithm.Table> indexedInputTables, int columnBitMask) {
        InclusionDependency lhs = this.translate(iar.getLhs(), indexedInputTables, columnBitMask);
        InclusionDependency rhs = this.translate(iar.getRhs(), indexedInputTables, columnBitMask);
        return String.format("%s => %s", lhs, rhs);
    }

    /**
     * Create proper table indices as required by {@link Andy} and also retrieve table and column names.
     *
     * @param inputGenerators that should be indexed
     * @param numColumnBits   the number of column bits in the table IDs; e.g. use 16 to share bits evenly among tables and columns
     * @return the indexed table descriptions
     */
    protected static Int2ObjectMap<MetanomeIndAlgorithm.Table> indexTables(Collection<RelationalInputGenerator> inputGenerators, int numColumnBits) {
        Int2ObjectMap<MetanomeIndAlgorithm.Table> index = new Int2ObjectOpenHashMap<>();
        int bitmask = -1 >>> (Integer.SIZE - numColumnBits);
        int tableIdDelta = bitmask + 1;
        int tableId = bitmask;
        for (RelationalInputGenerator inputGenerator : inputGenerators) {
            try {
                RelationalInput input = inputGenerator.generateNewCopy();
                MetanomeIndAlgorithm.Table table = new MetanomeIndAlgorithm.Table(
                        getUrl(inputGenerator),
                        input.relationName(),
                        input.columnNames()
                );
                index.put(tableId, table);
                tableId += tableIdDelta;
            } catch (InputGenerationException | AlgorithmConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return index;
    }

    /**
     * Extract an URL for a {@link RelationalInputGenerator}.
     *
     * @param relationalInputGenerator the {@link RelationalInputGenerator}
     * @return the URL
     * @throws IllegalArgumentException if the {@link RelationalInputGenerator} cannot be represented by a URL
     */
    protected static String getUrl(RelationalInputGenerator relationalInputGenerator) {
        if (relationalInputGenerator instanceof FileInputGenerator) {
            return ((FileInputGenerator) relationalInputGenerator).getInputFile().getAbsoluteFile().toURI().toString();
        }
        if (relationalInputGenerator instanceof HdfsInputGenerator) {
            return ((HdfsInputGenerator) relationalInputGenerator).getUrl();
        }
        throw new IllegalArgumentException(String.format("Cannot extract URL from %s.", relationalInputGenerator));
    }

    @Override
    public String getAuthors() {
        return "Sebastian Kruse";
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
        if ("inputFiles".equalsIgnoreCase(identifier)) {
            this.inputGenerators.addAll(Arrays.asList(values));
        } else {
            throw new AlgorithmConfigurationException("Unknown file input configuration.");
        }
    }

    @Override
    public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
        this.getPropertyLedger().configure(this, identifier, (Object[]) values);
    }

    @Override
    public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
        this.getPropertyLedger().configure(this, identifier, (Object[]) values);
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
        this.getPropertyLedger().configure(this, identifier, (Object[]) values);
    }

    @Override
    public void setProfileDBExperiment(Experiment experiment) throws AlgorithmConfigurationException {
        this.experiment = experiment;
    }

    /**
     * Describes a table that is being profiled.
     */
    protected static final class Table {

        final String url, name;
        final List<String> columnNames;

        public Table(String url, String name, List<String> columnNames) {
            this.url = url;
            this.name = name;
            this.columnNames = columnNames;
        }
    }
}
