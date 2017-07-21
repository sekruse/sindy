package de.hpi.isg.sindy.metanome;

import au.com.bytecode.opencsv.CSVParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import de.hpi.isg.sindy.core.Andy;
import de.hpi.isg.sindy.core.Sindy;
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
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.*;

/**
 * Metanome interface for the {@link Sindy} algorithm.
 */
public class ANDY implements InclusionDependencyAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, BooleanParameterAlgorithm, FileInputParameterAlgorithm {

    private FileInputGenerator[] fileInputGenerators;

    private InclusionDependencyResultReceiver resultReceiver;

    /**
     * @see Sindy#maxColumns
     */
    @MetanomeProperty
    private final int maxColumns = -1;

    /**
     * @see Sindy#sampleRows
     */
    @MetanomeProperty
    protected int sampleRows = -1;

    /**
     * @see Sindy#isDropNulls
     */
    @MetanomeProperty
    private final boolean isDropNulls = true;

    /**
     * @see Sindy#isNotUseGroupOperators
     */
    @MetanomeProperty
    private boolean isNotUseGroupOperators;

    /**
     * @see Sindy#candidateGenerator
     */
    @MetanomeProperty
    private final String candidateGenerator = "binder";

    /**
     * @see Sindy#maxArity
     */
    @MetanomeProperty
    private final int maxArity = -1;

    /**
     * @see de.hpi.isg.sindy.core.AbstractSindy#naryIndRestrictions
     */
    @MetanomeProperty
    protected String naryIndRestrictions = NaryIndRestrictions.NO_REPETITIONS.name();

    /**
     * The number of bits used to encode columns.
     */
    @MetanomeProperty
    private final int numColumnBits = 16;

    /**
     * The parallelism to use for Flink jobs. {@code -1} indicates default parallelism.
     */
    @MetanomeProperty
    private final int parallelism = -1;

    /**
     * An optional {@code host:port} specification of a remote Flink master.
     */
    @MetanomeProperty
    private String flinkMaster;

    /**
     * An optional Flink configuration file.
     */
    @MetanomeProperty
    private String flinkConfig;


    /**
     * Keeps track of the configuration of this algorithm.
     */
    private MetanomePropertyLedger propertyLedger;

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
        return null;
    }

    private MetanomePropertyLedger getPropertyLedger() {
        if (this.propertyLedger == null) {
            try {
                this.propertyLedger = MetanomePropertyLedger.createFor(this);
            } catch (AlgorithmConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return this.propertyLedger;
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        // Index the input files.
        Int2ObjectMap<ANDY.Table> indexedInputTables = indexTables(this.fileInputGenerators, this.numColumnBits);
        Int2ObjectMap<String> indexedInputFiles = new Int2ObjectOpenHashMap<>();
        for (Map.Entry<Integer, ANDY.Table> entry : indexedInputTables.entrySet()) {
            indexedInputFiles.put(entry.getKey(), entry.getValue().url);
        }

        // Set up Flink.
        ExecutionEnvironment executionEnvironment = this.createExecutionEnvironment();

        // Configure Sindy.
        Andy andy = new Andy(indexedInputFiles, this.numColumnBits, executionEnvironment, ind -> {
        });
        switch (this.candidateGenerator) {
            case "mind":
            case "apriori":
                andy.setExcludeVoidIndsFromCandidateGeneration(false);
                break;
            case "binder":
                andy.setExcludeVoidIndsFromCandidateGeneration(true);
                break;
            default:
                throw new AlgorithmExecutionException(String.format("Unknown candidate generator: %s", this.candidateGenerator));
        }
        andy.setMaxArity(this.maxArity);
        if (this.fileInputGenerators[0] instanceof DefaultFileInputGenerator) {
            DefaultFileInputGenerator fileInputGenerator = (DefaultFileInputGenerator) this.fileInputGenerators[0];
            ConfigurationSettingFileInput setting = fileInputGenerator.getSetting();
            andy.setFieldSeparator(setting.getSeparatorAsChar());
            andy.setQuoteChar(setting.getQuoteCharAsChar());
            andy.setEscapeChar(setting.getEscapeCharAsChar());
            andy.setNullString(setting.getNullValue());
            andy.setDropDifferingLines(setting.isSkipDifferingLines());
            andy.setIgnoreLeadingWhiteSpace(setting.isIgnoreLeadingWhiteSpace());
            andy.setUseStrictQuotes(setting.isStrictQuotes());
        } else {
            System.err.println("Could not read CSV settings from Metanome configuration.");
        }
        andy.setDropNulls(this.isDropNulls);
        andy.setSampleRows(this.sampleRows);
        andy.setMaxColumns(this.maxColumns);
        andy.setNotUseGroupOperators(this.isNotUseGroupOperators);
        andy.setOnlyCountInds(false);
        for (NaryIndRestrictions indRestrictions : NaryIndRestrictions.values()) {
            if (this.naryIndRestrictions.equalsIgnoreCase(indRestrictions.name())) {
                andy.setNaryIndRestrictions(indRestrictions);
                break;
            }
        }

        // Run Andy.
        andy.run();

        // Translate the INDs.
        int columnBitMask = -1 >>> (Integer.SIZE - this.numColumnBits);
        for (IND ind : andy.getConsolidatedINDs()) {
            InclusionDependency inclusionDependency = this.translate(ind, indexedInputTables, columnBitMask);
            this.resultReceiver.receiveResult(inclusionDependency);
        }

        // Print the IARs, so that they are not completely lost.
        System.out.println("IND augmentation rules:");
        for (IndAugmentationRule iar : andy.getAugmentationRules()) {
            System.out.println(this.format(iar, indexedInputTables, columnBitMask));
        }
        System.out.println("END");
    }

    /**
     * Create a {@link ExecutionEnvironment} according to the configuration of this instance.
     *
     * @return the readily configured {@link ExecutionEnvironment}
     */
    private ExecutionEnvironment createExecutionEnvironment() throws AlgorithmExecutionException {
        // Load a config if any.
        Configuration flinkConfiguration = this.flinkConfig != null
                ? parseTypeSafeConfig(new File(this.flinkConfig))
                : new Configuration();
        if (this.parallelism != -1)
            flinkConfiguration.setInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, this.parallelism);

        // Create a default or a remote execution environment.
        ExecutionEnvironment executionEnvironment;
        if (this.flinkMaster == null) {
            executionEnvironment = ExecutionEnvironment.createLocalEnvironment(flinkConfiguration);
        } else {
            String[] hostAndPort = this.flinkMaster.split(":");
            executionEnvironment = ExecutionEnvironment.createRemoteEnvironment(
                    hostAndPort[0],
                    Integer.parseInt(hostAndPort[1]),
                    flinkConfiguration,
                    collectContainingJars(this.getClass(), IntCollection.class, CSVParser.class)
            );
        }

        if (this.parallelism != -1) executionEnvironment.setParallelism(this.parallelism);

        return executionEnvironment;
    }

    /**
     * Collects the JAR files that the given {@code classes} reside in.
     *
     * @param classes {@link Class}es
     * @return an array of paths to the JAR files
     * @throws AlgorithmExecutionException if any of the {@link Class}es does not reside in a JAR file
     */
    private static String[] collectContainingJars(Class<?>... classes) throws AlgorithmExecutionException {
        Set<String> jars = new HashSet<>();
        for (Class<?> cls : classes) {
            String file = cls.getProtectionDomain().getCodeSource().getLocation().getFile();
            if (!file.endsWith(".jar")) {
                throw new AlgorithmExecutionException(String.format(
                        "%s does not reside in a JAR file (but in %s).", cls, file
                ));
            }
            jars.add(file);
        }
        return jars.toArray(new String[jars.size()]);
    }

    /**
     * Parse a Typesafe {@link Config} file.
     *
     * @param configFile the {@link File} to parse
     * @return a Flink {@link Configuration}
     */
    private static Configuration parseTypeSafeConfig(File configFile) {
        Configuration flinkConfiguration = new Configuration();
        Config typesafeConfig = ConfigFactory.parseFile(configFile);
        for (Map.Entry<String, ConfigValue> entry : typesafeConfig.entrySet()) {
            String key = entry.getKey();
            ConfigValue value = entry.getValue();
            switch (value.valueType()) {
                case BOOLEAN:
                    flinkConfiguration.setBoolean(key, (Boolean) value.unwrapped());
                    break;
                case NUMBER:
                    Number number = (Number) value.unwrapped();
                    if (number instanceof Float) {
                        flinkConfiguration.setFloat(key, number.floatValue());
                    } else if (number instanceof Double) {
                        flinkConfiguration.setDouble(key, number.doubleValue());
                    } else if (number instanceof Long) {
                        flinkConfiguration.setLong(key, number.longValue());
                    } else {
                        flinkConfiguration.setInteger(key, number.intValue());
                    }
                    break;
                case STRING:
                    flinkConfiguration.setString(key, (String) value.unwrapped());
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                            "Unsupported value type '%s' for key '%s'.",
                            value.valueType(), key
                    ));
            }
        }
        return flinkConfiguration;
    }

    /**
     * Translates an {@link IND} to a {@link InclusionDependency}.
     *
     * @param ind                that should be translated
     * @param indexedInputTables the indexed tables
     * @param columnBitMask      marks the column bits in the column IDs
     * @return the {@link InclusionDependency}
     */
    private InclusionDependency translate(IND ind, Int2ObjectMap<ANDY.Table> indexedInputTables, int columnBitMask) {
        InclusionDependency inclusionDependency;
        if (ind.getArity() == 0) {
            inclusionDependency = new InclusionDependency(
                    new ColumnPermutation(), new ColumnPermutation()
            );
        } else {
            int depMinColumnId = ind.getDependentColumns()[0] & ~columnBitMask;
            int depTableId = depMinColumnId | columnBitMask;
            ANDY.Table depTable = indexedInputTables.get(depTableId);
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
            ANDY.Table refTable = indexedInputTables.get(refTableId);
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
    private String format(IndAugmentationRule iar, Int2ObjectMap<ANDY.Table> indexedInputTables, int columnBitMask) {
        InclusionDependency lhs = this.translate(iar.getLhs(), indexedInputTables, columnBitMask);
        InclusionDependency rhs = this.translate(iar.getRhs(), indexedInputTables, columnBitMask);
        return String.format("%s => %s", lhs, rhs);
    }

    /**
     * Create proper table indices as required by {@link Sindy} and also retrieve table and column names.
     *
     * @param inputGenerators that should be indexed
     * @param numColumnBits   the number of column bits in the table IDs; e.g. use 16 to share bits evenly among tables and columns
     * @return the indexed table descriptions
     */
    public static Int2ObjectMap<ANDY.Table> indexTables(FileInputGenerator[] inputGenerators, int numColumnBits) {
        Int2ObjectMap<ANDY.Table> index = new Int2ObjectOpenHashMap<>();
        int bitmask = -1 >>> (Integer.SIZE - numColumnBits);
        int tableIdDelta = bitmask + 1;
        int tableId = bitmask;
        for (FileInputGenerator inputGenerator : inputGenerators) {
            try {
                RelationalInput input = inputGenerator.generateNewCopy();
                ANDY.Table table = new ANDY.Table(
                        inputGenerator.getInputFile().getAbsoluteFile().toURI().toString(),
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

    @Override
    public String getAuthors() {
        return "Sebastian Kruse";
    }

    @Override
    public String getDescription() {
        return "This inclusion dependency algorithm uses Flink to find both unary and n-ary INDs.";
    }

    @Override
    public void setFileInputConfigurationValue(String identifier, FileInputGenerator... values) throws AlgorithmConfigurationException {
        if ("inputFiles".equalsIgnoreCase(identifier)) {
            this.fileInputGenerators = values;
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

    private static final class Table {

        final String url, name;
        final List<String> columnNames;

        public Table(String url, String name, List<String> columnNames) {
            this.url = url;
            this.name = name;
            this.columnNames = columnNames;
        }
    }
}
