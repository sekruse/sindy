package de.hpi.isg.sindy.metanome;

import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.metanome.properties.MetanomeProperty;
import de.hpi.isg.sindy.metanome.util.MetanomeIndAlgorithm;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.util.IND;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.results.InclusionDependency;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Metanome interface for the {@link Sindy} algorithm.
 */
public class SINDY extends MetanomeIndAlgorithm {

    /**
     * @see Sindy#candidateGenerator
     */
    @MetanomeProperty
    private String candidateGenerator = "binder";


    @Override
    protected void execute(Int2ObjectMap<MetanomeIndAlgorithm.Table> indexedInputTables,
                           Int2ObjectMap<String> indexedInputFiles,
                           ExecutionEnvironment executionEnvironment)
            throws AlgorithmExecutionException {

        // Configure Sindy.
        Sindy sindy = new Sindy(indexedInputFiles, this.numColumnBits, executionEnvironment, ind -> {
        });
        switch (this.candidateGenerator) {
            case "mind":
            case "apriori":
                sindy.setExcludeVoidIndsFromCandidateGeneration(false);
                break;
            case "binder":
                sindy.setExcludeVoidIndsFromCandidateGeneration(true);
                break;
            default:
                throw new AlgorithmExecutionException(String.format("Unknown candidate generator: %s", this.candidateGenerator));
        }
        sindy.setMaxArity(this.maxArity);
        ConfigurationSettingFileInput setting = this.getConfigurationSettingFileInput();
        sindy.setFieldSeparator(setting.getSeparatorAsChar());
        sindy.setQuoteChar(setting.getQuoteCharAsChar());
        sindy.setEscapeChar(setting.getEscapeCharAsChar());
        sindy.setNullString(setting.getNullValue());
        sindy.setDropDifferingLines(setting.isSkipDifferingLines());
        sindy.setIgnoreLeadingWhiteSpace(setting.isIgnoreLeadingWhiteSpace());
        sindy.setUseStrictQuotes(setting.isStrictQuotes());
        sindy.setDropNulls(this.isDropNulls);
        sindy.setSampleRows(this.sampleRows);
        sindy.setMaxColumns(this.maxColumns);
        sindy.setNotUseGroupOperators(this.isNotUseGroupOperators);
        sindy.setOnlyCountInds(false);
        for (NaryIndRestrictions indRestrictions : NaryIndRestrictions.values()) {
            if (this.naryIndRestrictions.equalsIgnoreCase(indRestrictions.name())) {
                sindy.setNaryIndRestrictions(indRestrictions);
                break;
            }
        }
        sindy.setExperiment(this.experiment);

        // Run Sindy.
        sindy.run();

        // Translate the INDs.
        int columnBitMask = -1 >>> (Integer.SIZE - this.numColumnBits);
        for (IND ind : sindy.getConsolidatedINDs()) {
            InclusionDependency inclusionDependency = this.translate(ind, indexedInputTables, columnBitMask);
            this.resultReceiver.receiveResult(inclusionDependency);
        }
    }


    @Override
    public String getDescription() {
        return "This inclusion dependency algorithm uses Flink to find both unary and n-ary INDs. " +
                "It uses an apriori-like candidate generation for that purpose.";
    }

}
