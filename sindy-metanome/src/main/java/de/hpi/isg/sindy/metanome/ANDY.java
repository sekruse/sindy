package de.hpi.isg.sindy.metanome;

import de.hpi.isg.sindy.core.Andy;
import de.hpi.isg.sindy.metanome.util.MetanomeIndAlgorithm;
import de.hpi.isg.sindy.searchspace.IndAugmentationRule;
import de.hpi.isg.sindy.searchspace.NaryIndRestrictions;
import de.hpi.isg.sindy.util.IND;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.cli.HdfsInputGenerator;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Metanome interface for the {@link Andy} algorithm.
 */
public class ANDY extends MetanomeIndAlgorithm {

    @Override
    protected void execute(Int2ObjectMap<Table> indexedInputTables, Int2ObjectMap<String> indexedInputFiles, ExecutionEnvironment executionEnvironment) throws CouldNotReceiveResultException, ColumnNameMismatchException {
        // Configure Andy.
        Andy andy = new Andy(indexedInputFiles, this.numColumnBits, executionEnvironment, ind -> {
        });
        andy.setMaxArity(this.maxArity);
        if (!this.inputGenerators.isEmpty()) {
            RelationalInputGenerator inputGenerator = this.inputGenerators.get(0);
            ConfigurationSettingFileInput fileInputSettings = null;
            if (inputGenerator instanceof DefaultFileInputGenerator) {
                fileInputSettings = ((DefaultFileInputGenerator) inputGenerator).getSetting();
            } else if (inputGenerator instanceof HdfsInputGenerator) {
                fileInputSettings = ((HdfsInputGenerator) inputGenerator).getSettings();
            }

            if (fileInputSettings != null) {
                andy.setFieldSeparator(fileInputSettings.getSeparatorAsChar());
                andy.setQuoteChar(fileInputSettings.getQuoteCharAsChar());
                andy.setEscapeChar(fileInputSettings.getEscapeCharAsChar());
                andy.setNullString(fileInputSettings.getNullValue());
                andy.setDropDifferingLines(fileInputSettings.isSkipDifferingLines());
                andy.setIgnoreLeadingWhiteSpace(fileInputSettings.isIgnoreLeadingWhiteSpace());
                andy.setUseStrictQuotes(fileInputSettings.isStrictQuotes());
            } else {
                System.err.println("Could not read CSV settings from Metanome configuration.");
            }
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
        andy.setExperiment(this.experiment);

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

    @Override
    public String getDescription() {
        return "This inclusion dependency algorithm uses Flink to find both unary and n-ary INDs. " +
                "It uses an apriori-like candidate generation for that purpose and additionally introduces the " +
                "concept of IND augmentation rules that explain how basic INDs can be inflated to INDs of higher " +
                "arity. Unfortunately, the IND augmentation rules cannot be passed to Metanome, so this algorithm " +
                "will only output only INDs. These are not necessarily maximal INDs because they might be augmentable " +
                "via an IND augmentation rule.";
    }
}
