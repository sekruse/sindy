package de.hpi.isg.sindy.metanome;

import de.hpi.isg.sindy.core.Sindy;
import de.hpi.isg.sindy.metanome.properties.MetanomeProperty;
import de.hpi.isg.sindy.metanome.util.MetanomeIndAlgorithm;
import de.hpi.isg.sindy.searchspace.AprioriCandidateGenerator;
import de.hpi.isg.sindy.searchspace.BinderCandidateGenerator;
import de.hpi.isg.sindy.util.IND;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
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
        this.applyBasicConfiguration(sindy);
        switch (this.candidateGenerator) {
            case "mind":
            case "apriori":
                sindy.setExcludeVoidIndsFromCandidateGeneration(false);
                sindy.setCandidateGenerator(new AprioriCandidateGenerator());
                break;
            case "apriori-no-void":
            case "mind-no-void":
                sindy.setExcludeVoidIndsFromCandidateGeneration(true);
                sindy.setCandidateGenerator(new AprioriCandidateGenerator());
                break;
            case "binder":
                sindy.setExcludeVoidIndsFromCandidateGeneration(true);
                sindy.setCandidateGenerator(new BinderCandidateGenerator());
                break;
            default:
                throw new AlgorithmExecutionException(String.format("Unknown candidate generator: %s", this.candidateGenerator));
        }

        try {
            // Run Sindy.
            sindy.run();
        } finally {
            // Try to rescue any INDs in case of a crash, too.
            try {
                // Translate the INDs.
                int columnBitMask = -1 >>> (Integer.SIZE - this.numColumnBits);
                for (IND ind : sindy.getConsolidatedINDs()) {
                    InclusionDependency inclusionDependency = this.translate(ind, indexedInputTables, columnBitMask);
                    this.resultReceiver.receiveResult(inclusionDependency);
                }
            } catch (Throwable t) {
                logger.error("Could not write the results.", t);
            }
        }
    }


    @Override
    public String getDescription() {
        return "This inclusion dependency algorithm uses Flink to find both unary and n-ary INDs. " +
                "It uses an apriori-like candidate generation for that purpose.";
    }

}
