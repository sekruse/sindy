package de.hpi.isg.sindy.metanome;

import de.hpi.isg.sindy.core.Sandy;
import de.hpi.isg.sindy.metanome.properties.MetanomeProperty;
import de.hpi.isg.sindy.metanome.util.MetanomeIndAlgorithm;
import de.hpi.isg.sindy.util.PartialIND;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.results.InclusionDependency;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Metanome interface for the {@link Sandy} algorithm.
 */
public class SANDY extends MetanomeIndAlgorithm {

    @MetanomeProperty
    private double maxError = 0.1d;

    {
        this.maxArity = 1;
    }

    @Override
    protected void execute(Int2ObjectMap<Table> indexedInputTables,
                           Int2ObjectMap<String> indexedInputFiles,
                           ExecutionEnvironment executionEnvironment)
            throws AlgorithmExecutionException {

        // Configure Sindy.
        Sandy sandy = new Sandy(indexedInputFiles, this.numColumnBits, executionEnvironment, ind -> {
        });
        this.applyBasicConfiguration(sandy);
        sandy.setMinRelativeOverlap(1d - this.maxError);

        // Run Sandy.
        sandy.run();

        // Translate the INDs.
        int columnBitMask = -1 >>> (Integer.SIZE - this.numColumnBits);
        for (PartialIND pind : sandy.getAllInds()) {
            InclusionDependency inclusionDependency = this.translate(pind, indexedInputTables, columnBitMask);
            this.resultReceiver.receiveResult(inclusionDependency);
        }
    }


    @Override
    public String getDescription() {
        return "This inclusion dependency algorithm uses Flink to find unary partial INDs. " +
                "Metanome does not support partial INDs, though, so they are reported as regular INDs, instead.";
    }

}
