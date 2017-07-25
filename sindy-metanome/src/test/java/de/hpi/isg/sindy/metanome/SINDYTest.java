package de.hpi.isg.sindy.metanome;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.ColumnPermutation;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

/**
 * Integration test for {@link SINDY}.
 */
public class SINDYTest {

    private static File getFile(String testFileName) {
        try {
            return new File(Thread.currentThread().getContextClassLoader().getResource(testFileName).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static DefaultFileInputGenerator getFileInputGenerator(String testFileName) {
        try {
            return new DefaultFileInputGenerator(
                    getFile(testFileName),
                    new ConfigurationSettingFileInput(
                            testFileName,
                            false,
                            ',',
                            '\0',
                            '\0',
                            true,
                            false,
                            0,
                            false,
                            false,
                            ""
                    )
            );
        } catch (AlgorithmConfigurationException | FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMetanomeIntegration() throws AlgorithmExecutionException, FileNotFoundException {
        SINDY sindy = new SINDY();
        sindy.setRelationalInputConfigurationValue("inputFiles",
                getFileInputGenerator("letters.csv"), getFileInputGenerator("letters-ext.csv")
        );

        ResultCache resultCache = new ResultCache("SINDY", null);
        sindy.setResultReceiver(resultCache);
        sindy.execute();

        Set<InclusionDependency> expectedInds = new HashSet<>();
        expectedInds.add(new InclusionDependency(
                new ColumnPermutation(
                        new ColumnIdentifier("letters.csv", "column1"),
                        new ColumnIdentifier("letters.csv", "column2"),
                        new ColumnIdentifier("letters.csv", "column3"),
                        new ColumnIdentifier("letters.csv", "column4")
                ),
                new ColumnPermutation(
                        new ColumnIdentifier("letters-ext.csv", "column1"),
                        new ColumnIdentifier("letters-ext.csv", "column2"),
                        new ColumnIdentifier("letters-ext.csv", "column3"),
                        new ColumnIdentifier("letters-ext.csv", "column4")
                )
        ));
        expectedInds.add(new InclusionDependency(
                new ColumnPermutation(
                        new ColumnIdentifier("letters-ext.csv", "column2"),
                        new ColumnIdentifier("letters-ext.csv", "column3"),
                        new ColumnIdentifier("letters-ext.csv", "column4")
                ),
                new ColumnPermutation(
                        new ColumnIdentifier("letters.csv", "column2"),
                        new ColumnIdentifier("letters.csv", "column3"),
                        new ColumnIdentifier("letters.csv", "column4")
                )
        ));

        Set<InclusionDependency> discoveredInds = new HashSet<>();
        for (Result result : resultCache.fetchNewResults()) {
            discoveredInds.add((InclusionDependency) result);
        }

        Assert.assertEquals(expectedInds, discoveredInds);
    }


}
