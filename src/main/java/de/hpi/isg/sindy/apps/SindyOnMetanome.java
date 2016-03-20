package de.hpi.isg.sindy.apps;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.FileInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.StringParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Created by basti on 02/17/16.
 */
public class SindyOnMetanome implements InclusionDependencyAlgorithm, StringParameterAlgorithm, FileInputParameterAlgorithm {

    private InclusionDependencyResultReceiver resultReceiver;

    @Override
    public void setResultReceiver(InclusionDependencyResultReceiver inclusionDependencyResultReceiver) {
        this.resultReceiver = inclusionDependencyResultReceiver;
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        // TODO: Describe the configuration needed by SINDY.
        return null;
    }

    @Override
    public void execute() throws AlgorithmExecutionException {
        // TODO: Set up SINDY.
        // TODO: Invoke SINDY.
    }

    @Override
    public void setFileInputConfigurationValue(String identifier, FileInputGenerator... values) throws AlgorithmConfigurationException {
        // TODO: Use this to find the input files to take.
        final Stream<URI> inputTableUris = Arrays.stream(values).map(FileInputGenerator::getInputFile).map(File::toURI);
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values) throws AlgorithmConfigurationException {
        // TODO: Initialize...
    }
}
