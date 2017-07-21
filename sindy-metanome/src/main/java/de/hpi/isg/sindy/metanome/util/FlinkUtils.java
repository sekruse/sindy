package de.hpi.isg.sindy.metanome.util;

import au.com.bytecode.opencsv.CSVParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import it.unimi.dsi.fastutil.ints.IntCollection;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utilities to make the work with Flink easier.
 */
public class FlinkUtils {

    /**
     * Create a {@link ExecutionEnvironment} according to the configuration of this instance.
     *
     * @param flinkMaster a {code host:port} Flink master specification or {@code null} to start a local Flink cluster
     * @param parallelism a desired parallelism for the {@link ExecutionEnvironment} or {@code -1}
     * @param flinkConfig a path to a Typesafe {@link Config} file to initialize Flink from or {@code null}
     * @return the readily configured {@link ExecutionEnvironment}
     */
    public static ExecutionEnvironment createExecutionEnvironment(String flinkMaster, int parallelism, String flinkConfig) throws AlgorithmExecutionException {
        // Load a config if any.
        Configuration flinkConfiguration = flinkConfig != null
                ? parseTypeSafeConfig(new File(flinkConfig))
                : new Configuration();
        if (parallelism != -1)
            flinkConfiguration.setInteger(ConfigConstants.DEFAULT_PARALLELISM_KEY, parallelism);

        // Create a default or a remote execution environment.
        ExecutionEnvironment executionEnvironment;
        if (flinkMaster == null) {
            executionEnvironment = ExecutionEnvironment.createLocalEnvironment(flinkConfiguration);
        } else {
            String[] hostAndPort = flinkMaster.split(":");
            executionEnvironment = ExecutionEnvironment.createRemoteEnvironment(
                    hostAndPort[0],
                    Integer.parseInt(hostAndPort[1]),
                    flinkConfiguration,
                    collectContainingJars(FlinkUtils.class, IntCollection.class, CSVParser.class)
            );
        }

        if (parallelism != -1) executionEnvironment.setParallelism(parallelism);

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

}
