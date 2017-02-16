package de.hpi.isg.sindy.metanome.properties;

import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;

/**
 * Manages a reflectively declared Metanome configuration property.
 *
 * @see MetanomeProperty
 */
public class MetanomePropertyManager {

    private final ConfigurationRequirement<?> configurationRequirement;

    private final Accessor accessor;

    public MetanomePropertyManager(ConfigurationRequirement<?> configurationRequirement, Accessor accessor) {
        this.configurationRequirement = configurationRequirement;
        this.accessor = accessor;
    }

    public String getPropertyIdentifier() {
        return this.configurationRequirement.getIdentifier();
    }

    public ConfigurationRequirement<?> getConfigurationRequirement() {
        return this.configurationRequirement;
    }

    public void set(Object algorithm, Object value) throws Exception {
        this.accessor.set(algorithm, value);
    }

    @FunctionalInterface
    public interface Accessor {

        void set(Object algorithm, Object value) throws Exception;

    }

}
