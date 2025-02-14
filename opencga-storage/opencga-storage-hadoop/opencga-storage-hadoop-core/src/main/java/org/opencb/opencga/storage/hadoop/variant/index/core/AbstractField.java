package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.FieldConfiguration;

public abstract class AbstractField {

    protected final FieldConfiguration configuration;

    protected AbstractField(FieldConfiguration configuration) {
        this.configuration = configuration;
    }

    public String getId() {
        return configuration.getId();
    }

    public FieldConfiguration.Source getSource() {
        return configuration.getSource();
    }

    public String getKey() {
        return configuration.getKey();
    }

    public FieldConfiguration getConfiguration() {
        return configuration;
    }

    public FieldConfiguration.Type getType() {
        return configuration.getType();
    }

}
