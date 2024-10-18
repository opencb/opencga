package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

public abstract class AbstractField {

    protected final IndexFieldConfiguration configuration;

    protected AbstractField(IndexFieldConfiguration configuration) {
        this.configuration = configuration;
    }

    public String getId() {
        return configuration.getId();
    }

    public IndexFieldConfiguration.Source getSource() {
        return configuration.getSource();
    }

    public String getKey() {
        return configuration.getKey();
    }

    public IndexFieldConfiguration getConfiguration() {
        return configuration;
    }

    public IndexFieldConfiguration.Type getType() {
        return configuration.getType();
    }

}
