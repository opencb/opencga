package org.opencb.opencga.core.models.variant;

import org.opencb.commons.datastore.core.ObjectMap;

public class VariantConfigureParams {

    private ObjectMap configuration;

    public ObjectMap getConfiguration() {
        return configuration;
    }

    public VariantConfigureParams setConfiguration(ObjectMap configuration) {
        this.configuration = configuration;
        return this;
    }
}
