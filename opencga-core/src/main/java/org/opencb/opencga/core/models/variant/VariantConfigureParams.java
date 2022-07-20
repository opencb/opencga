package org.opencb.opencga.core.models.variant;

import org.opencb.commons.datastore.core.ObjectMap;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class VariantConfigureParams {

    @DataField(description = ParamConstants.VARIANT_CONFIGURE_PARAMS_CONFIGURATION_DESCRIPTION)
    private ObjectMap configuration;

    public ObjectMap getConfiguration() {
        return configuration;
    }

    public VariantConfigureParams setConfiguration(ObjectMap configuration) {
        this.configuration = configuration;
        return this;
    }
}
