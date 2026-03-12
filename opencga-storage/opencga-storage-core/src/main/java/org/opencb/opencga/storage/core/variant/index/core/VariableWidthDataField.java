package org.opencb.opencga.storage.core.variant.index.core;

import org.opencb.opencga.core.config.storage.FieldConfiguration;

/**
 * Data field with variable data length.
 * @param <T>
 */
public abstract class VariableWidthDataField<T> extends DataField<T> {

    public VariableWidthDataField(FieldConfiguration configuration) {
        super(configuration);
    }
}
