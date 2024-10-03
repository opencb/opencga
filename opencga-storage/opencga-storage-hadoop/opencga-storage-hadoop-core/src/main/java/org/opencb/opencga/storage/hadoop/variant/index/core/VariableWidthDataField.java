package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

/**
 * Data field with variable data length.
 * @param <T>
 */
public abstract class VariableWidthDataField<T> extends DataField<T> {

    public VariableWidthDataField(IndexFieldConfiguration configuration) {
        super(configuration);
    }
}
