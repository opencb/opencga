package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

/**
 * Data field with dynamic length.
 * @param <T>
 */
public abstract class DynamicDataField<T> extends DataField<T> {

    protected static final byte FIELD_SEPARATOR = (byte) 0;

    public DynamicDataField(IndexFieldConfiguration configuration, int valuePosition) {
        super(configuration, valuePosition);
    }
}
