package org.opencb.opencga.storage.hadoop.variant.index.core;

public abstract class DynamicIndexSchema extends IndexSchema {

    public DynamicIndexSchema() {
    }

    public abstract boolean hasMoreValues();

}
