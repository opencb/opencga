package org.opencb.opencga.storage.hadoop.variant.index.core;

public abstract class DynamicIndex extends Index {

    public DynamicIndex() {
    }

    public abstract boolean hasMoreValues();

}
