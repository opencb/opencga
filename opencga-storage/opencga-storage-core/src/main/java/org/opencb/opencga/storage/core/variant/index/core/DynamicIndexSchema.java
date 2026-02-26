package org.opencb.opencga.storage.core.variant.index.core;

import java.util.List;

public abstract class DynamicIndexSchema extends IndexSchema {

    public DynamicIndexSchema() {
    }

    public DynamicIndexSchema(List<IndexField<?>> fields) {
        super(fields);
    }


}
