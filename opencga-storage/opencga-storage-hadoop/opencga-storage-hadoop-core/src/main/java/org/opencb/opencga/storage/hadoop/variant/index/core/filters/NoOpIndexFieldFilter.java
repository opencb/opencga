package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;

public class NoOpIndexFieldFilter extends IndexFieldFilter {


    public NoOpIndexFieldFilter(IndexField<?> indexField) {
        super(indexField);
    }

    @Override
    public boolean test(int code) {
        return true;
    }

    /**
     * No operation filter is never exact, as it's not doing anything.
     * @return false
     */
    @Override
    public boolean isExactFilter() {
        return false;
    }
}
