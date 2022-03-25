package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;

import java.util.Arrays;
import java.util.List;

public class AndIndexFieldFilter extends IndexFieldFilter {

    private final List<IndexFieldFilter> filters;
    private final boolean exactFilter;

    public AndIndexFieldFilter(IndexField<?> indexField, IndexFieldFilter... filters) {
        this(indexField, Arrays.asList(filters));
    }

    public AndIndexFieldFilter(IndexField<?> indexField, List<IndexFieldFilter> filters) {
        super(indexField);
        this.filters = filters;
        exactFilter = filters.stream().allMatch(IndexFieldFilter::isExactFilter);
    }

    @Override
    public boolean test(int code) {
        for (IndexFieldFilter filter : filters) {
            if (!filter.test(code)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isExactFilter() {
        return exactFilter;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AndIndexFieldFilter{");
        sb.append("exact:").append(exactFilter).append(", ");
        sb.append("filters:").append(filters);
        sb.append('}');
        return sb.toString();
    }
}
