package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexSchema;

import java.util.Collections;
import java.util.List;

/**
 * Multi field index filter.
 */
public abstract class IndexFilter {

    protected final IndexSchema index;
    protected final List<IndexFieldFilter> filters;
    protected final VariantQueryUtils.QueryOperation op;
    private final boolean exact;

    public static IndexFilter noOp(IndexSchema index) {
        return build(index, Collections.emptyList(), VariantQueryUtils.QueryOperation.AND);
    }

    public static IndexFilter build(IndexSchema index, IndexFieldFilter filter) {
        return build(index, Collections.singletonList(filter), VariantQueryUtils.QueryOperation.AND);
    }

    public static IndexFilter build(IndexSchema index, List<IndexFieldFilter> filters, VariantQueryUtils.QueryOperation operation) {
        if (filters == null || filters.isEmpty()) {
            return new NoOpIndexFilter(index);
        } else if (filters.size() == 1) {
            return new SingleIndexFilter(index, filters.get(0));
        } else if (operation == VariantQueryUtils.QueryOperation.AND) {
            return new AndIndexFilter(index, filters);
        } else if (operation == VariantQueryUtils.QueryOperation.OR) {
            return new OrIndexFilter(index, filters);
        } else {
            throw new IllegalArgumentException("Missing query operation");
        }
    }

    protected IndexFilter(IndexSchema index, List<IndexFieldFilter> filters, VariantQueryUtils.QueryOperation op) {
        this.index = index;
        this.filters = filters;
        this.op = op;
        exact = filters.stream().allMatch(IndexFieldFilter::isExactFilter);
    }

    public abstract boolean test(BitBuffer bitBuffer);

    public IndexSchema getIndex() {
        return index;
    }

    public List<IndexFieldFilter> getFilters() {
        return filters;
    }

    public VariantQueryUtils.QueryOperation getOp() {
        return op;
    }

    public boolean isExactFilter() {
        return exact;
    }

    public boolean isNoOp() {
        return false;
    }

    private static class OrIndexFilter extends IndexFilter {
        OrIndexFilter(IndexSchema index, List<IndexFieldFilter> filters) {
            super(index, filters, VariantQueryUtils.QueryOperation.OR);
        }

        @Override
        public boolean test(BitBuffer bitBuffer) {
            for (IndexFieldFilter filter : filters) {
                if (filter.readAndTest(bitBuffer)) {
                    // Require ANY match
                    // If any match, SUCCESS
                    return true;
                }
            }
            // NONE matches
            return false;
        }
    }

    private static class AndIndexFilter extends IndexFilter {

        AndIndexFilter(IndexSchema index, List<IndexFieldFilter> filters) {
            super(index, filters, VariantQueryUtils.QueryOperation.AND);
        }

        @Override
        public boolean test(BitBuffer bitBuffer) {
            for (IndexFieldFilter filter : filters) {
                if (!filter.readAndTest(bitBuffer)) {
                    // Require ALL matches.
                    // If any fail, FAIL
                    return false;
                }
            }
            // NONE fails
            return true;
        }
    }

    private static class SingleIndexFilter extends IndexFilter {
        private final IndexFieldFilter filter;

        SingleIndexFilter(IndexSchema index, IndexFieldFilter filter) {
            super(index, Collections.singletonList(filter), null);
            this.filter = filter;
        }

        @Override
        public boolean test(BitBuffer bitBuffer) {
            return filter.readAndTest(bitBuffer);
        }
    }

    private static class NoOpIndexFilter extends IndexFilter {
        NoOpIndexFilter(IndexSchema index) {
            super(index, Collections.emptyList(), null);
        }

        @Override
        public boolean test(BitBuffer bitBuffer) {
            return true;
        }

        @Override
        public boolean isExactFilter() {
            return false;
        }

        @Override
        public boolean isNoOp() {
            return true;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndexFilter{");
        sb.append("op=").append(op).append(", ");
        sb.append("filters=").append(filters);
        sb.append('}');
        return sb.toString();
    }
}
