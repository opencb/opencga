package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFilter;

import java.util.List;

public abstract class IndexSchema {

    protected List<IndexField<?>> fields;

    protected IndexSchema() {
    }

    public IndexSchema(List<IndexField<?>> fields) {
        this.fields = fields;
    }

    public IndexField<?> getField(IndexFieldConfiguration.Source source, String key) {
        return fields.stream().filter(i -> i.getSource().equals(source) && i.getKey().equals(key)).findFirst().orElse(null);
    }

    public List<IndexField<?>> getFields() {
        return fields;
    }

    public IndexFilter buildFilter(List<IndexFieldFilter> filters, VariantQueryUtils.QueryOperation operation) {
        return IndexFilter.build(this, filters, operation);
    }

    public IndexFilter buildFilter(IndexFieldFilter filter) {
        return IndexFilter.build(this, filter);
    }

    public IndexFilter noOpFilter() {
        return IndexFilter.noOp(this);
    }
}
