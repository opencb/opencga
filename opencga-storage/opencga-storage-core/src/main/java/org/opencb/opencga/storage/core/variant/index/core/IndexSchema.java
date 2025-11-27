package org.opencb.opencga.storage.core.variant.index.core;

import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.core.variant.index.core.filters.IndexFilter;

import java.util.List;

/**
 * Index schema.
 * Contains a list of fields that define the schema of the index.
 * <p>
 * The main purpose of an index is to filter data.
 * This schema is used to build the filters and to read the data from the index.
 * <p>
 * Each input data will produce a document in the index. Each document contains a set of fields.
 * The generated entries are stored in a BitBuffer or BitInputStream.
 * <p>
 * The fields of each document are stored in the same order as they are added to the schema.
 * <p>
 *  - BitBuffer
 *    - Doc 1
 *      - FieldValue 1
 *      - ...
 *      - FieldValue n
 *    - ...
 *    - Doc n
 */
public abstract class IndexSchema extends AbstractSchema<IndexField<?>> {

    protected List<IndexField<?>> fields;

    protected IndexSchema() {
    }

    public IndexSchema(List<IndexField<?>> fields) {
        this.fields = fields;
    }

    @Override
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
