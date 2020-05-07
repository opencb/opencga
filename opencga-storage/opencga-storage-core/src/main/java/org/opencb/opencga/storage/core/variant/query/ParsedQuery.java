package org.opencb.opencga.storage.core.variant.query;

import org.opencb.commons.datastore.core.QueryParam;

import javax.annotation.Nullable;
import java.util.List;

public class ParsedQuery<T> {

    private final QueryParam param;
    private final VariantQueryUtils.QueryOperation operation;
    private final List<T> values;

    public ParsedQuery(QueryParam param, VariantQueryUtils.QueryOperation operation, List<T> values) {
        this.param = param;
        this.operation = operation;
        this.values = values;
    }

    public QueryParam getParam() {
        return param;
    }

    @Nullable
    public VariantQueryUtils.QueryOperation getOperation() {
        return operation;
    }

    public List<T> getValues() {
        return values;
    }
}
