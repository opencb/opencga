package org.opencb.opencga.storage.core.variant.query;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.LinkedList;
import java.util.List;

public class ParsedQuery<T> extends Values<T> {

    private final QueryParam key;

    public ParsedQuery(QueryParam key) {
        super(null, new LinkedList<>());
        this.key = key;
    }

    public ParsedQuery(QueryParam key, VariantQueryUtils.QueryOperation operation, List<T> value) {
        super(operation, value);
        this.key = key;
    }

    public QueryParam getKey() {
        return key;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ParsedQuery{");
        sb.append("key=").append(key.key());
        sb.append(", operation=").append(getOperation());
        sb.append(", values=").append(getValues());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void toQuery(StringBuilder sb) {
        super.toQuery(sb);
    }

//    @Override
//    public String describe() {
//        StringBuilder sb = new StringBuilder();
//        super.describe(sb);
//        String describe = sb.toString();
//        if (describe.startsWith("( ") && describe.endsWith(" )")) {
//            describe = describe.substring(2, describe.length() - 2);
//        }
//        return key.key() + " : " + describe;
//    }

    @Override
    public void describe(StringBuilder sb) {
        sb.append(key.key());
        sb.append(" : ");
        super.describe(sb);
    }
}
