package org.opencb.opencga.storage.core.variant.query;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

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
    public ParsedQuery<T> filter(Predicate<T> selector) {
        Values<T> values = super.filter(selector);
        return new ParsedQuery<>(key, values.operation, values.values);
    }

    @Override
    public <R> ParsedQuery<R> map(Function<T, R> function) {
        return new ParsedQuery<>(key, operation, mapValues(function));
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
