package org.opencb.opencga.storage.core.variant.query;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

public class Values<V> implements QueryElement, Iterable<V> {

    protected VariantQueryUtils.QueryOperation operation;
    protected List<V> values;

    public Values() {
    }

    public Values(VariantQueryUtils.QueryOperation operation, List<V> values) {
        this.operation = operation;
        this.values = values;
    }

    public VariantQueryUtils.QueryOperation getOperation() {
        return operation;
    }

    public Values<V> setOperation(VariantQueryUtils.QueryOperation operation) {
        this.operation = operation;
        return this;
    }

    public List<V> getValues() {
        return values;
    }

    public V getValue(Predicate<V> selector) {
        return values.stream().filter(selector).findFirst().orElse(null);
    }

    public Values<V> setValues(List<V> values) {
        this.values = values;
        return this;
    }

    @Override
    public Iterator<V> iterator() {
        return values.iterator();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Values{");
        sb.append("operation=").append(operation);
        sb.append(", values=").append(values);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void toQuery(StringBuilder sb) {
        boolean first = true;
        for (V value : values) {
            if (!first) {
                sb.append(operation.separator());
            }
            first = false;
            if (value instanceof QueryElement) {
                ((QueryElement) value).toQuery(sb);
            } else {
                sb.append(value);
            }
        }
    }

    @Override
    public void describe(StringBuilder sb) {
        boolean externalParentesis = values.size() > 1;
        boolean first = true;
        if (externalParentesis) {
            sb.append("( ");
        }
        for (V value : values) {
            if (!first) {
                sb.append(' ').append(operation.name()).append(' ');
            }
            first = false;
            if (value instanceof QueryElement) {
                ((QueryElement) value).describe(sb);
            } else {
                sb.append("'").append(value).append("'");
            }
        }
        if (externalParentesis) {
            sb.append(" )");
        }
    }
}
