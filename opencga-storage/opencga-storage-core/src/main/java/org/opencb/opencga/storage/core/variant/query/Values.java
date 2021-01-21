package org.opencb.opencga.storage.core.variant.query;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public boolean isNotEmpty() {
        return !isEmpty();
    }

    public int size() {
        return values.size();
    }

    public V get(int idx) {
        return values.get(idx);
    }

    public boolean contains(V v) {
        return values.contains(v);
    }

    public List<V> getValues() {
        return values;
    }

    public <R> List<R> getValues(Function<V, R> function) {
        return values.stream().map(function).collect(Collectors.toList());
    }

    public V getValue(Predicate<V> selector) {
        return values.stream().filter(selector).findFirst().orElse(null);
    }

    public Values<V> setValues(List<V> values) {
        this.values = values;
        return this;
    }

    public Stream<V> stream() {
        return values.stream();
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
                sb.append(QueryElement.objectToString(value));
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
                sb.append("'").append(QueryElement.objectToString(value)).append("'");
            }
        }
        if (externalParentesis) {
            sb.append(" )");
        }
    }
}
