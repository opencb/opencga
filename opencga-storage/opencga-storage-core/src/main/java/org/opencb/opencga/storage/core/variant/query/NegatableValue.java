package org.opencb.opencga.storage.core.variant.query;

public class NegatableValue<T> implements QueryElement {
    private final boolean negated;
    private final T value;

    protected NegatableValue() {
        this.negated = false;
        this.value = null;
    }

    public NegatableValue(T value) {
        this(value, false);
    }

    public NegatableValue(T value, boolean negated) {
        this.negated = negated;
        this.value = value;
    }

    public boolean isNegated() {
        return negated;
    }

    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "NegatableValue{"
                + "negated=" + negated
                + ", value=" + value
                + '}';
    }

    @Override
    public void toQuery(StringBuilder sb) {
        if (negated) {
            sb.append("!");
        }
        sb.append(QueryElement.objectToString(value));
    }

    @Override
    public void describe(StringBuilder sb) {
        if (negated) {
            sb.append("NOT ");
        }
        sb.append(QueryElement.objectToDescriptionString(value));
    }

}
