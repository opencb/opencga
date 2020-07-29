package org.opencb.opencga.storage.core.variant.query;

import org.apache.commons.lang3.StringUtils;

public class OpValue<V> implements QueryElement {

    protected String op; // TODO: Make an enum for this!
    protected V value;

    public OpValue(String op, V value) {
        this.op = op;
        this.value = value;
    }

    public String getOp() {
        return op;
    }

    public OpValue<V> setOp(String op) {
        this.op = op;
        return this;
    }

    public V getValue() {
        return value;
    }

    public OpValue<V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OpValue{");
        sb.append(", op='").append(op).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void toQuery(StringBuilder sb) {
        if (StringUtils.isEmpty(op)) {
            sb.append("=");
        } else {
            sb.append(op);
        }
        sb.append(value);
    }

    @Override
    public void describe(StringBuilder sb) {
        sb.append("(");
        if (StringUtils.isEmpty(op)) {
            sb.append("=");
        } else {
            sb.append(op);
        }
        if (value instanceof String) {
            sb.append(" '").append(value).append("' )");
        } else {
            sb.append(" ").append(value).append(" )");
        }
    }
}
