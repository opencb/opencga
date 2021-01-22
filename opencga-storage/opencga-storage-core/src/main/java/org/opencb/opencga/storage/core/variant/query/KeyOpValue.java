package org.opencb.opencga.storage.core.variant.query;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.storage.core.metadata.models.StudyResourceMetadata;

public class KeyOpValue<K, V> extends OpValue<V> {

    private K key;

    public KeyOpValue(K key, String op, V value) {
        super(op, value);
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    public KeyOpValue<K, V> setKey(K key) {
        this.key = key;
        return this;
    }

    public String getOp() {
        return op;
    }

    public KeyOpValue<K, V> setOp(String op) {
        this.op = op;
        return this;
    }

    public V getValue() {
        return value;
    }

    public KeyOpValue<K, V> setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KeyOpValue{");
        sb.append("key=").append(key);
        sb.append(", op='").append(op).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void toQuery(StringBuilder sb) {
        sb.append(QueryElement.objectToString(key));
        if (StringUtils.isEmpty(op)) {
            sb.append("=");
        } else {
            sb.append(op);
        }
        sb.append(QueryElement.objectToString(value));
    }

    @Override
    public void describe(StringBuilder sb) {
        if (key instanceof String || key instanceof StudyResourceMetadata<?>) {
            sb.append("( '").append(QueryElement.objectToDescriptionString(key)).append("' ");
        } else {
            sb.append("( ").append(QueryElement.objectToDescriptionString(key)).append(" ");
        }
        if (StringUtils.isEmpty(op)) {
            sb.append("=");
        } else {
            sb.append(op);
        }
        if (value instanceof String || value instanceof StudyResourceMetadata<?>) {
            sb.append(" '").append(QueryElement.objectToDescriptionString(value)).append("' )");
        } else {
            sb.append(" ").append(QueryElement.objectToDescriptionString(value)).append(" )");
        }
    }
}
