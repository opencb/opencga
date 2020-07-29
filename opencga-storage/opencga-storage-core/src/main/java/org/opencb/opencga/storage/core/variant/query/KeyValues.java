package org.opencb.opencga.storage.core.variant.query;

import org.opencb.commons.datastore.core.QueryParam;

import java.util.List;

public class KeyValues<K, V> extends Values<V> {

    protected K key;

    public KeyValues() {
    }

    public KeyValues(K key, Values<V> values) {
        super(values.operation, values.values);
        this.key = key;
    }

    public KeyValues(K key, VariantQueryUtils.QueryOperation operation, List<V> values) {
        super(operation, values);
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    public KeyValues<K, V> setKey(K key) {
        this.key = key;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("KeyValues{");
        sb.append("key=").append(key);
        sb.append(", operation=").append(getOperation());
        sb.append(", values=").append(getValues());
        sb.append('}');
        return sb.toString();
    }

    @Override
    public void toQuery(StringBuilder sb) {
        if (key instanceof QueryParam) {
            sb.append(((QueryParam) key).key());
        } else {
            sb.append(key);
        }
        sb.append(VariantQueryUtils.IS);
        super.toQuery(sb);
    }

    @Override
    public void describe(StringBuilder sb) {
        sb.append("( '");
        if (key instanceof QueryParam) {
            sb.append(((QueryParam) key).key());
        } else {
            sb.append(key);
        }
        sb.append("' IS ");
        super.describe(sb);
        sb.append(" )");
    }
}
