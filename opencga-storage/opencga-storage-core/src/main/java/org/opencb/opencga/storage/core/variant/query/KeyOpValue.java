package org.opencb.opencga.storage.core.variant.query;

public class KeyOpValue<K, V> {

    private K key;
    private String op; // TODO: Make an enum for this!
    private V value;

    public KeyOpValue(K key, String op, V value) {
        this.key = key;
        this.op = op;
        this.value = value;
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
}
