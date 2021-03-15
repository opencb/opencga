package org.opencb.opencga.core.config.storage;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.Objects;

public class IndexFieldConfiguration {
    protected final Source source;
    protected final String key;
    protected Type type;
    protected double[] thresholds;
    protected String[] values;

    @ConstructorProperties({"source", "key", "type"})
    protected IndexFieldConfiguration(Source source, String key, Type type) {
        this.source = source;
        this.key = key;
        this.type = type;
    }

    public IndexFieldConfiguration(Source source, String key, double[] thresholds) {
        this.key = key;
        this.source = source;
        this.type = Type.RANGE;
        this.thresholds = thresholds;
        this.values = null;
    }

    public IndexFieldConfiguration(Source source, String key, Type type, String... values) {
        this.key = key;
        this.source = source;
        this.type = type;
        this.thresholds = null;
        this.values = values;
    }

    public String getId() {
        return getSource() + ":" + getKey();
    }

    public String getKey() {
        return key;
    }

    public Source getSource() {
        return source;
    }

    public Type getType() {
        return type;
    }

    public IndexFieldConfiguration setType(Type type) {
        this.type = type;
        return this;
    }

    public double[] getThresholds() {
        return thresholds;
    }

    public IndexFieldConfiguration setThresholds(double[] thresholds) {
        this.thresholds = thresholds;
        return this;
    }

    public String[] getValues() {
        return values;
    }

    public IndexFieldConfiguration setValues(String[] values) {
        this.values = values;
        return this;
    }

    public enum Source {
        VARIANT,
        META,
        FILE,
        SAMPLE,
        ANNOTATION
    }

    public enum Type {
        RANGE,
        CATEGORICAL,
        CATEGORICAL_MULTI_VALUE
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexFieldConfiguration that = (IndexFieldConfiguration) o;
        return source == that.source
                && Objects.equals(key, that.key)
                && type == that.type
                && Arrays.equals(thresholds, that.thresholds)
                && Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(source, key, type);
        result = 31 * result + Arrays.hashCode(thresholds);
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndexFieldConfiguration{");
        sb.append("source=").append(source);
        sb.append(", key='").append(key).append('\'');
        sb.append(", type=").append(type);
        sb.append(", thresholds=").append(Arrays.toString(thresholds));
        sb.append(", values=").append(Arrays.toString(values));
        sb.append('}');
        return sb.toString();
    }
}
