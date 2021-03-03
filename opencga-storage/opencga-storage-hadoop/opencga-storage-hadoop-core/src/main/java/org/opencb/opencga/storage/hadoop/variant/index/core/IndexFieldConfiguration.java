package org.opencb.opencga.storage.hadoop.variant.index.core;

public class IndexFieldConfiguration {
    private final Source source;
    private final String key;
    private Type type;
    private double[] thresholds;
    private String[] values;

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
}
