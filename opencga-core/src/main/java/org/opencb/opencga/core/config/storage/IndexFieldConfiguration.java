package org.opencb.opencga.core.config.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.ArrayUtils;

import java.beans.ConstructorProperties;
import java.util.*;

public class IndexFieldConfiguration {
    protected final Source source;
    protected final String key;
    protected Type type;
    protected double[] thresholds;
    protected String[] values;
    protected Map<String, List<String>> valuesMapping;
    protected boolean nullable = true;

    public IndexFieldConfiguration(IndexFieldConfiguration other) {
        this.source = other.source;
        this.key = other.key;
        this.type = other.type;
        this.thresholds = ArrayUtils.clone(other.thresholds);
        this.values = ArrayUtils.clone(other.values);
        this.nullable = other.nullable;
    }

    @ConstructorProperties({"source", "key", "type"})
    protected IndexFieldConfiguration(Source source, String key, Type type) {
        this.source = source;
        this.key = key;
        this.type = type;
    }

    public IndexFieldConfiguration(Source source, String key, double[] thresholds) {
        this(source, key, thresholds, Type.RANGE_LT);
    }

    public IndexFieldConfiguration(Source source, String key, double[] thresholds, Type rangeType) {
        this.key = key;
        this.source = source;
        this.type = rangeType;
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

    @JsonIgnore
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

    public IndexFieldConfiguration setValues(String... values) {
        this.values = values;
        return this;
    }

    public Map<String, List<String>> getValuesMapping() {
        return valuesMapping;
    }

    public IndexFieldConfiguration setValuesMapping(Map<String, List<String>> valuesMapping) {
        this.valuesMapping = valuesMapping;
        return this;
    }

    public boolean getNullable() {
        return nullable;
    }

    public IndexFieldConfiguration setNullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public void validate() {
        if (key == null) {
            throw new IllegalArgumentException("Missing field KEY in index field");
        }
        if (source == null) {
            throw new IllegalArgumentException("Missing field SOURCE in index field " + key);
        }
        if (type == null) {
            throw new IllegalArgumentException("Missing field TYPE in index field " + source + ":" + key);
        }
        switch (type) {
            case RANGE_LT:
            case RANGE_GT:
                if (thresholds == null || thresholds.length == 0) {
                    throw new IllegalArgumentException("Missing 'thresholds' for index field " + getId());
                }
                if (!ArrayUtils.isSorted(thresholds)) {
                    throw new IllegalArgumentException("Thresholds must be sorted!");
                }
//                if (Integer.bitCount(thresholds.length + 1) != 1) {
//                    throw new IllegalArgumentException("Invalid number of thresholds. Got "
//                            + thresholds.length + " thresholds. "
//                            + "Must be a power of 2 minus 1. e.g. 1, 3, 7, 15...");
//                }
                if (values != null && values.length != 0) {
                    throw new IllegalArgumentException("Invalid 'values' with type " + type + " in field " + getId());
                }
                if (valuesMapping != null && !valuesMapping.isEmpty()) {
                    throw new IllegalArgumentException("Invalid 'valuesMapping' with type " + type + " in field " + getId());
                }
                break;
            case CATEGORICAL:
            case CATEGORICAL_MULTI_VALUE:
                if (values == null || values.length == 0) {
                    throw new IllegalArgumentException("Missing 'values' for index field " + getId());
                }
                if (valuesMapping != null) {
                    for (String key : valuesMapping.keySet()) {
                        if (!ArrayUtils.contains(values, key)) {
                            throw new IllegalArgumentException("Unknown value mapping from '" + key + "'");
                        }
                    }
                    Set<String> allValues = new HashSet<>();
                    Set<String> duplicatedValues = new HashSet<>();
                    for (List<String> strings : valuesMapping.values()) {
                        for (String string : strings) {
                            if (!allValues.add(string)) {
                                duplicatedValues.add(string);
                            }
                        }
                    }
                    throw new IllegalArgumentException("Found multiple mappings for these values: " + duplicatedValues);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown type " + type + " for index field " + getId());
        }
    }

    public enum Source {
        VARIANT,
        META,
        FILE,
        SAMPLE,
        ANNOTATION
    }

    public enum Type {
        RANGE_LT,
        RANGE_GT,
        CATEGORICAL,
        CATEGORICAL_MULTI_VALUE;

        @JsonCreator
        public static Type forValues(String value) {
            if (value == null) {
                return null;
            }
            switch (value.toUpperCase()) {
                case "RANGE":
                case "RANGE_LT":
                case "RANGELT":
                case "RANGE_GE":
                case "RANGEGE":
                    return RANGE_LT;
                case "RANGE_GT":
                case "RANGEGT":
                case "RANGE_LE":
                case "RANGELE":
                    return RANGE_GT;
                case "CATEGORICAL":
                    return CATEGORICAL;
                case "CATEGORICAL_MULTI_VALUE":
                case "CATEGORICALMULTIVALUE":
                    return CATEGORICAL_MULTI_VALUE;
                default:
                    throw new IllegalArgumentException("Unknown index field type " + value);
            }
        }
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
