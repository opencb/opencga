package org.opencb.opencga.catalog.models.summaries;

/**
 * Created by pfurio on 12/08/16.
 */
public class FeatureCount {
    private Object name;
    private long count;

    public FeatureCount(Object name, long count) {
        this.name = name;
        this.count = count;
    }

    public Object getName() {
        return name;
    }

    public FeatureCount setName(Object name) {
        this.name = name;
        return this;
    }

    public long getCount() {
        return count;
    }

    public FeatureCount setCount(long count) {
        this.count = count;
        return this;
    }
}
