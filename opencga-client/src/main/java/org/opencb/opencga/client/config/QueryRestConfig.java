package org.opencb.opencga.client.config;

public class QueryRestConfig {

    private int batchSize;
    private int limit;

    public QueryRestConfig() {
    }

    public QueryRestConfig(int batchSize, int limit) {
        this.batchSize = batchSize;
        this.limit = limit;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryRestConfig{");
        sb.append("batchSize=").append(batchSize);
        sb.append(", limit=").append(limit);
        sb.append('}');
        return sb.toString();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public QueryRestConfig setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public QueryRestConfig setLimit(int limit) {
        this.limit = limit;
        return this;
    }
}
