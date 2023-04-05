package org.opencb.opencga.core.config;

import java.util.List;

public class RgaSearchConfiguration extends SearchConfiguration {

    private boolean cache;
    private int cacheSize;
    private String suffix;

    public RgaSearchConfiguration() {
    }

    public RgaSearchConfiguration(List<String> hosts, String configSet, String mode, String user, String password, String manager,
                                  boolean active, int timeout, int insertBatchSize, boolean cache, int cacheSize, String suffix) {
        super(hosts, configSet, mode, user, password, manager, active, timeout, insertBatchSize);
        this.cache = cache;
        this.cacheSize = cacheSize;
        this.suffix = suffix;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RgaSearchConfiguration{");
        sb.append("cache=").append(cache);
        sb.append(", cacheSize=").append(cacheSize);
        sb.append(", suffix='").append(suffix).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public boolean isCache() {
        return cache;
    }

    public RgaSearchConfiguration setCache(boolean cache) {
        this.cache = cache;
        return this;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public RgaSearchConfiguration setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public String getSuffix() {
        return suffix;
    }

    public RgaSearchConfiguration setSuffix(String suffix) {
        this.suffix = suffix;
        return this;
    }
}
