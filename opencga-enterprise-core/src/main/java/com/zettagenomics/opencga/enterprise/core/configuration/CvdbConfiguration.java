package com.zettagenomics.opencga.enterprise.core.configuration;

import org.opencb.opencga.core.config.SearchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CvdbConfiguration extends AbstractModuleConfiguration {
    private String prefix;
    private SearchConfiguration database;

    protected static Logger logger = LoggerFactory.getLogger(CvdbConfiguration.class);

    public CvdbConfiguration() {
        super();
    }

    public CvdbConfiguration(boolean active, String prefix, SearchConfiguration database) {
        super(active);
        this.prefix = prefix;
        this.database = database;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbConfiguration{");
        sb.append("active=").append(active);
        sb.append(", prefix='").append(prefix).append('\'');
        sb.append(", database=").append(database);
        sb.append('}');
        return sb.toString();
    }

    public String getPrefix() {
        return prefix;
    }

    public CvdbConfiguration setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public SearchConfiguration getDatabase() {
        return database;
    }

    public CvdbConfiguration setDatabase(SearchConfiguration database) {
        this.database = database;
        return this;
    }
}
