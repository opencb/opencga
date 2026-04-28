package org.opencb.opencga.core.config;

public class CvdbConfiguration extends AbstractModuleConfiguration {

    private SearchConfiguration database;

    public CvdbConfiguration() {
        super();
    }

    public CvdbConfiguration(boolean active, SearchConfiguration database) {
        super(active);
        this.database = database;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbConfiguration{");
        sb.append("active=").append(active);
        sb.append(", database=").append(database);
        sb.append('}');
        return sb.toString();
    }

    public SearchConfiguration getDatabase() {
        return database;
    }

    public CvdbConfiguration setDatabase(SearchConfiguration database) {
        this.database = database;
        return this;
    }
}
