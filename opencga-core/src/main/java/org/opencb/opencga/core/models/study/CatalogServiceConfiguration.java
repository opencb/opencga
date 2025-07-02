package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;

public abstract class CatalogServiceConfiguration {

    @DataField(id = "active", description = "Indicates whether the service is active or not")
    private boolean active;

    @DataField(id = "concurrentJobs", description = "Number of concurrent jobs that can be run by this service")
    private int concurrentJobs;

    public CatalogServiceConfiguration() {
    }

    public CatalogServiceConfiguration(boolean active, int concurrentJobs) {
        this.active = active;
        this.concurrentJobs = concurrentJobs;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogServiceConfiguration{");
        sb.append("active=").append(active);
        sb.append(", concurrentJobs=").append(concurrentJobs);
        sb.append('}');
        return sb.toString();
    }

    public boolean isActive() {
        return active;
    }

    public CatalogServiceConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public int getConcurrentJobs() {
        return concurrentJobs;
    }

    public CatalogServiceConfiguration setConcurrentJobs(int concurrentJobs) {
        this.concurrentJobs = concurrentJobs;
        return this;
    }
}
