package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;

public class CvdbCatalogServiceConfiguration extends CatalogServiceConfiguration {

    @DataField(id = "batchSize", description = "Maximum number of cases that can be launched per job")
    private int batchSize;

    public CvdbCatalogServiceConfiguration() {
    }

    public CvdbCatalogServiceConfiguration(boolean active, int concurrentJobs, int batchSize) {
        super(active, concurrentJobs);
        this.batchSize = batchSize;
    }

    public static CvdbCatalogServiceConfiguration defaultConfiguration() {
        return new CvdbCatalogServiceConfiguration(false, 5, 10);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CvdbCatalogServiceConfiguration{");
        sb.append("batchSize=").append(batchSize);
        sb.append('}');
        return sb.toString();
    }

    public int getBatchSize() {
        return batchSize;
    }

    public CvdbCatalogServiceConfiguration setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    @Override
    public CvdbCatalogServiceConfiguration setActive(boolean active) {
        super.setActive(active);
        return this;
    }

    @Override
    public CvdbCatalogServiceConfiguration setConcurrentJobs(int concurrentJobs) {
        super.setConcurrentJobs(concurrentJobs);
        return this;
    }
}
