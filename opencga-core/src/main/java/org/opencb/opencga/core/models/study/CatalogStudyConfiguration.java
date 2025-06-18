package org.opencb.opencga.core.models.study;

public class CatalogStudyConfiguration {

    private CatalogServiceConfiguration cvdb;
    private CatalogServiceConfiguration variantQualityControl;

    public CatalogStudyConfiguration() {
    }

    public CatalogStudyConfiguration(CatalogServiceConfiguration cvdb, CatalogServiceConfiguration variantQualityControl) {
        this.cvdb = cvdb;
        this.variantQualityControl = variantQualityControl;
    }

    public static CatalogStudyConfiguration defaultConfiguration() {
        return new CatalogStudyConfiguration(
                CatalogServiceConfiguration.defaultConfiguration(), // cvdb
                CatalogServiceConfiguration.defaultConfiguration()  // variantQualityControl
        );
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyCatalogConfiguration{");
        sb.append("cvdb=").append(cvdb);
        sb.append(", variantQualityControl=").append(variantQualityControl);
        sb.append('}');
        return sb.toString();
    }

    public CatalogServiceConfiguration getCvdb() {
        return cvdb;
    }

    public CatalogStudyConfiguration setCvdb(CatalogServiceConfiguration cvdb) {
        this.cvdb = cvdb;
        return this;
    }

    public CatalogServiceConfiguration getVariantQualityControl() {
        return variantQualityControl;
    }

    public CatalogStudyConfiguration setVariantQualityControl(CatalogServiceConfiguration variantQualityControl) {
        this.variantQualityControl = variantQualityControl;
        return this;
    }
}
