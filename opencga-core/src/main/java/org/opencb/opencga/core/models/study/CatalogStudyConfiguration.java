package org.opencb.opencga.core.models.study;

public class CatalogStudyConfiguration {

    private CvdbCatalogServiceConfiguration cvdb;
    private QualityControlServiceConfiguration variantQualityControl;

    public CatalogStudyConfiguration() {
    }

    public CatalogStudyConfiguration(CvdbCatalogServiceConfiguration cvdb, QualityControlServiceConfiguration variantQualityControl) {
        this.cvdb = cvdb;
        this.variantQualityControl = variantQualityControl;
    }

    public static CatalogStudyConfiguration defaultConfiguration() {
        return new CatalogStudyConfiguration(CvdbCatalogServiceConfiguration.defaultConfiguration(),
                QualityControlServiceConfiguration.defaultConfiguration());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CatalogStudyConfiguration{");
        sb.append("cvdb=").append(cvdb);
        sb.append(", variantQualityControl=").append(variantQualityControl);
        sb.append('}');
        return sb.toString();
    }

    public CvdbCatalogServiceConfiguration getCvdb() {
        return cvdb;
    }

    public CatalogStudyConfiguration setCvdb(CvdbCatalogServiceConfiguration cvdb) {
        this.cvdb = cvdb;
        return this;
    }

    public QualityControlServiceConfiguration getVariantQualityControl() {
        return variantQualityControl;
    }

    public CatalogStudyConfiguration setVariantQualityControl(QualityControlServiceConfiguration variantQualityControl) {
        this.variantQualityControl = variantQualityControl;
        return this;
    }
}
