package org.opencb.opencga.analysis.variant.operations;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

@Analysis(id = "variant-secondary-index", type = Analysis.AnalysisType.VARIANT)
public class VariantSecondaryIndexStorageOperation extends StorageOperation {

    private String projectStr;
    private String region;
    private boolean overwrite;

    public VariantSecondaryIndexStorageOperation setProject(String projectStr) {
        this.projectStr = projectStr;
        return this;
    }

    public VariantSecondaryIndexStorageOperation setRegion(String region) {
        this.region = region;
        return this;
    }

    public VariantSecondaryIndexStorageOperation setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    @Override
    protected void run() throws Exception {
        step(()->{
            VariantStorageEngine variantStorageEngine = getVariantStorageEngineByProject(projectStr);

            Query inputQuery = new Query();
            inputQuery.putIfNotEmpty(VariantQueryParam.REGION.key(), region);
            variantStorageEngine.searchIndex(inputQuery, new QueryOptions(params), overwrite);
        });
    }
}
