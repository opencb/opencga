package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.models.DataStore;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Analysis(id = "variant-annotation-save", type = Analysis.AnalysisType.VARIANT)
public class VariantAnnotationSaveStorageOperation extends StorageOperation {

    public String project;
    public String annotationName;

    public VariantAnnotationSaveStorageOperation setProject(String project) {
        this.project = project;
        return this;
    }

    public VariantAnnotationSaveStorageOperation setAnnotationName(String annotationName) {
        this.annotationName = annotationName;
        return this;
    }

    @Override
    protected void run() throws Exception {
        step(()->{
            VariantStorageEngine variantStorageEngine = getVariantStorageEngineByProject(project);
            CatalogStorageMetadataSynchronizer
                    .updateProjectMetadata(catalogManager, variantStorageEngine.getMetadataManager(), project, token);
            variantStorageEngine.saveAnnotation(annotationName, params);
        });
    }
}
