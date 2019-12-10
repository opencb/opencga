package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Tool(id = VariantAnnotationSaveStorageOperation.ID, type = Tool.ToolType.VARIANT)
public class VariantAnnotationSaveStorageOperation extends StorageOperation {

    public static final String ID = "variant-annotation-save";
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
