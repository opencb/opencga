package org.opencb.opencga.analysis.variant.operations;

import org.opencb.opencga.analysis.variant.metadata.CatalogStorageMetadataSynchronizer;
import org.opencb.opencga.core.annotations.Tool;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;

@Tool(id = VariantAnnotationDeleteStorageOperation.ID, type = Tool.ToolType.VARIANT)
public class VariantAnnotationDeleteStorageOperation extends StorageOperation {

    public static final String ID = "variant-annotation-delete";
    public String project;
    public String annotationName;

    public VariantAnnotationDeleteStorageOperation setProject(String project) {
        this.project = project;
        return this;
    }

    public VariantAnnotationDeleteStorageOperation setAnnotationName(String annotationName) {
        this.annotationName = annotationName;
        return this;
    }

    @Override
    protected void run() throws Exception {
        step(()->{
            VariantStorageEngine variantStorageEngine = getVariantStorageEngineByProject(project);
            CatalogStorageMetadataSynchronizer
                    .updateProjectMetadata(catalogManager, variantStorageEngine.getMetadataManager(), project, token);
            variantStorageEngine.deleteAnnotation(annotationName, params);
        });
    }
}
