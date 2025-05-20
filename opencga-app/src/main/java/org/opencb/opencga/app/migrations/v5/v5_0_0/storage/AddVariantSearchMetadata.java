package org.opencb.opencga.app.migrations.v5.v5_0_0.storage;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.opencb.opencga.app.migrations.StorageMigrationTool;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

@Migration(id = "add_variant_search_metadata",
        description = "Add variant search metadata to ProjectMetadata where needed. #TASK-6217", version = "5.0.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.STORAGE,
        patch = 1,
        date = 20240519)
public class AddVariantSearchMetadata extends StorageMigrationTool {

    @Override
    protected void run() throws Exception {

        for (String variantStorageProject : getVariantStorageProjects()) {
            VariantStorageEngine engine = getVariantStorageEngineByProject(variantStorageProject);
            logger.info("Checking project '" + variantStorageProject + "'");
            if (engine.getMetadataManager().exists()) {
                ProjectMetadata projectMetadata = engine.getMetadataManager().getProjectMetadata();
                if (projectMetadata.getSecondaryAnnotationIndex().getValues().isEmpty()) {
                    VariantSearchManager variantSearchManager = engine.getVariantSearchManager();

                    SearchIndexMetadata indexMetadata = variantSearchManager.getSearchIndexMetadata();

                    if (indexMetadata == null) {
                        String dbName = engine.getDBName();
                        if (!variantSearchManager.existsCollection(dbName)) {
                            logger.info("Collection '" + dbName + "' does not exist. Skipping project '" + variantStorageProject + "'");
                            continue;
                        }
                        if (variantSearchManager.existsCollection(dbName)) {
                            // Check if a default collection exists
                            variantSearchManager.createMissingIndexMetadata();
                        }
                    }

                } else {
                    logger.info("Variant search metadata already exists for project '" + variantStorageProject + "'");
                }
            }
        }
    }
}
