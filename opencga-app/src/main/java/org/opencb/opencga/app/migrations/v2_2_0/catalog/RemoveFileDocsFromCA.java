package org.opencb.opencga.app.migrations.v2_2_0.catalog;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "remove_file_docs_from_clinical_analyses",
        description = "Store references of File in Clinical Analysis and not full File documents #1837", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211102)
public class RemoveFileDocsFromCA extends MigrationTool {

    @Override
    protected void run() throws Exception {
        ClinicalAnalysisConverter converter = new ClinicalAnalysisConverter();

        migrateCollection(MongoDBAdaptorFactory.CLINICAL_ANALYSIS_COLLECTION,
                new Document(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key() + ".id", new Document("$exists", true)),
                Projections.include("_id", ClinicalAnalysisDBAdaptor.QueryParams.FILES.key()),
                (clinicalDoc, bulk) -> {
                    // Filter files to store only the fields expected
                    converter.validateFilesToUpdate(clinicalDoc);

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", clinicalDoc.get("_id")),
                            new Document("$set", new Document(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(),
                                    clinicalDoc.get(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key()))))
                    );

                }
        );
    }
}
