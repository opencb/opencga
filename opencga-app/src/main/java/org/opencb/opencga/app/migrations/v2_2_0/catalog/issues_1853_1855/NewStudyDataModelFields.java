package org.opencb.opencga.app.migrations.v2_2_0.catalog.issues_1853_1855;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.study.StudyType;

import java.util.Collections;

import static com.mongodb.client.model.Filters.eq;

@Migration(id = "new_study_data_model_fields_#1853",
        description = "New Study 'sources', 'type' and 'additionalInfo' fields #1853", version = "2.2.0",
        language = Migration.MigrationLanguage.JAVA,
        domain = Migration.MigrationDomain.CATALOG,
        date = 20211202)
public class NewStudyDataModelFields extends MigrationTool {

    @Override
    protected void run() throws Exception {
        migrateCollection(MongoDBAdaptorFactory.STUDY_COLLECTION,
                new Document("type", new Document("$exists", false)),
                Projections.include("_id"),
                (doc, bulk) -> {
                    Document typeDoc = convertToDocument(StudyType.init());

                    bulk.add(new UpdateOneModel<>(
                            eq("_id", doc.get("_id")),
                            new Document("$set", new Document()
                                    .append("type", typeDoc)
                                    .append("sources", Collections.emptyList())
                                    .append("additionalInfo", Collections.emptyList())
                            ))
                    );
                }
        );
    }

}
