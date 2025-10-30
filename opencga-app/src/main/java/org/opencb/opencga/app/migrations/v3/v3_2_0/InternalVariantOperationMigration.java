package org.opencb.opencga.app.migrations.v3.v3_2_0;

import org.bson.Document;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.project.ProjectInternalVariant;
import org.opencb.opencga.core.models.study.StudyInternalVariant;

import java.util.Arrays;

@Migration(id = "internalVariant_3_2_0", description = "Add internal.variant fields to Project and Study #TASK-6219", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240703)
public class InternalVariantOperationMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        ProjectInternalVariant projectInternalVariant = new ProjectInternalVariant();
        StudyInternalVariant studyInternalVariant = new StudyInternalVariant();
        Document projectInternalVariantDoc = convertToDocument(projectInternalVariant);
        Document studyInternalVariantDoc = convertToDocument(studyInternalVariant);

        Document query = new Document()
                .append("internal.variant", new Document("$exists", false));
        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_PROJECT_COLLECTION)) {
            Document update = new Document()
                    .append("$set", new Document()
                            .append("internal.variant", projectInternalVariantDoc)
                    );
            getMongoCollection(collection).updateMany(query, update);
        }

        for (String collection : Arrays.asList(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
            Document update = new Document()
                    .append("$set", new Document()
                            .append("internal.variant", studyInternalVariantDoc)
                    );
            getMongoCollection(collection).updateMany(query, update);
        }
    }

}
