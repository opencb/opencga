package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;
import java.util.Collections;

@Migration(id = "federationChanges__task_7192",
        description = "Federation changes, #TASK-7192", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250120)
public class FederationChangesMigration extends MigrationTool {

    /*
     * [NEW] Organization -> federation: {clients: [], servers: []}
     * [NEW] Project -> federation: {id: "", description: "", version: ""}
     *               -> internal.federated: [true|false]
     * [NEW] Study -> federation: {id: "", description: "", version: ""}
     *               -> internal.federated: [true|false]
     * [NEW] User -> internal.account.authentication.federation: [true|false]
     */

    @Override
    protected void run() throws Exception {
        // Organization update
        MongoCollection<Document> orgCollection = getMongoCollection(OrganizationMongoDBAdaptorFactory.ORGANIZATION_COLLECTION);
        Bson query = Filters.exists("federation", false);
        Bson update = Updates.set("federation", new Document()
                .append("clients", Collections.emptyList())
                .append("servers", Collections.emptyList())
        );
        orgCollection.updateMany(query, update);

        // Project and Study
        Bson projectStudyQuery = Filters.exists("federation", false);
        Bson projectStudyUpdate = Updates.combine(
                Updates.set("federation", new Document()
                        .append("id", "")
                        .append("description", "")
                        .append("version", "")
                ),
                Updates.set("internal.federated", false)
        );
        for (String collectionStr : Arrays.asList(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_PROJECT_COLLECTION, OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_STUDY_COLLECTION)) {
            getMongoCollection(collectionStr).updateMany(projectStudyQuery, projectStudyUpdate);
        }

        // User
        Bson userQuery = Filters.exists("internal.account.authentication.federation", false);
        Bson userUpdate = Updates.set("internal.account.authentication.federation", false);
        for (String collectionStr : Arrays.asList(OrganizationMongoDBAdaptorFactory.USER_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_USER_COLLECTION)) {
            getMongoCollection(collectionStr).updateMany(userQuery, userUpdate);
        }

        // Drop project id index (no longer unique)
        Document oldIndex = new Document()
                .append("id", 1);
        dropIndex(OrganizationMongoDBAdaptorFactory.PROJECT_COLLECTION, oldIndex);
    }
}
