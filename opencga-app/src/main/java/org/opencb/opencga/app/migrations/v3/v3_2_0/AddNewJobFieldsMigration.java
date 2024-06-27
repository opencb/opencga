package org.opencb.opencga.app.migrations.v3.v3_2_0;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.Arrays;

@Migration(id = "add_jobParentId_scheduledStartTime",
        description = "Add 'jobParentId' and 'scheduledStartTime' to existing jobs #TASK-6171 #TASK-6089", version = "3.2.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20240506)
public class AddNewJobFieldsMigration extends MigrationTool {


    @Override
    protected void run() throws Exception {
        for (String jobCollection : Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION,
                OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION)) {
            MongoCollection<Document> mongoCollection = getMongoCollection(jobCollection);
            Bson query = Filters.exists("parentId", false);
            Bson update = Updates.combine(
                    Updates.set("parentId", ""),
                    Updates.set("scheduledStartTime", "")
            );
            mongoCollection.updateMany(query, update);
        }
    }

}
