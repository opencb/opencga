package org.opencb.opencga.app.migrations.v4.v4_0_0.catalog;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogDBRuntimeException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogRuntimeException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileInternal;

import java.nio.file.Paths;
import java.util.Collections;

@Migration(id = "add_missing_resources", description = "Add missing RESOURCES folder and dependencies #TASK-6442", version = "4.0.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20241016)
public class ResourcesMigration extends MigrationTool {

    @Override
    protected void run() throws Exception {
        MongoCollection<Document> fileCollection = getMongoCollection(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION);

        queryMongo(OrganizationMongoDBAdaptorFactory.STUDY_COLLECTION, new Document(),
                Projections.include("fqn", "uid", "uri"), studyDoc -> {
                    String studyFqn = studyDoc.getString("fqn");
                    long studyUid = studyDoc.get("uid", Number.class).longValue();
                    String studyUri = studyDoc.getString("uri");

                    // Check if the resources folder already exists
                    Bson fileQuery = Filters.and(
                            Filters.eq("studyUid", studyUid),
                            Filters.eq("path", "RESOURCES/")
                    );
                    if (fileCollection.countDocuments(fileQuery) > 0) {
                        // Nothing to do. Resources folder already exists.
                        return;
                    }

                    // Obtain JOBS folder to get the release number that should be associated to the RESSOURCES folder
                    fileQuery = Filters.and(
                            Filters.eq("studyUid", studyUid),
                            Filters.eq("path", "JOBS/")
                    );
                    Bson projection = Projections.include("release");
                    try {
                        queryMongo(OrganizationMongoDBAdaptorFactory.FILE_COLLECTION, fileQuery, projection, fileDoc -> {
                            int release = fileDoc.get("release", Number.class).intValue();

                            // Create the RESOURCES folder
                            File file = new File("RESOURCES", File.Type.DIRECTORY, File.Format.UNKNOWN, File.Bioformat.UNKNOWN,
                                    "RESOURCES/", Paths.get(studyUri).resolve("RESOURCES").toUri(), "Default resources folder",
                                    FileInternal.init(), true, 0, release);
                            try {
                                dbAdaptorFactory.getCatalogFileDBAdaptor(organizationId).insert(studyUid, file, Collections.emptyList(),
                                        Collections.emptyList(), Collections.emptyList(), QueryOptions.empty());
                                logger.info("Missing RESOURCES folder created for study '{}'", studyFqn);
                            } catch (CatalogException e) {
                                throw new CatalogRuntimeException("Could not create missing RESOURCES folder for study '" + studyFqn + "'.",
                                        e);
                            }
                        });
                    } catch (CatalogDBException e) {
                        throw new CatalogDBRuntimeException(e);
                    }

                });
    }

}
