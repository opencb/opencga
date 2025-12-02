package org.opencb.opencga.app.migrations.v3.v3_5_0;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.OrganizationMongoDBAdaptorFactory;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.*;

@Migration(id = "minimize_file_data_in_job_7358",
        description = "Minimize file data in Job  #7358", version = "3.5.0",
        language = Migration.MigrationLanguage.JAVA, domain = Migration.MigrationDomain.CATALOG, date = 20250224)
public class MinimiseFileDataInJob_TASK_7358 extends MigrationTool {

    @Override
    protected void run() throws Exception {
        Bson query = Filters.or(
                Filters.exists("attributes._opencga.deletedInputFiles.attributes" , true),
                Filters.exists("attributes._opencga.deletedOutputFiles.attributes" , true)
        );
        Bson projection = Projections.include("uid", "attributes");
        migrateCollection(Arrays.asList(OrganizationMongoDBAdaptorFactory.JOB_COLLECTION, OrganizationMongoDBAdaptorFactory.DELETED_JOB_COLLECTION),
                query, projection, (document, bulk) -> {
                    MongoDBAdaptor.UpdateDocument updateDocument = new MongoDBAdaptor.UpdateDocument();
                    Document ocgaAttributes = document.get("attributes", Document.class).get("_opencga", Document.class);

                    // Process deleted input files list
                    if (ocgaAttributes.containsKey("deletedInputFiles")) {
                        List<Document> deletedInputFiles = ocgaAttributes.getList("deletedInputFiles", Document.class);
                        if (deletedInputFiles != null) {
                            List<Document> reducedDeletedInputFiles = new ArrayList<>(deletedInputFiles.size());
                            for (Document deletedInputFile : deletedInputFiles) {
                                reducedDeletedInputFiles.add(getReducedFileDocument(deletedInputFile));
                            }
                            updateDocument.getSet().put("attributes._opencga.deletedInputFiles", reducedDeletedInputFiles);
                        }
                    }

                    // Process deleted output files list
                    if (ocgaAttributes.containsKey("deletedOutputFiles")) {
                        Object deletedOutputFilesObject = ocgaAttributes.get("deletedOutputFiles");
                        List<Document> deletedOutputFiles;
                        if (deletedOutputFilesObject instanceof Map) {
                            deletedOutputFiles = Collections.singletonList((Document) deletedOutputFilesObject);
                        } else {
                            deletedOutputFiles = ocgaAttributes.getList("deletedOutputFiles", Document.class);
                        }
                        if (deletedOutputFiles != null) {
                            List<Document> reducedDeletedOutputFiles = new ArrayList<>(deletedOutputFiles.size());
                            for (Document deletedOutputFile : deletedOutputFiles) {
                                reducedDeletedOutputFiles.add(getReducedFileDocument(deletedOutputFile));
                            }
                            updateDocument.getSet().put("attributes._opencga.deletedOutputFiles", reducedDeletedOutputFiles);
                        }
                    }

                    bulk.add(new UpdateOneModel<>(Filters.eq("_id", document.get("_id")), updateDocument.toFinalUpdateDocument()));
                });
    }

    private Document getReducedFileDocument(Document file) {
        return new Document()
                .append(FileDBAdaptor.QueryParams.ID.key(), file.get(FileDBAdaptor.QueryParams.ID.key()))
                .append(FileDBAdaptor.QueryParams.UUID.key(), file.get(FileDBAdaptor.QueryParams.UUID.key()))
                .append(FileDBAdaptor.QueryParams.PATH.key(), file.get(FileDBAdaptor.QueryParams.PATH.key()))
                .append(FileDBAdaptor.QueryParams.URI.key(), file.get(FileDBAdaptor.QueryParams.URI.key()))
                .append(FileDBAdaptor.QueryParams.TYPE.key(), file.get(FileDBAdaptor.QueryParams.TYPE.key()))
                .append(FileDBAdaptor.QueryParams.FORMAT.key(), file.get(FileDBAdaptor.QueryParams.FORMAT.key()))
                .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), file.get(FileDBAdaptor.QueryParams.BIOFORMAT.key()));
    }

}
