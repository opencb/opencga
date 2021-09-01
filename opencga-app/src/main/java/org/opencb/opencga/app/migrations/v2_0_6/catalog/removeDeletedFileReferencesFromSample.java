package org.opencb.opencga.app.migrations.v2_0_6.catalog;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.migration.Migration;
import org.opencb.opencga.catalog.migration.MigrationTool;

import java.util.*;

@Migration(id="remove_file_references_from_sample", description = "Remove deleted file references from samples #1815", version = "2.0.6",
        rank = 1)
public class removeDeletedFileReferencesFromSample extends MigrationTool {

    @Override
    protected void run() throws CatalogException {
        // Obtain all file ids that have been deleted
        Set<String> deletedFileIds = new HashSet<>();
        queryMongo(MongoDBAdaptorFactory.DELETED_FILE_COLLECTION, new Document(), Projections.include("id"),
                (file) -> deletedFileIds.add(file.getString("id"))
        );

        if (deletedFileIds.isEmpty()) {
            return;
        }

        List<List<String>> batches = getBatches(deletedFileIds);
        Set<String> foundFiles = new HashSet<>();
        for (List<String> batch : batches) {
            queryMongo(MongoDBAdaptorFactory.FILE_COLLECTION, new Document(FileDBAdaptor.QueryParams.ID.key(), batch),
                    Projections.include("id"), (file) -> foundFiles.add(file.getString("id")));
        }
        // Remove from the original list of deleted files the one that also exist in the main file collection (they've been replaced for
        // new ones with same ids)
        deletedFileIds.removeAll(foundFiles);

        if (deletedFileIds.isEmpty()) {
            return;
        }

        // Check and remove any of the deletedFileIds from the sample references
        batches = getBatches(deletedFileIds);
        MongoCollection<Document> sampleCollection = getMongoCollection(MongoDBAdaptorFactory.SAMPLE_COLLECTION);
        for (List<String> batch : batches) {
            Bson update = Updates.pullAll(SampleDBAdaptor.QueryParams.FILE_IDS.key(), batch);
            sampleCollection.updateMany(new Document(SampleDBAdaptor.QueryParams.FILE_IDS.key(), batch), update);
        }
    }

    private List<List<String>> getBatches(Set<String> fileIds) {
        // Generate batches of 50 ids
        int size = 50;

        List<List<String>> batches = new LinkedList<>();
        List<String> tmpList = null;
        int counter = 0;
        for (String fileId : fileIds) {
            if (counter % size == 0) {
                if (counter > 0) {
                    batches.add(tmpList);
                }
                tmpList = new ArrayList<>();
            }
            tmpList.add(fileId);

            counter++;
        }
        batches.add(tmpList);

        return batches;
    }
}
