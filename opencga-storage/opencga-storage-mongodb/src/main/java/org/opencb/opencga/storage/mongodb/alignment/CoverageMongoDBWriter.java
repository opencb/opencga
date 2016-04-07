/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.alignment;

import org.bson.Document;
import org.opencb.biodata.models.alignment.AlignmentRegion;
import org.opencb.biodata.models.alignment.stats.MeanCoverage;
import org.opencb.biodata.models.alignment.stats.RegionCoverage;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Date 26/08/14.
 *
 * @author Jacobo Coll Moragon <jcoll@ebi.ac.uk>
 */
public class CoverageMongoDBWriter implements DataWriter<AlignmentRegion> {

    public static final String COVERAGE_COLLECTION_NAME = "alignment";

    public static final String ID_FIELD = "_id";
    public static final String COVERAGE_FIELD = "cov";
    public static final String FILES_FIELD = "files";
    public static final String FILE_ID_FIELD = "id";
    public static final String AVERAGE_FIELD = "avg";
    public static final String START_FIELD = "start";
    public static final String CHR_FIELD = "chr";
    public static final String SIZE_FIELD = "size";

    private final MongoDataStoreManager mongoManager;
    private final String fileId;
    private final QueryOptions updateOptions;
    private MongoDataStore db;
    private final DocumentToRegionCoverageConverter coverageConverter;
    private final DocumentToMeanCoverageConverter meanCoverageConverter;
    private final MongoCredentials credentials;
    private final String collectionName;
    private MongoDBCollection collection;

    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(CoverageMongoDBWriter.class);

    public CoverageMongoDBWriter(MongoCredentials credentials, String fileId) {
        this.collectionName = COVERAGE_COLLECTION_NAME;
        this.credentials = credentials;
        this.fileId = fileId;

        mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        coverageConverter = new DocumentToRegionCoverageConverter();
        meanCoverageConverter = new DocumentToMeanCoverageConverter();
        updateOptions = new QueryOptions(MongoDBCollection.UPSERT, true);
    }

    @Override
    public boolean open() {
        //MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder().add("username", "biouser").add("password",
        // "*******").build();
        db = mongoManager.get(credentials.getMongoDbName());

        return true;
    }

    @Override
    public boolean close() {
        mongoManager.close(db.getDatabaseName());
        return true;
    }

    @Override
    public boolean pre() {
        collection = db.createCollection(collectionName);
//        DBCollection nativeCollection = db.getDb().getCollection(collectionName);
//        nativeCollection.createIndex(new Document(FILES_FIELD + "." + FILE_ID_FIELD, "text"));
//        nativeCollection.createIndex(new Document(FILES_FIELD, 1));
        return true;
    }

    @Override
    public boolean post() {
        return true;
    }

    @Override
    public boolean write(AlignmentRegion elem) {
        List<MeanCoverage> meanCoverageList = elem.getMeanCoverage();
        RegionCoverage regionCoverage = elem.getCoverage();

        if (regionCoverage != null) {
            Document coverageQuery = coverageConverter.getIdObject(regionCoverage);
            Document coverageObject = coverageConverter.convertToStorageType(regionCoverage);
            secureInsert(coverageQuery, coverageObject, regionCoverage.getChromosome(), (int) regionCoverage.getStart(), regionCoverage
                    .getAll().length);
        }

        if (meanCoverageList != null) {
            for (MeanCoverage meanCoverage : meanCoverageList) {
                Document query = this.meanCoverageConverter.getIdObject(meanCoverage);  //{_id:"20_2354_1k"}
                Document object = meanCoverageConverter.convertToStorageType(meanCoverage);  //{avg:4.5662}
                secureInsert(query, object, meanCoverage.getRegion().getChromosome(), meanCoverage.getRegion().getStart(), meanCoverage
                        .getSize());
            }
        }
        return true;
    }

    private void secureInsert(Document query, Document object, String chromosome, int start, int size) {
        boolean documentExists = true;
        boolean fileExists = true;

        //Check if the document exists
//        {
        QueryResult countId = collection.count(query);
        if (countId.getNumResults() == 1 && countId.getResultType().equals(Long.class.getCanonicalName())) {
            if ((Long) countId.getResult().get(0) < 1) {
                Document document = new Document()
                        .append(FILES_FIELD, new LinkedList<>())
                        .append(CHR_FIELD, chromosome)
                        .append(START_FIELD, start)
                        .append(SIZE_FIELD, size);
                document.putAll(query);             //{_id:<chunkId>, files:[]}
                collection.insert(document, null);        //Insert a document with empty files array.
                fileExists = false;
            }
        } else {
            logger.error(countId.getErrorMsg(), countId);
        }
//        }

        if (documentExists) {
            //Check if the file exists
            Document fileQuery = new Document(FILES_FIELD + "." + FILE_ID_FIELD, fileId);
            fileQuery.putAll(query);
            QueryResult countFile = collection.count(fileQuery);
            if (countFile.getNumResults() == 1 && countFile.getResultType().equals(Long.class.getCanonicalName())) {
                if ((Long) countFile.getResult().get(0) < 1) {
                    fileExists = false;
                }
            } else {
                logger.error(countFile.getErrorMsg(), countFile);
            }
        }

        if (fileExists) {
            Document fileQuery = new Document(FILES_FIELD + "." + FILE_ID_FIELD, fileId);
            fileQuery.putAll(query);

            Document fileObject = new Document();
            for (String key : object.keySet()) {
                fileObject.put(FILES_FIELD + ".$." + key, object.get(key));
            }
//            DBObject update = new Document("$set", new Document(FILES_FIELD, fileObject));
            Document update = new Document("$set", fileObject);

            //db.<collectionName>.update({_id:<chunkId>  , "files.id":<fileId>}, {$set:{"files.$.<objKey>":<objValue>}})
            collection.update(fileQuery, update, updateOptions);
        } else {
            Document fileObject = new Document(FILE_ID_FIELD, fileId);
            fileObject.putAll(object);
            Document update = new Document("$addToSet", new Document(FILES_FIELD, fileObject));

            //db.<collectionName>.update({_id:<chunkId>} , {$addToSet:{files:{id:<fileId>, <object>}}})
            collection.update(query, update, updateOptions);
        }
    }

    @Override
    public boolean write(List<AlignmentRegion> batch) {
        for (AlignmentRegion region : batch) {
            if (region != null) {
                if (!write(region)) {
                    return false;
                }
            }
        }
        return true;
    }
}
