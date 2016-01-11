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

package org.opencb.opencga.catalog.db.mongodb2;

import com.mongodb.BasicDBObject;
import com.mongodb.DuplicateKeyException;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.catalog.db.AbstractCatalogDBAdaptor;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api2.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Metadata;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.mongodb2.CatalogMongoDBUtils.getMongoDBDocument;

/**
 * Created by jacobo on 12/09/14.
 */
public class CatalogMongoDBAdaptor extends AbstractCatalogDBAdaptor implements CatalogDBAdaptorFactory {

    static final String METADATA_OBJECT_ID = "METADATA";
    //Keys to foreign objects.
    static final String _ID = "_id";
    static final String _PROJECT_ID = "_projectId";
    static final String _STUDY_ID = "_studyId";
    static final String FILTER_ROUTE_STUDIES = "projects.studies.";
    static final String FILTER_ROUTE_INDIVIDUALS = "projects.studies.individuals.";
    static final String FILTER_ROUTE_SAMPLES = "projects.studies.samples.";
    static final String FILTER_ROUTE_FILES = "projects.studies.files.";
    static final String FILTER_ROUTE_JOBS = "projects.studies.jobs.";
    private static final String USER_COLLECTION = "user";
    private static final String STUDY_COLLECTION = "study";
    private static final String FILE_COLLECTION = "file";
    private static final String JOB_COLLECTION = "job";
    private static final String SAMPLE_COLLECTION = "sample";
    private static final String INDIVIDUAL_COLLECTION = "individual";
    private static final String METADATA_COLLECTION = "metadata";
    private static final String AUDIT_COLLECTION = "audit";

    private final MongoDataStoreManager mongoManager;
    private final MongoDBConfiguration configuration;
    private final String database;
    //    private final DataStoreServerAddress dataStoreServerAddress;
    private MongoDataStore db;

    protected MongoDBCollection metaCollection;
    protected MongoDBCollection userCollection;
    protected MongoDBCollection studyCollection;
    protected MongoDBCollection fileCollection;
    protected MongoDBCollection sampleCollection;
    protected MongoDBCollection individualCollection;
    protected MongoDBCollection jobCollection;
    protected MongoDBCollection auditCollection;
    protected Map<String, MongoDBCollection> collections;

    private CatalogMongoUserDBAdaptor userDBAdaptor;
    private CatalogMongoProjectDBAdaptor projectDBAdaptor;
    private CatalogMongoStudyDBAdaptor studyDBAdaptor;
    private CatalogMongoFileDBAdaptor fileDBAdaptor;
    private CatalogMongoJobDBAdaptor jobDBAdaptor;
    private CatalogMongoSampleDBAdaptor sampleDBAdaptor;
    private CatalogMongoIndividualDBAdaptor individualDBAdaptor;
    private CatalogAuditDBAdaptor auditDBAdaptor;



    //    private static final Logger logger = LoggerFactory.getLogger(CatalogMongoDBAdaptor.class);

    public CatalogMongoDBAdaptor() {
        super(LoggerFactory.getLogger(CatalogMongoDBAdaptor.class));
        this.mongoManager = null;
        this.configuration = null;
        this.database = "";
    }

    public CatalogMongoDBAdaptor(List<DataStoreServerAddress> dataStoreServerAddressList, MongoDBConfiguration configuration,
                                 String database) throws CatalogDBException {
        super(LoggerFactory.getLogger(CatalogMongoDBAdaptor.class));
        this.mongoManager = new MongoDataStoreManager(dataStoreServerAddressList);
        this.configuration = configuration;
        this.database = database;

        connect();
    }


    @Override
    public CatalogUserDBAdaptor getCatalogUserDBAdaptor() {
        return userDBAdaptor;
    }

    @Override
    public CatalogProjectDBAdaptor getCatalogProjectDbAdaptor() {
        return projectDBAdaptor;
    }

    @Override
    public CatalogStudyDBAdaptor getCatalogStudyDBAdaptor() {
        return studyDBAdaptor;
    }

    @Override
    public CatalogFileDBAdaptor getCatalogFileDBAdaptor() {
        return fileDBAdaptor;
    }

    @Override
    public CatalogSampleDBAdaptor getCatalogSampleDBAdaptor() {
        return sampleDBAdaptor;
    }

    @Override
    public CatalogIndividualDBAdaptor getCatalogIndividualDBAdaptor() {
        return individualDBAdaptor;
    }

    @Override
    public CatalogJobDBAdaptor getCatalogJobDBAdaptor() {
        return jobDBAdaptor;
    }

    @Override
    public CatalogAuditDBAdaptor getCatalogAuditDbAdaptor() {
        return auditDBAdaptor;
    }

    private void connect() throws CatalogDBException {
        db = mongoManager.get(database, configuration);
        if (db == null) {
            throw new CatalogDBException("Unable to connect to MongoDB");
        }

        collections = new HashMap<>();
        collections.put(METADATA_COLLECTION, metaCollection = db.getCollection(METADATA_COLLECTION));
        collections.put(USER_COLLECTION, userCollection = db.getCollection(USER_COLLECTION));
        collections.put(STUDY_COLLECTION, studyCollection = db.getCollection(STUDY_COLLECTION));
        collections.put(FILE_COLLECTION, fileCollection = db.getCollection(FILE_COLLECTION));
        collections.put(SAMPLE_COLLECTION, sampleCollection = db.getCollection(SAMPLE_COLLECTION));
        collections.put(INDIVIDUAL_COLLECTION, individualCollection = db.getCollection(INDIVIDUAL_COLLECTION));
        collections.put(JOB_COLLECTION, jobCollection = db.getCollection(JOB_COLLECTION));
        collections.put(AUDIT_COLLECTION, auditCollection = db.getCollection(AUDIT_COLLECTION));

        userDBAdaptor = new CatalogMongoUserDBAdaptor(this, metaCollection, userCollection);
        studyDBAdaptor = new CatalogMongoStudyDBAdaptor(this, metaCollection, studyCollection, fileCollection);
        individualDBAdaptor = new CatalogMongoIndividualDBAdaptor(this, metaCollection, individualCollection);
        sampleDBAdaptor = new CatalogMongoSampleDBAdaptor(this, metaCollection, sampleCollection, studyCollection);
        auditDBAdaptor = new CatalogMongoAuditDBAdaptor(auditCollection);
    }

    @Override
    public void initializeCatalogDB() throws CatalogDBException {
        //If "metadata" document doesn't exist, create.
        if (!isCatalogDBReady()) {

            /* Check all collections are empty */
            for (Map.Entry<String, MongoDBCollection> entry : collections.entrySet()) {
                if (entry.getValue().count().first() != 0L) {
                    throw new CatalogDBException("Fail to initialize Catalog Database in MongoDB. Collection " + entry.getKey() + " is " +
                            "not empty.");
                }
            }

            try {
                Document metadataObject = getMongoDBDocument(new Metadata(), "Metadata").append("_id", METADATA_OBJECT_ID);
                metaCollection.insert(metadataObject, null);

            } catch (DuplicateKeyException e) {
                logger.warn("Trying to replace MetadataObject. DuplicateKey");
            }
            //Set indexes
//            BasicDBObject unique = new BasicDBObject("unique", true);
//            nativeUserCollection.createIndex(new BasicDBObject("id", 1), unique);
//            nativeFileCollection.createIndex(BasicDBObjectBuilder.start("studyId", 1).append("path", 1).get(), unique);
//            nativeJobCollection.createIndex(new BasicDBObject("id", 1), unique);
        } else {
            throw new CatalogDBException("Catalog already initialized");
        }
    }

    /**
     * CatalogMongoDBAdaptor is ready when contains the METADATA_OBJECT
     *
     * @return
     */
    @Override
    public boolean isCatalogDBReady() {
        QueryResult<Long> queryResult = metaCollection.count(new BasicDBObject("_id", METADATA_OBJECT_ID));
        return queryResult.getResult().get(0) == 1;
    }

    @Override
    public void close() {
        mongoManager.close(db.getDatabaseName());
    }


    /**
     * Auxiliary query methods
     */
    protected int getNewId() {
        return CatalogMongoDBUtils.getNewAutoIncrementId(metaCollection);
    }






    protected void createOrQuery(Query query, String queryParam, String mongoDbField, List<Bson> andBsonList) {
        if (query != null && query.getString(queryParam) != null && !query.getString(queryParam).isEmpty()) {
            createOrQuery(query.getAsStringList(queryParam), mongoDbField, andBsonList);
        }
    }

    protected void createOrQuery(List<String> queryValues, String mongoDbField, List<Bson> andBsonList) {
        if (queryValues.size() == 1) {
            andBsonList.add(Filters.eq(mongoDbField, queryValues.get(0)));
        } else {
            List<Bson> orBsonList = new ArrayList<>(queryValues.size());
            for (String queryItem : queryValues) {
                orBsonList.add(Filters.eq(mongoDbField, queryItem));
            }
            andBsonList.add(Filters.or(orBsonList));
        }
    }

    protected QueryResult groupBy(MongoDBCollection collection, Bson query, String groupByField, String idField, QueryOptions options) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new QueryResult();
        }

        if (groupByField.contains(",")) {
            // call to multiple groupBy if commas are present
            return groupBy(collection, query, Arrays.asList(groupByField.split(",")), idField, options);
        } else {
            Bson match = Aggregates.match(query);
            Bson project = Aggregates.project(Projections.include(groupByField, idField));
            Bson group;
            if (options.getBoolean("count", false)) {
                group = Aggregates.group("$" + groupByField, Accumulators.sum("count", 1));
            } else {
                group = Aggregates.group("$" + groupByField, Accumulators.addToSet("features", "$" + idField));
            }
            return collection.aggregate(Arrays.asList(match, project, group), options);
        }
    }

    protected QueryResult groupBy(MongoDBCollection collection, Bson query, List<String> groupByField, String idField, QueryOptions options) {
        if (groupByField == null || groupByField.isEmpty()) {
            return new QueryResult();
        }

        if (groupByField.size() == 1) {
            // if only one field then we call to simple groupBy
            return groupBy(collection, query, groupByField.get(0), idField, options);
        } else {
            Bson match = Aggregates.match(query);

            // add all group-by fields to the projection together with the aggregation field name
            List<String> groupByFields = new ArrayList<>(groupByField);
            groupByFields.add(idField);
            Bson project = Aggregates.project(Projections.include(groupByFields));

            // _id document creation to have the multiple id
            Document id = new Document();
            for (String s : groupByField) {
                id.append(s, "$" + s);
            }
            Bson group;
            if (options.getBoolean("count", false)) {
                group = Aggregates.group(id, Accumulators.sum("count", 1));
            } else {
                group = Aggregates.group(id, Accumulators.addToSet("features", "$" + idField));
            }
            return collection.aggregate(Arrays.asList(match, project, group), options);
        }
    }

}
