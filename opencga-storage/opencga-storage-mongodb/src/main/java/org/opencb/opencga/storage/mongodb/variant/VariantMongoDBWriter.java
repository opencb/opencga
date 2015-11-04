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

package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.io.VariantDBWriter;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBWriter extends VariantDBWriter {

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
//    public static final Logger logger = Logger.getLogger(VariantMongoDBWriter.class.getName());
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(VariantDBWriter.class);
    private final VariantMongoDBAdaptor dbAdaptor;

//    @Deprecated private final VariantSource source;

//    private MongoCredentials credentials;
//
//    private MongoDataStore mongoDataStore;
//    private MongoDataStoreManager mongoDataStoreManager;
//    private MongoDBCollection variantMongoCollection;
//    private MongoDBCollection filesMongoCollection;
//
//    private String filesCollectionName;
//    private String variantsCollectionName;
//
//    @Deprecated private DBCollection variantsCollection;    //Used only for Bulk Operations. TODO: Use Datastore API for BulkOperations
////    @Deprecated private MongoClient mongoClient;
//    @Deprecated private DB db;
//

    private boolean includeStats;
    private boolean includeSrc = true;
    private boolean includeSamples;
//    private boolean compressDefaultGenotype = true;
//    private String defaultGenotype = null;

    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceConverter sourceConverter;
    private DBObjectToStudyVariantEntryConverter sourceEntryConverter;
    private DBObjectToSamplesConverter sampleConverter;

//    private long numVariantsWritten;
    private static long staticNumVariantsWritten;

//    private BulkWriteOperation bulk;
//    private int currentBulkSize = 0;
//    private int bulkSize = 0;
//
//    private long checkExistsTime = 0;
//    private long checkExistsDBTime = 0;
    private long insertionTime = 0;
    private StudyConfiguration studyConfiguration;
//    private Integer fileId;
    private int fileId;
    private boolean writeStudyConfiguration = true;
//    private boolean writeVariantSource = true;
//    private VariantSource source;

    private AtomicBoolean variantSourceWritten = new AtomicBoolean(false);
    private MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();
    private HashSet<String> coveredChromosomes = new HashSet<>();
    private List<Integer> fileSampleIds;
    private List<Integer> loadedSampleIds;

//
//    public VariantMongoDBWriter(Integer fileId, StudyConfiguration studyConfiguration, MongoCredentials credentials, String variantsCollection, String filesCollection,
//                                boolean includeSamples, boolean includeStats) {
//        if (credentials == null) {
//            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
//        }
//        this.studyConfiguration = studyConfiguration;
////        this.source = source;
//        this.fileId = String.valueOf(fileId);
//        this.credentials = credentials;
//        this.filesCollectionName = filesCollection;
//        this.variantsCollectionName = variantsCollection;
//
//        this.includeSamples = includeSamples;
//        this.includeStats = includeStats;
//    }

    public VariantMongoDBWriter(Integer fileId, StudyConfiguration studyConfiguration, VariantMongoDBAdaptor dbAdaptor,
                                boolean includeSamples, boolean includeStats) {
        this.dbAdaptor = dbAdaptor;
//        if (credentials == null) {
//            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
//        }
        this.studyConfiguration = studyConfiguration;
        this.fileId = fileId;

        this.includeSamples = includeSamples;
        this.includeStats = includeStats;
    }

    @Override
    public boolean open() {
        staticNumVariantsWritten = 0;
        insertionTime = 0;
        coveredChromosomes.clear();
//        numVariantsWritten = 0;

        return true;
//        mongoDataStoreManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
//        mongoDataStore = mongoDataStoreManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
//        db = mongoDataStore.getDb();
//
//
//        return mongoDataStore != null;
    }

    @Override
    public boolean pre() {
        this.fileSampleIds = new LinkedList<>(studyConfiguration.getSamplesInFiles().get(fileId));
        loadedSampleIds = VariantMongoDBAdaptor.getLoadedSamples(fileId, studyConfiguration);
        // Mongo collection creation
//        variantMongoCollection = mongoDataStore.getCollection(variantsCollectionName);
//        filesMongoCollection = mongoDataStore.getCollection(filesCollectionName);
//        variantsCollection = mongoDataStore.getDb().getCollection(variantsCollectionName);

        setConverters();

//        resetBulk();

//        return variantMongoCollection != null && filesMongoCollection != null;
        return true;
    }

    @Override
    public boolean write(Variant variant) {
        return write(Collections.singletonList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
//        return write_setOnInsert(data);
        synchronized (variantSourceWritten) {
            long l = staticNumVariantsWritten/1000;
            staticNumVariantsWritten += data.size();
            if (staticNumVariantsWritten/1000 != l) {
                logger.info("Num variants written " + staticNumVariantsWritten);
            }
        }
//
//        if (fileSampleIds == null) {
//            fileSampleIds = new LinkedList<>();
//            loadedSampleIds = new LinkedList<>();
//            data.get(0).getSampleNames(Integer.toString(studyConfiguration.getStudyId()), Integer.toString(fileId)).iterator().forEachRemaining(s -> fileSampleIds.add(studyConfiguration.getSampleIds().get(s)));
//            LinkedHashSet<Integer> fileSampleIdsSet = new LinkedHashSet<>(fileSampleIds);
//            loadedSampleIds.addAll(studyConfiguration.getSampleIds().keySet().stream()
//                    .filter(sample -> !fileSampleIdsSet.contains(studyConfiguration.getSampleIds().get(sample)))
//                    .map(sample -> studyConfiguration.getSampleIds().get(sample))
//                    .collect(Collectors.toList()));
//        }

        if (!data.isEmpty()) {
            coveredChromosomes.add(data.get(0).getChromosome());
        }
        QueryResult<MongoDBVariantWriteResult> queryResult = dbAdaptor.insert(data, fileId, this.variantConverter, this.sourceEntryConverter, studyConfiguration, loadedSampleIds);

        MongoDBVariantWriteResult batchWriteResult = queryResult.first();
        logger.debug("New batch of {} elements. WriteResult: {}", data.size(), batchWriteResult);
        writeResult.merge(batchWriteResult);

        insertionTime += queryResult.getDbTime();
        return true;
    }

//    public boolean write_updateInsert(List<Variant> data) {
//        List<String> variantIds = new ArrayList<>(data.size());
//
//        Map<String, Variant> idVariantMap = new HashMap<>();
//        Set<String> existingVariants = new HashSet<>();
//
//        long start1 = System.nanoTime();
//        for (Variant variant : data) {
//            numVariantsWritten++;
//            if(numVariantsWritten % 1000 == 0) {
//                logger.info("Num variants written " + numVariantsWritten);
//            }
//            String id = variantConverter.buildStorageId(variant);
//            variantIds.add(id);
//            idVariantMap.put(id, variant);
//        }
//
//        long startQuery = System.nanoTime();
//        BasicDBObject query = new BasicDBObject("_id", new BasicDBObject("$in", variantIds));
//        QueryResult<DBObject> idsResult =
//                variantMongoCollection.find(query, new BasicDBObject("_id", true), null);
//        this.checkExistsDBTime += System.nanoTime() - startQuery;
//
//        for (DBObject dbObject : idsResult.getResult()) {
//            String id = dbObject.get("_id").toString();
//            existingVariants.add(id);
//        }
//        this.checkExistsTime += System.nanoTime() - start1;
//
//
//
//        for (Variant variant : data) {
//            variant.setAnnotation(null);
//            String id = variantConverter.buildStorageId(variant);
//
//            if (!existingVariants.contains(id)) {
//                DBObject variantDocument = variantConverter.convertToStorageType(variant);
//                List<DBObject> mongoFiles = new LinkedList<>();
//                for (VariantSourceEntry variantSourceEntry : variant.getStudies()) {
//                    if (!variantSourceEntry.getFileId().equals(fileId)) {
//                        continue;
//                    }
//                    DBObject mongoFile = sourceEntryConverter.convertToStorageType(variantSourceEntry);
//                    mongoFiles.add(mongoFile);
//                }
//                variantDocument.put(DBObjectToVariantConverter.STUDIES_FIELD, mongoFiles);
//                bulk.insert(variantDocument);
//                currentBulkSize++;
//            } else {
//                for (VariantSourceEntry variantSourceEntry : variant.getStudies()) {
//                    if (!variantSourceEntry.getFileId().equals(fileId)) {
//                        continue;
//                    }
//                    bulk.find(new BasicDBObject("_id", id)).updateOne(new BasicDBObject().append("$push",
//                            new BasicDBObject(DBObjectToVariantConverter.STUDIES_FIELD, sourceEntryConverter.convertToStorageType(variantSourceEntry))));
//                    currentBulkSize++;
//                }
//            }
//        }
//        if (currentBulkSize >= bulkSize) {
//            executeBulk();
//        }
//
//        return true;
//    }
//
//    public boolean write_setOnInsert(List<Variant> data) {
////        numVariantsWritten += data.size();
////        if(numVariantsWritten % 1000 == 0) {
////            logger.info("Num variants written " + numVariantsWritten);
////        }
//        synchronized (variantSourceWritten) {
//            long l = staticNumVariantsWritten/1000;
//            staticNumVariantsWritten += data.size();
//            if (staticNumVariantsWritten/1000 != l) {
//                logger.info("Num variants written " + staticNumVariantsWritten);
//            }
//        }
//
//
//        for (Variant variant : data) {
//            variant.setAnnotation(null);
//            String id = variantConverter.buildStorageId(variant);
//
//            for (VariantSourceEntry variantSourceEntry : variant.getStudies()) {
//                if (!variantSourceEntry.getFileId().equals(fileId)) {
//                    continue;
//                }
//                BasicDBObject addToSet = new BasicDBObject(
//                        DBObjectToVariantConverter.STUDIES_FIELD,
//                        sourceEntryConverter.convertToStorageType(variantSourceEntry));
//
//                BasicDBObject update = new BasicDBObject()
//                        .append("$addToSet", addToSet)
//                        .append("$setOnInsert", variantConverter.convertToStorageType(variant));
//                if (variant.getIds() != null && !variant.getIds().isEmpty()) {
//                    addToSet.put(DBObjectToVariantConverter.IDS_FIELD, new BasicDBObject("$each", variant.getIds()));
//                }
//                bulk.find(new BasicDBObject("_id", id)).upsert().updateOne(update);
//                currentBulkSize++;
//            }
//
//        }
//        if (currentBulkSize >= bulkSize) {
//            executeBulk();
//        }
//        return true;
//    }

    @Override @Deprecated
    protected boolean buildBatchRaw(List<Variant> data) {
        return true;
    }

    @Override @Deprecated
    protected boolean buildEffectRaw(List<Variant> variants) {
        return false;
    }

    @Override @Deprecated
    protected boolean buildBatchIndex(List<Variant> data) {
        return false;
    }

    @Override @Deprecated
    protected boolean writeBatch(List<Variant> batch) {
        return true;
    }

//    private boolean writeSourceSummary(VariantSource source) {
//        DBObject studyMongo = sourceConverter.convertToStorageType(source);
//        DBObject query = new BasicDBObject(DBObjectToVariantSourceConverter.FILEID_FIELD, source.getFileId());
//        filesMongoCollection.update(query, studyMongo, new QueryOptions("upsert", true));
//        return true;
//    }
//
//    private boolean writeStudyConfiguration() {
//        DBObject studyMongo = new DBObjectToStudyConfigurationConverter().convertToStorageType(studyConfiguration);
//        //(DBObject) JSON.parse(new ObjectMapper().writeValueAsString(study).replace(".", "&#46;"));
//
//        DBObject query = new BasicDBObject("studyId", studyConfiguration.getStudyId());
//        filesMongoCollection.update(query, studyMongo, new QueryOptions("upsert", true));
//        return true;
//    }
    private boolean writeStudyConfiguration() {
        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, new QueryOptions());
        return true;
    }

    @Override
    public boolean post() {
//        if (currentBulkSize != 0) {
//            executeBulk();
//        }
        logger.debug("POST");
        if (!variantSourceWritten.getAndSet(true)) {
            if (writeStudyConfiguration) {
                writeStudyConfiguration();
            }
//            if (writeVariantSource) {
//                writeSourceSummary(source);
//            }

            List<Region> regions = coveredChromosomes.stream().map(Region::new).collect(Collectors.toList());
            dbAdaptor.fillFileGaps(fileId, new LinkedList<>(coveredChromosomes), fileSampleIds, studyConfiguration);
            dbAdaptor.createIndexes(new QueryOptions());
//            DBObject onBackground = new BasicDBObject("background", true);
//            variantMongoCollection.createIndex(new BasicDBObject("_at.chunkIds", 1), onBackground);
//            variantMongoCollection.createIndex(new BasicDBObject("annot.xrefs.id", 1), onBackground);
//            variantMongoCollection.createIndex(new BasicDBObject("annot.ct.so", 1), onBackground);
//            variantMongoCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.IDS_FIELD, 1), onBackground);
//            variantMongoCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1), onBackground);
//            variantMongoCollection.createIndex(
//                    new BasicDBObject(DBObjectToVariantConverter.STUDIES_FIELD + "." + DBObjectToVariantSourceEntryConverter.STUDYID_FIELD, 1)
//                            .append(DBObjectToVariantConverter.STUDIES_FIELD + "." + DBObjectToVariantSourceEntryConverter.FILEID_FIELD, 1), onBackground);
//            variantMongoCollection.createIndex(new BasicDBObject("st.maf", 1), onBackground);
//            variantMongoCollection.createIndex(new BasicDBObject("st.mgf", 1), onBackground);
//            variantMongoCollection.createIndex(
//                    new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1)
//                            .append(DBObjectToVariantConverter.START_FIELD, 1)
//                            .append(DBObjectToVariantConverter.END_FIELD, 1), onBackground);
//            logger.debug("sent order to create indices");
        }

//        logger.debug("checkExistsTime " + checkExistsTime / 1000000.0 + "ms ");
//        logger.debug("checkExistsDBTime " + checkExistsDBTime / 1000000.0 + "ms ");
//        logger.debug("bulkTime " + bulkTime / 1000000.0 + "ms ");
        logger.debug("insertionTime " + insertionTime / 1000000.0 + "ms ");
        return true;
    }

    @Override
    public boolean close() {
//        this.mongoDataStoreManager.close(this.mongoDataStore.getDb().getName());
        return true;
    }

    @Override
    public final void includeStats(boolean b) {
        includeStats = b;
    }

    public final void includeSrc(boolean b) {
        includeSrc = b;
    }

//    public void setVariantSource(VariantSource source) {
//        this.source = source;
//        writeVariantSource = source != null;
//    }

    @Override
    public final void includeSamples(boolean b) {
        includeSamples = b;
    }

    @Override @Deprecated
    public final void includeEffect(boolean b) {
    }
//
//    public void setCompressDefaultGenotype(boolean compressDefaultGenotype) {
//        this.compressDefaultGenotype = compressDefaultGenotype;
//    }
//
//    public void setDefaultGenotype(String defaultGenotype) {
//        this.defaultGenotype = defaultGenotype;
//    }

    public void setThreadSynchronizationBoolean(AtomicBoolean atomicBoolean) {
        this.variantSourceWritten = atomicBoolean;
    }

    private void setConverters() {

        sourceConverter = new DBObjectToVariantSourceConverter();
        statsConverter = includeStats ? new DBObjectToVariantStatsConverter(dbAdaptor.getStudyConfigurationManager()) : null;
        sampleConverter = includeSamples ? new DBObjectToSamplesConverter(studyConfiguration) : null;

        sourceEntryConverter = new DBObjectToStudyVariantEntryConverter(includeSrc, sampleConverter);

        sourceEntryConverter.setIncludeSrc(includeSrc);

        // Do not create the VariantConverter with the sourceEntryConverter.
        // The variantSourceEntry conversion will be done on demand to create a proper mongoDB update query.
        // variantConverter = new DBObjectToVariantConverter(sourceEntryConverter);
        variantConverter = new DBObjectToVariantConverter(null, statsConverter);
    }

//    public void setBulkSize(int bulkSize) {
//        this.bulkSize = bulkSize;
//    }
//
//    /**
//     * This sample Ids will be used at conversion, to replace sample name for some numerical Id.
//     * If this param is not provided, the variantSource.samplesPosition will be used instead.
//     * @param samplesIds    Map between sampleName and sampleId
//     */
//    public void setSamplesIds(Map<String, Integer> samplesIds) {
//        this.samplesIds = samplesIds;
//    }
//
//
//    private void executeBulk() {
//        logger.debug("Execute bulk. BulkSize : " + currentBulkSize);
//        long startBulk = System.nanoTime();
//        bulk.execute();
//        resetBulk();
//        this.bulkTime += System.nanoTime() - startBulk;
//    }
//
//    private void resetBulk() {
//        bulk = variantsCollection.initializeUnorderedBulkOperation();
//        currentBulkSize = 0;
//    }

    public void setWriteStudyConfiguration(boolean writeStudyConfiguration) {
        this.writeStudyConfiguration = writeStudyConfiguration;
    }

    public MongoDBVariantWriteResult getWriteResult() {
        return writeResult;
    }
}
