package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.annotation.VariantEffect;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;
import org.opencb.opencga.storage.core.variant.io.VariantDBWriter;
import org.slf4j.LoggerFactory;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBWriter extends VariantDBWriter {

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
//    public static final Logger logger = Logger.getLogger(VariantMongoDBWriter.class.getName());
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(VariantDBWriter.class);

    private final VariantSource source;

    private MongoCredentials credentials;

    private MongoDataStore mongoDataStore;
    private MongoDataStoreManager mongoDataStoreManager;
    private MongoDBCollection variantMongoCollection;
    private MongoDBCollection filesMongoCollection;

    private String filesCollectionName;
    private String variantsCollectionName;

    @Deprecated private MongoClient mongoClient;
    @Deprecated private DB db;
    @Deprecated private DBCollection filesCollection;
    @Deprecated private DBCollection variantsCollection;

    @Deprecated private Map<String, DBObject> mongoMap;
    @Deprecated private Map<String, DBObject> mongoFileMap;


    private boolean includeStats;
    @Deprecated private boolean includeEffect;
    private boolean includeSrc = true;
    private boolean includeSamples;
    private boolean compressDefaultGenotype = true;
    private String defaultGenotype = null;

    private Map<String, Integer> samplesIds;
    @Deprecated private List<String> samples;
    @Deprecated private Map<String, Integer> conseqTypes;

    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceConverter sourceConverter;
    private DBObjectToVariantSourceEntryConverter sourceEntryConverter;
    private DBObjectToSamplesConverter sampleConverter;

    private long numVariantsWritten;

    private BulkWriteOperation bulk;
    private int currentBulkSize = 0;
    private int bulkSize = 0;

    private long checkExistsTime = 0;
    private long checkExistsDBTime = 0;
    private long bulkTime = 0;


    public VariantMongoDBWriter(final VariantSource source, MongoCredentials credentials) {
        this(source, credentials, "variants", "files");
    }

    public VariantMongoDBWriter(final VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection) {
        this(source, credentials, variantsCollection, filesCollection, false, false, false);
    }

    public VariantMongoDBWriter(final VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection,
                                boolean includeSamples, boolean includeStats, @Deprecated boolean includeEffect) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.source = source;
        this.credentials = credentials;
        this.filesCollectionName = filesCollection;
        this.variantsCollectionName = variantsCollection;

        this.mongoMap = new HashMap<>();
        this.mongoFileMap = new HashMap<>();

        this.includeSamples = includeSamples;
        this.includeStats = includeStats;
        this.includeEffect = includeEffect;

        conseqTypes = new LinkedHashMap<>();
        samples = new ArrayList<>();

        numVariantsWritten = 0;
    }

    @Override
    public boolean open() {
        try {
            // Mongo configuration
            ServerAddress address = new ServerAddress(credentials.getMongoHost(), credentials.getMongoPort());
            if (credentials.getMongoCredentials() != null) {
                mongoClient = new MongoClient(address, Arrays.asList(credentials.getMongoCredentials()));
            } else {
                mongoClient = new MongoClient(address);
            }
            db = mongoClient.getDB(credentials.getMongoDbName());
        } catch (UnknownHostException ex) {
            Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        mongoDataStoreManager = new MongoDataStoreManager(credentials.getMongoHost(), credentials.getMongoPort());
        MongoDBConfiguration mongoDBConfiguration = MongoDBConfiguration.builder().init()
                .add("username", credentials.getUsername())
                .add("password", credentials.getPassword())
                .build();
        mongoDataStore = mongoDataStoreManager.get(credentials.getMongoDbName(), mongoDBConfiguration);

        return mongoDataStore != null;
    }

    @Override
    public boolean pre() {
        // Mongo collection creation
        variantMongoCollection = mongoDataStore.getCollection(variantsCollectionName);
        filesMongoCollection = mongoDataStore.getCollection(filesCollectionName);

        filesCollection = db.getCollection(filesCollectionName);
        variantsCollection = db.getCollection(variantsCollectionName);

        setConverters();

        resetBulk();

        return variantMongoCollection != null && filesMongoCollection != null;
    }

    @Override
    public boolean write(Variant variant) {
        return write(Collections.singletonList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
        return write_setOnInsert(data);
    }

    @Deprecated public boolean write_old(List<Variant> data) {
        buildBatchRaw(data);
        if (this.includeEffect) {
            buildEffectRaw(data);
        }
        buildBatchIndex(data);
        return writeBatch(data);
    }


    public boolean write_updateInsert(List<Variant> data) {
        List<String> variantIds = new ArrayList<>(data.size());

        Map<String, Variant> idVariantMap = new HashMap<>();
        Set<String> existingVariants = new HashSet<>();

        long start1 = System.nanoTime();
        for (Variant variant : data) {
            numVariantsWritten++;
            if(numVariantsWritten % 1000 == 0) {
                logger.info("Num variants written " + numVariantsWritten);
            }
            String id = variantConverter.buildStorageId(variant);
            variantIds.add(id);
            idVariantMap.put(id, variant);
        }

        long startQuery = System.nanoTime();
        BasicDBObject query = new BasicDBObject("_id", new BasicDBObject("$in", variantIds));
        QueryResult<DBObject> idsResult =
                variantMongoCollection.find(query, new BasicDBObject("_id", true), null);
        this.checkExistsDBTime += System.nanoTime() - startQuery;

        for (DBObject dbObject : idsResult.getResult()) {
            String id = dbObject.get("_id").toString();
            existingVariants.add(id);
        }
        this.checkExistsTime += System.nanoTime() - start1;



        for (Variant variant : data) {
            variant.setAnnotation(null);
            String id = variantConverter.buildStorageId(variant);

            if (!existingVariants.contains(id)) {
                DBObject variantDocument = variantConverter.convertToStorageType(variant);
                List<DBObject> mongoFiles = new LinkedList<>();
                for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                    if (!variantSourceEntry.getFileId().equals(source.getFileId())) {
                        continue;
                    }
                    DBObject mongoFile = sourceEntryConverter.convertToStorageType(variantSourceEntry);
                    mongoFiles.add(mongoFile);
                }
                variantDocument.put(DBObjectToVariantConverter.FILES_FIELD, mongoFiles);
                bulk.insert(variantDocument);
                currentBulkSize++;
            } else {
                for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                    if (!variantSourceEntry.getFileId().equals(source.getFileId())) {
                        continue;
                    }
                    bulk.find(new BasicDBObject("_id", id)).updateOne(new BasicDBObject().append("$push",
                            new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD, sourceEntryConverter.convertToStorageType(variantSourceEntry))));
                    currentBulkSize++;
                }
            }
        }
        if (currentBulkSize >= bulkSize) {
            executeBulk();
        }

        return true;
    }

    public boolean write_setOnInsert(List<Variant> data) {
            numVariantsWritten += data.size();
            if(numVariantsWritten % 1000 == 0) {
                logger.info("Num variants written " + numVariantsWritten);
            }


        for (Variant variant : data) {
            variant.setAnnotation(null);
            String id = variantConverter.buildStorageId(variant);

            for (VariantSourceEntry variantSourceEntry : variant.getSourceEntries().values()) {
                    if (!variantSourceEntry.getFileId().equals(source.getFileId())) {
                        continue;
                    }
                BasicDBObject update = new BasicDBObject()
                        .append("$push",
                                new BasicDBObject(
                                        DBObjectToVariantConverter.FILES_FIELD,
                                        sourceEntryConverter.convertToStorageType(variantSourceEntry)))
                        .append("$setOnInsert", variantConverter.convertToStorageType(variant));
                if (variant.getIds() != null && !variant.getIds().isEmpty()) {
                    update.append("$addToSet",
                            new BasicDBObject(
                                    DBObjectToVariantConverter.IDS_FIELD,
                                    new BasicDBObject(
                                            "$each",
                                            variant.getIds())));
                }
//                System.out.println("update = " + update);
                bulk.find(new BasicDBObject("_id", id)).upsert().updateOne(update);
                currentBulkSize++;
            }

        }
        if (currentBulkSize >= bulkSize) {
            executeBulk();
        }
        return true;
    }

    @Override @Deprecated
    protected boolean buildBatchRaw(List<Variant> data) {
        for (Variant v : data) {
            // Check if this variant is already stored
            String rowkey = variantConverter.buildStorageId(v);
            DBObject mongoVariant = new BasicDBObject("_id", rowkey);

            if (variantsCollection.count(mongoVariant) == 0) {
                mongoVariant = variantConverter.convertToStorageType(v);
            } /*else {
                System.out.println("Variant " + v.getChromosome() + ":" + v.getStart() + "-" + v.getEnd() + " already found");
            }*/

            BasicDBList mongoFiles = new BasicDBList();
            for (VariantSourceEntry archiveFile : v.getSourceEntries().values()) {
                if (!archiveFile.getFileId().equals(source.getFileId())) {
                    continue;
                }

                if (this.includeSamples && samples.isEmpty() && archiveFile.getSamplesData().size() > 0) {
                    // First time a variant is loaded, the list of samples is populated.
                    // This guarantees that samples are loaded only once to keep order among variants,
                    // and that they are loaded before needed by the ArchivedVariantFileConverter
                    samples.addAll(archiveFile.getSampleNames());
                }

                DBObject mongoFile = sourceEntryConverter.convertToStorageType(archiveFile);
                mongoFiles.add(mongoFile);
                mongoFileMap.put(rowkey + "_" + archiveFile.getFileId(), mongoFile);
            }

            mongoVariant.put(DBObjectToVariantConverter.FILES_FIELD, mongoFiles);
            mongoMap.put(rowkey, mongoVariant);
        }

        return true;
    }

    @Override @Deprecated
    protected boolean buildEffectRaw(List<Variant> variants) {
        for (Variant v : variants) {
            DBObject mongoVariant = mongoMap.get(variantConverter.buildStorageId(v));

            if (!mongoVariant.containsField(DBObjectToVariantConverter.CHROMOSOME_FIELD)) {
                // TODO It means that the same position was already found in this file, so __for now__ it won't be processed again
                continue;
            }

            Set<String> genesSet = new HashSet<>();
            Set<String> soSet = new HashSet<>();

            // Add effects to file
            if (!v.getAnnotation().getEffects().isEmpty()) {
                Set<BasicDBObject> effectsSet = new HashSet<>();

                for (List<VariantEffect> effects : v.getAnnotation().getEffects().values()) {
                    for (VariantEffect effect : effects) {
                        BasicDBObject object = getVariantEffectDBObject(effect);
                        effectsSet.add(object);

                        addConsequenceType(effect);
                        soSet.addAll(Arrays.asList((String[]) object.get("so")));
                        if (object.containsField("geneName")) {
                            genesSet.add(object.get("geneName").toString());
                        }
                    }
                }

                BasicDBList effectsList = new BasicDBList();
                effectsList.addAll(effectsSet);
                mongoVariant.put("effects", effectsList);
            }

            // Add gene fields directly to the variant, for query optimization purposes
            BasicDBObject _at = (BasicDBObject) mongoVariant.get("_at");
            if (!genesSet.isEmpty()) {
                BasicDBList genesList = new BasicDBList(); genesList.addAll(genesSet);
                _at.append("gn", genesList);
            }
            if (!soSet.isEmpty()) {
                BasicDBList soList = new BasicDBList(); soList.addAll(soSet);
                _at.append("ct", soList);
            }
        }

        return false;
    }

    @Deprecated
    private BasicDBObject getVariantEffectDBObject(VariantEffect effect) {
        String[] consequenceTypes = new String[effect.getConsequenceTypes().length];
        for (int i = 0; i < effect.getConsequenceTypes().length; i++) {
            consequenceTypes[i] = ConsequenceTypeMappings.accessionToTerm.get(effect.getConsequenceTypes()[i]);
        }

        BasicDBObject object = new BasicDBObject("so", consequenceTypes).append("featureId", effect.getFeatureId());
        if (effect.getGeneName() != null && !effect.getGeneName().isEmpty()) {
            object.append("geneName", effect.getGeneName());
        }
        return object;
    }

    @Override @Deprecated
    protected boolean buildBatchIndex(List<Variant> data) {
//        variantsCollection.createIndex(new BasicDBObject("_at.chunkIds", 1));
//        variantsCollection.createIndex(new BasicDBObject("_at.gn", 1));
//        variantsCollection.createIndex(new BasicDBObject("_at.ct", 1));
//        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.ID_FIELD, 1));
//        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1));
//        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.STUDYID_FIELD, 1)
//                .append(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.FILEID_FIELD, 1));
        return true;
    }

    @Override @Deprecated
    protected boolean writeBatch(List<Variant> batch) {
        for (Variant v : batch) {
            String rowkey = variantConverter.buildStorageId(v);
            DBObject mongoVariant = mongoMap.get(rowkey);
            DBObject query = new BasicDBObject("_id", rowkey);
            WriteResult wr;

            if (mongoVariant.containsField(DBObjectToVariantConverter.CHROMOSOME_FIELD)) {
                // Was fully built in this run because it didn't exist, and must be inserted
                try {
                    wr = variantsCollection.insert(mongoVariant);
                    if (!wr.getLastError().ok()) {
                        // TODO If not correct, retry?
                        Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, wr.getError(), wr.getLastError());
                    }
                } catch(MongoInternalException ex) {
                    System.out.println(v);
                    Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, v.getChromosome() + ":" + v.getStart(), ex);
                } catch(DuplicateKeyException ex) {
                    Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.WARNING,
                            "Variant already existed: {0}:{1}", new Object[]{v.getChromosome(), v.getStart()});
                }

            } else { // It existed previously, was not fully built in this run and only files need to be updated
                // TODO How to do this efficiently, inserting all files at once?
                for (VariantSourceEntry archiveFile : v.getSourceEntries().values()) {
                    DBObject mongoFile = mongoFileMap.get(rowkey + "_" + archiveFile.getFileId());
                    BasicDBObject changes = new BasicDBObject().append("$addToSet",
                            new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD, mongoFile));

                    wr = variantsCollection.update(query, changes, true, false);
                    if (!wr.getLastError().ok()) {
                        // TODO If not correct, retry?
                        Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.SEVERE, wr.getError(), wr.getLastError());
                    }
                }
            }

        }

        mongoMap.clear();
        mongoFileMap.clear();

        numVariantsWritten += batch.size();
        Variant lastVariantInBatch = batch.get(batch.size()-1);
        Logger.getLogger(VariantMongoDBWriter.class.getName()).log(Level.INFO, "{0}\tvariants written upto position {1}:{2}",
                new Object[]{numVariantsWritten, lastVariantInBatch.getChromosome(), lastVariantInBatch.getStart()});

        return true;
    }

    private boolean writeSourceSummary(VariantSource source) {
        DBObject studyMongo = sourceConverter.convertToStorageType(source);
        DBObject query = new BasicDBObject(DBObjectToVariantSourceConverter.FILEID_FIELD, source.getFileName());
        filesMongoCollection.update(query, studyMongo, new QueryOptions("upsert", true));
        return true;
    }

    @Override
    public boolean post() {
        if (currentBulkSize != 0) {
            executeBulk();
        }
        logger.info("POST");
        writeSourceSummary(source);
        logger.debug("checkExistsTime " + checkExistsTime / 1000000.0 + "ms ");
        logger.debug("checkExistsDBTime " + checkExistsDBTime / 1000000.0 + "ms ");
        logger.debug("bulkTime " + bulkTime / 1000000.0 + "ms ");
        return true;
    }

    @Override
    public boolean close() {
        mongoClient.close();
        this.mongoDataStoreManager.close(this.mongoDataStore.getDb().getName());
        return true;
    }

    @Override
    public final void includeStats(boolean b) {
        includeStats = b;
    }

    public final void includeSrc(boolean b) {
        includeSrc = b;
    }

    @Override
    public final void includeSamples(boolean b) {
        includeSamples = b;
    }

    @Override @Deprecated
    public final void includeEffect(boolean b) {
        includeEffect = b;
    }

    public void setCompressDefaultGenotype(boolean compressDefaultGenotype) {
        this.compressDefaultGenotype = compressDefaultGenotype;
    }

    public void setDefaultGenotype(String defaultGenotype) {
        this.defaultGenotype = defaultGenotype;
    }

    private void setConverters() {

        if (samplesIds == null || samplesIds.isEmpty()) {
            logger.info("Using sample position as sample id");
            samplesIds = source.getSamplesPosition();
        }

        sourceConverter = new DBObjectToVariantSourceConverter();
        statsConverter = includeStats ? new DBObjectToVariantStatsConverter() : null;
        sampleConverter = includeSamples ? new DBObjectToSamplesConverter(compressDefaultGenotype, samplesIds): null; //TODO: Add default genotype

        sourceEntryConverter = new DBObjectToVariantSourceEntryConverter(
                includeSrc,
                sampleConverter
        );
        sourceEntryConverter.setIncludeSrc(includeSrc);

        // Do not create the VariantConverter with the sourceEntryConverter.
        // The variantSourceEntry conversion will be done on demand to create a proper mongoDB update query.
        // variantConverter = new DBObjectToVariantConverter(sourceEntryConverter);
        variantConverter = new DBObjectToVariantConverter();
    }

    @Deprecated private void addConsequenceType(VariantEffect effect) {
        for (int so : effect.getConsequenceTypes()) {
            String ct = ConsequenceTypeMappings.accessionToTerm.get(so);
            int ctCount = conseqTypes.containsKey(ct) ? conseqTypes.get(ct)+1 : 1;
            conseqTypes.put(ct, ctCount);
        }
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    /**
     * This sample Ids will be used at conversion, to replace sample name for some numerical Id.
     * If this param is not provided, the variantSource.samplesPosition will be used instead.
     * @param samplesIds    Map between sampleName and sampleId
     */
    public void setSamplesIds(Map<String, Integer> samplesIds) {
        this.samplesIds = samplesIds;
    }


    private void executeBulk() {
        logger.debug("Execute bulk. BulkSize : " + currentBulkSize);
        long startBulk = System.nanoTime();
        bulk.execute();
        resetBulk();
        this.bulkTime += System.nanoTime() - startBulk;
    }

    private void resetBulk() {
        bulk = variantsCollection.initializeUnorderedBulkOperation();
        currentBulkSize = 0;
    }

}
