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

    private VariantSource source;

    private MongoClient mongoClient;
    private DB db;

    private String filesCollectionName;
    private String variantsCollectionName;
    private DBCollection filesCollection;
    private DBCollection variantsCollection;

    private Map<String, DBObject> mongoMap;
    private Map<String, DBObject> mongoFileMap;

    private MongoCredentials credentials;

    private boolean includeStats;
    private boolean includeEffect;
    private boolean includeSrc = true;
    private boolean includeSamples;
    private boolean compressSamples = true;
    private String defaultGenotype = null;

    private List<String> samples;

    private Map<String, Integer> conseqTypes;
    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceConverter sourceConverter;

    private DBObjectToVariantSourceEntryConverter sourceEntryConverter;

    private long numVariantsWritten;
    private MongoDataStore mongoDataStore;
    private MongoDataStoreManager mongoDataStoreManager;
    private MongoDBCollection variantMongoCollection;
    private MongoDBCollection filesMongoCollection;

    private BulkWriteOperation bulk;
    private int currentBulkSize = 0;
    private int bulkSize = 0;

    private long checkExistsTime = 0;
    private long checkExistsDBTime = 0;
    private long bulkTime = 0;


    public VariantMongoDBWriter(VariantSource source, MongoCredentials credentials) {
        this(source, credentials, "variants", "files");
    }

    public VariantMongoDBWriter(VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection) {
        this(source, credentials, variantsCollection, filesCollection, false, false, false);
    }

    public VariantMongoDBWriter(VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection,
                                boolean includeSamples, boolean includeStats, boolean includeEffect) {
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

//        return db != null;


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

    public boolean write_old(List<Variant> data) {
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
                variantMongoCollection.find(query, null, new BasicDBObject("_id", true));
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
                bulk.find(new BasicDBObject("_id", id)).upsert().updateOne(new BasicDBObject().append("$push",
                        new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD, sourceEntryConverter.convertToStorageType(variantSourceEntry)))
                        .append("$setOnInsert", variantConverter.convertToStorageType(variant))
                );
                currentBulkSize++;
            }

        }
        if (currentBulkSize >= bulkSize) {
            executeBulk();
        }
        return true;
    }

    @Override
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

    @Override
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

    @Override
    protected boolean buildBatchIndex(List<Variant> data) {
        variantsCollection.createIndex(new BasicDBObject("_at.chunkIds", 1));
        variantsCollection.createIndex(new BasicDBObject("_at.gn", 1));
        variantsCollection.createIndex(new BasicDBObject("_at.ct", 1));
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.ID_FIELD, 1));
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1));
        variantsCollection.createIndex(new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.STUDYID_FIELD, 1)
                .append(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToVariantSourceEntryConverter.FILEID_FIELD, 1));
        return true;
    }

    @Override
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
//        WriteResult wr = filesCollection.update(query, studyMongo, true, false);
        filesMongoCollection.update(query, studyMongo, true, false);
//        return wr.getLastError().ok(); // TODO Is this a proper return statement?
        return true;
    }

    @Override
    public boolean post() {
        if (currentBulkSize != 0) {
            executeBulk();
        }
        writeSourceSummary(source);
        logger.debug("checkExistsTime " + checkExistsTime / 1000000.0 + "ms ");
        logger.debug("checkExistsDBTime " + checkExistsDBTime / 1000000.0 + "ms ");
        logger.debug("bulkTime " + bulkTime / 1000000.0 + "ms ");
        return true;
    }

    @Override
    public boolean close() {
        mongoClient.close();
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

    @Override
    public final void includeEffect(boolean b) {
        includeEffect = b;
    }

    public void setCompressSamples(boolean compressSamples) {
        this.compressSamples = compressSamples;
    }

    public void setDefaultGenotype(String defaultGenotype) {
        this.defaultGenotype = defaultGenotype;
    }
    private void setConverters() {
//        switch (source.getType()) {
//            case FAMILY:
//            case TRIO:
//                compressSamples = true;
//                defaultValue = false;
//                break;
//            case CONTROL:
//            case CASE:
//            case CASE_CONTROL:
//            case COLLECTION:
//            default:
//                compressSamples = true;
//                defaultValue = true;
//        }

        samples = source.getSamples();

        sourceConverter = new DBObjectToVariantSourceConverter();
        statsConverter = new DBObjectToVariantStatsConverter();

        boolean useDefaultGenotype = defaultGenotype != null && !defaultGenotype.isEmpty();
        sourceEntryConverter = new DBObjectToVariantSourceEntryConverter(
                compressSamples, useDefaultGenotype,
                includeSamples ? samples : null,
                includeStats ? statsConverter : null);
        sourceEntryConverter.setIncludeSrc(includeSrc);
        // TODO Not sure about commenting this, but otherwise it looks like the ArchiveVariantFile will be processed twice
//        variantConverter = new DBObjectToVariantConverter(sourceEntryConverter);
        variantConverter = new DBObjectToVariantConverter();
    }

    private void addConsequenceType(VariantEffect effect) {
        for (int so : effect.getConsequenceTypes()) {
            String ct = ConsequenceTypeMappings.accessionToTerm.get(so);
            int ctCount = conseqTypes.containsKey(ct) ? conseqTypes.get(ct)+1 : 1;
            conseqTypes.put(ct, ctCount);
        }
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    private void executeBulk() {
        logger.debug("Current BulkSize : " + currentBulkSize);
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
