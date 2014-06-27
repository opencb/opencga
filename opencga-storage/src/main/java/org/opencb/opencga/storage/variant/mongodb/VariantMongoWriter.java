package org.opencb.opencga.storage.variant.mongodb;

import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.effect.VariantEffect;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.storage.variant.VariantDBWriter;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoWriter extends VariantDBWriter {

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
    
    private VariantSource file;

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
    private boolean includeSamples;

    private List<String> samples;
    private Map<String, Integer> conseqTypes;
    
    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceConverter sourceConverter;
    private DBObjectToArchivedVariantFileConverter archivedVariantFileConverter;
    
    private long numVariantsWritten;
    
    public VariantMongoWriter(VariantSource source, MongoCredentials credentials) {
        this(source, credentials, "variants", "files");
    }
    
    public VariantMongoWriter(VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection) {
        this(source, credentials, variantsCollection, filesCollection, false, false, false);
    }

    public VariantMongoWriter(VariantSource source, MongoCredentials credentials, String variantsCollection, String filesCollection,
            boolean includeSamples, boolean includeStats, boolean includeEffect) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.file = source;
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
        
        setConverters(this.includeStats, this.includeSamples, this.includeEffect);
        
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
            Logger.getLogger(VariantMongoWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return db != null;
    }

    @Override
    public boolean pre() {
        // Mongo collection creation
        filesCollection = db.getCollection(filesCollectionName);
        variantsCollection = db.getCollection(variantsCollectionName);

        return variantsCollection != null && filesCollection != null;
    }

    @Override
    public boolean write(Variant variant) {
        return write(Arrays.asList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
        buildBatchRaw(data);
        if (this.includeEffect) {
            buildEffectRaw(data);
        }
        buildBatchIndex(data);
        return writeBatch(data);
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
            for (ArchivedVariantFile archiveFile : v.getFiles().values()) {
                if (!archiveFile.getFileId().equals(file.getFileId())) {
                    continue;
                }
                
                if (this.includeSamples && samples.isEmpty() && archiveFile.getSamplesData().size() > 0) {
                    // First time a variant is loaded, the list of samples is populated. 
                    // This guarantees that samples are loaded only once to keep order among variants,
                    // and that they are loaded before needed by the ArchivedVariantFileConverter
                    samples.addAll(archiveFile.getSampleNames());
                }
                
                DBObject mongoFile = archivedVariantFileConverter.convertToStorageType(archiveFile);
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
            if (!v.getEffect().isEmpty()) {
                Set<BasicDBObject> effectsSet = new HashSet<>();

                for (VariantEffect effect : v.getEffect()) {
                    BasicDBObject object = getVariantEffectDBObject(effect);
                    effectsSet.add(object);
                    addConsequenceType(effect.getConsequenceTypeObo());
                    
                    soSet.add(object.get("so").toString());
                    if (object.containsField("geneName")) {
                        genesSet.add(object.get("geneName").toString());
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
        BasicDBObject object = new BasicDBObject("so", effect.getConsequenceTypeObo());
        object.append("featureId", effect.getFeatureId());
        if (effect.getGeneName() != null && !effect.getGeneName().isEmpty()) {
            object.append("geneName", effect.getGeneName());
        }
        return object;
    }
    
    @Override
    protected boolean buildBatchIndex(List<Variant> data) {
        variantsCollection.ensureIndex(new BasicDBObject("_at.chunkIds", 1));
        variantsCollection.ensureIndex(new BasicDBObject("_at.gn", 1));
        variantsCollection.ensureIndex(new BasicDBObject("_at.ct", 1));
        variantsCollection.ensureIndex(new BasicDBObject(DBObjectToVariantConverter.ID_FIELD, 1));
        variantsCollection.ensureIndex(new BasicDBObject(DBObjectToVariantConverter.CHROMOSOME_FIELD, 1));
        variantsCollection.ensureIndex(new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToArchivedVariantFileConverter.STUDYID_FIELD, 1)
                .append(DBObjectToVariantConverter.FILES_FIELD + "." + DBObjectToArchivedVariantFileConverter.FILEID_FIELD, 1), "studyAndFile");
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
                        Logger.getLogger(VariantMongoWriter.class.getName()).log(Level.SEVERE, wr.getError(), wr.getLastError());
                    }
                } catch(MongoInternalException ex) {
                    System.out.println(v);
                    Logger.getLogger(VariantMongoWriter.class.getName()).log(Level.SEVERE, v.getChromosome() + ":" + v.getStart(), ex);
                } catch(MongoException.DuplicateKey ex) {
                    Logger.getLogger(VariantMongoWriter.class.getName()).log(Level.WARNING, 
                            "Variant already existed: {0}:{1}", new Object[]{v.getChromosome(), v.getStart()});
                }
                
            } else { // It existed previously, was not fully built in this run and only files need to be updated
                // TODO How to do this efficiently, inserting all files at once?
                for (ArchivedVariantFile archiveFile : v.getFiles().values()) {
                    DBObject mongoFile = mongoFileMap.get(rowkey + "_" + archiveFile.getFileId());
                    BasicDBObject changes = new BasicDBObject().append("$addToSet", 
                            new BasicDBObject(DBObjectToVariantConverter.FILES_FIELD, mongoFile));
                    
                    wr = variantsCollection.update(query, changes, true, false);
                    if (!wr.getLastError().ok()) {
                        // TODO If not correct, retry?
                        Logger.getLogger(VariantMongoWriter.class.getName()).log(Level.SEVERE, wr.getError(), wr.getLastError());
                    }
                }
            }
            
        }

        mongoMap.clear();
        mongoFileMap.clear();

        numVariantsWritten += batch.size();
        Variant lastVariantInBatch = batch.get(batch.size()-1);
        Logger.getLogger(VariantMongoWriter.class.getName()).log(Level.INFO, "{0}\tvariants written upto position {1}:{2}", 
                new Object[]{numVariantsWritten, lastVariantInBatch.getChromosome(), lastVariantInBatch.getStart()});
        
        return true;
    }

    private boolean writeSourceSummary(VariantSource source) {
        DBObject studyMongo = sourceConverter.convertToStorageType(source);
        DBObject query = new BasicDBObject(DBObjectToVariantSourceConverter.FILENAME_FIELD, source.getFileName());
        WriteResult wr = filesCollection.update(query, studyMongo, true, false);

        return wr.getLastError().ok(); // TODO Is this a proper return statement?
    }

    @Override
    public boolean post() {
        writeSourceSummary(file);
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
        setConverters(includeStats, includeSamples, includeEffect);
    }

    @Override
    public final void includeSamples(boolean b) {
        includeSamples = b;
        setConverters(includeStats, includeSamples, includeEffect);
    }

    @Override
    public final void includeEffect(boolean b) {
        includeEffect = b;
        setConverters(includeStats, includeSamples, includeEffect);
    }

    private void setConverters(boolean includeStats, boolean includeSamples, boolean includeEffect) {
        sourceConverter = new DBObjectToVariantSourceConverter();
        statsConverter = new DBObjectToVariantStatsConverter();
        archivedVariantFileConverter = new DBObjectToArchivedVariantFileConverter(
                includeSamples ? samples : null,
                includeStats ? statsConverter : null);
        // TODO Not sure about commenting this, but otherwise it looks like the ArchiveVariantFile will be processed twice
//        variantConverter = new DBObjectToVariantConverter(archivedVariantFileConverter);
        variantConverter = new DBObjectToVariantConverter();
    }
    
    private void addConsequenceType(String ct) {
        int ctCount = conseqTypes.containsKey(ct) ? conseqTypes.get(ct) : 1;
        conseqTypes.put(ct, ctCount);
    }

}
