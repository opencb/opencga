package org.opencb.opencga.storage.variant;

import com.mongodb.*;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.ArchivedVariantFile;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.effect.VariantEffect;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.lib.auth.MongoCredentials;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantVcfMongoDataWriter extends VariantDBWriter {

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
    
    private VariantSource file;

    private MongoClient mongoClient;
    private DB db;
    private DBCollection filesCollection;
    private DBCollection variantCollection;
    private Map<String, BasicDBObject> mongoMap;
    private Map<String, BasicDBObject> mongoFileMap;

    private MongoCredentials credentials;

    private boolean includeStats;
    private boolean includeEffect;
    private boolean includeSamples;

    private Map<String, Integer> conseqTypes;
    
    private BufferedWriter writer;

    public VariantVcfMongoDataWriter(VariantSource source, String species, MongoCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.file = source;
        this.credentials = credentials;
        this.mongoMap = new HashMap<>();
        this.mongoFileMap = new HashMap<>();

        conseqTypes = new LinkedHashMap<>();
//        try {
//            writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(source.getName() + ".json.gz"))));
//        } catch (IOException ex) {
//            Logger.getLogger(VariantVcfMongoDataWriter.class.getName()).log(Level.SEVERE, null, ex);
//        }
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
            Logger.getLogger(VariantVcfMongoDataWriter.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }

        return db != null;
    }

    @Override
    public boolean pre() {
        // Mongo collection creation
        filesCollection = db.getCollection("files");
        variantCollection = db.getCollection("variants");

        return variantCollection != null && filesCollection != null;
    }

    @Override
    public boolean write(Variant variant) {
        return write(Arrays.asList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
        buildBatchRaw(data);
//        if (this.includeStats) {
            buildStatsRaw(data);
//        }
//        if (this.includeEffect) {
            buildEffectRaw(data);
//        }
        buildBatchIndex(data);
        return writeBatch(data);
    }

    @Override
    boolean buildBatchRaw(List<Variant> data) {
        for (Variant v : data) {
            // Check if this variant is already stored
            String rowkey = buildRowkey(v);
            BasicDBObject mongoVariant = new BasicDBObject("_id", rowkey);

            if (variantCollection.count(mongoVariant) == 0) {
                mongoVariant = getVariantDBObject(v, rowkey);
            } /*else {
                System.out.println("Variant " + v.getChromosome() + ":" + v.getStart() + "-" + v.getEnd() + " already found");
            }*/
            
            BasicDBList mongoFiles = new BasicDBList();
            for (ArchivedVariantFile archiveFile : v.getFiles().values()) {
//                BasicDBObject mongoFile = new BasicDBObject("fileName", archiveFile.getFileName()).append("fileId", archiveFile.getFileId());
                BasicDBObject mongoFile = new BasicDBObject("fileId", archiveFile.getFileId());
                mongoFile.append("studyId", file.getAlias());

                // Attributes
                if (archiveFile.getAttributes().size() > 0) {
                    BasicDBObject attrs = null;
                    for (Map.Entry<String, String> entry : archiveFile.getAttributes().entrySet()) {
                        if (attrs == null) {
                            attrs = new BasicDBObject(entry.getKey(), entry.getValue());
                        } else {
                            attrs.append(entry.getKey(), entry.getValue());
                        }
                    }

                    if (attrs != null) {
                        mongoFile.put("attributes", attrs);
                    }
                }

                // Samples
                if (this.includeSamples && archiveFile.getSamplesData().size() > 0) {
                    BasicDBObject samples = new BasicDBObject();

                    for (Map.Entry<String, Map<String, String>> entry : archiveFile.getSamplesData().entrySet()) {
                        BasicDBObject sampleData = new BasicDBObject();
                        for (Map.Entry<String, String> sampleEntry : entry.getValue().entrySet()) {
                            sampleData.put(sampleEntry.getKey(), sampleEntry.getValue());
                        }
                        samples.put(entry.getKey(), sampleData);
                    }
                    mongoFile.put("samples", samples);
                }

                mongoFiles.add(mongoFile);
                mongoFileMap.put(rowkey + "_" + archiveFile.getFileId(), mongoFile);
            }
            
            mongoVariant.append("files", mongoFiles);
            mongoMap.put(rowkey, mongoVariant);
        }

        return true;
    }

    private BasicDBObject getVariantDBObject(Variant v, String rowkey) {
        // Attributes easily calculated
        BasicDBObject object = new BasicDBObject("_id", rowkey).append("id", v.getId()).append("type", v.getType().name());
        object.append("chr", v.getChromosome()).append("start", v.getStart()).append("end", v.getStart());
        object.append("length", v.getLength()).append("ref", v.getReference()).append("alt", v.getAlternate());
        
        // ChunkID (1k and 10k)
        String chunkSmall = v.getChromosome() + "_" + v.getStart() / CHUNK_SIZE_SMALL + "_" + CHUNK_SIZE_SMALL / 1000 + "k";
        String chunkBig = v.getChromosome() + "_" + v.getStart() / CHUNK_SIZE_BIG + "_" + CHUNK_SIZE_BIG / 1000 + "k";
        BasicDBList chunkIds = new BasicDBList(); chunkIds.add(chunkSmall); chunkIds.add(chunkBig);
        object.append("chunkIds", chunkIds);
        
        // Transform HGVS: Map of lists -> List of map entries
        BasicDBList hgvs = new BasicDBList();
        for (Map.Entry<String, List<String>> entry : v.getHgvs().entrySet()) {
            for (String value : entry.getValue()) {
                hgvs.add(new BasicDBObject("type", entry.getKey()).append("name", value));
            }
        }
        object.append("hgvs", hgvs);
        
        return object;
    }
    
    @Override
    boolean buildStatsRaw(List<Variant> data) {
        for (Variant v : data) {
            for (ArchivedVariantFile archiveFile : v.getFiles().values()) {
                VariantStats vs = archiveFile.getStats();
                if (vs == null) {
                    continue;
                }

                // Generate genotype counts
                BasicDBObject genotypes = new BasicDBObject();

                for (Genotype g : vs.getGenotypes()) {
                    String count = (g.getAllele1() == null ? -1 : g.getAllele1()) + "/" + (g.getAllele2() == null ? -1 : g.getAllele2());
                    genotypes.append(count, g.getCount());
                }

                BasicDBObject mongoStats = new BasicDBObject("maf", vs.getMaf());
                mongoStats.append("mgf", vs.getMgf());
                mongoStats.append("alleleMaf", vs.getMafAllele());
                mongoStats.append("genotypeMaf", vs.getMgfGenotype());
                mongoStats.append("missAllele", vs.getMissingAlleles());
                mongoStats.append("missGenotypes", vs.getMissingGenotypes());
                mongoStats.append("mendelErr", vs.getMendelianErrors());
                mongoStats.append("casesPercentDominant", vs.getCasesPercentDominant());
                mongoStats.append("controlsPercentDominant", vs.getControlsPercentDominant());
                mongoStats.append("casesPercentRecessive", vs.getCasesPercentRecessive());
                mongoStats.append("controlsPercentRecessive", vs.getControlsPercentRecessive());
                mongoStats.append("genotypeCount", genotypes);

                BasicDBObject mongoFile = mongoFileMap.get(buildRowkey(v) + "_" + archiveFile.getFileId());
                mongoFile.put("stats", mongoStats);
            }
        }

        return true;
    }

    @Override
    boolean buildEffectRaw(List<Variant> variants) {
        for (Variant v : variants) {
            BasicDBObject mongoVariant = mongoMap.get(buildRowkey(v));

            if (!mongoVariant.containsField("chr")) {
                // TODO It means that the same position was already found in this file, so __for now__ it won't be processed again
                continue;
            }

            // Add effects to file
            if (!v.getEffect().isEmpty()) {
                BasicDBList effectsSet = new BasicDBList();

                for (VariantEffect effect : v.getEffect()) {
                    effectsSet.add(getVariantEffectDBObject(effect));
                    addConsequenceType(effect.getConsequenceTypeObo());
                }
                
                mongoVariant.put("effects", effectsSet);
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
    boolean buildBatchIndex(List<Variant> data) {
//        variantCollection.ensureIndex(new BasicDBObject("chr", 1).append("start", 1));
        variantCollection.ensureIndex(new BasicDBObject("files.studyId", 1));
        variantCollection.ensureIndex(new BasicDBObject("chunkIds", 1));
        return true;
    }

    @Override
    boolean writeBatch(List<Variant> data) {
        for (Variant v : data) {
            String rowkey = buildRowkey(v);
            BasicDBObject mongoVariant = mongoMap.get(rowkey);
            BasicDBObject query = new BasicDBObject("_id", rowkey);
            WriteResult wr;
            
            if (mongoVariant.containsField("chr")) {
                // Was fully built in this run because it didn't exist, and must be inserted
                wr = variantCollection.insert(mongoVariant);
//                try {
//                    writer.write(mongoVariant.toString() + "\n") ;
//                } catch (IOException ex) {
//                    Logger.getLogger(VariantVcfMongoDataWriter.class.getName()).log(Level.SEVERE, null, ex);
//                }
            } else { // It existed previously, was not fully built in this run and only files need to be updated
                // TODO How to do this efficiently, inserting all files at once?
                for (ArchivedVariantFile archiveFile : v.getFiles().values()) {
                    BasicDBObject mongoFile = mongoFileMap.get(rowkey + "_" + archiveFile.getFileId());
                    BasicDBObject changes = new BasicDBObject().append("$push", new BasicDBObject("files", mongoFile));
                    wr = variantCollection.update(query, changes, true, false);
                }
            }
            
//            if (!wr.getLastError().ok()) {
//                // TODO If not correct, retry?
//                return false;
//            }
        }

        mongoMap.clear();
        mongoFileMap.clear();

        return true;
    }

    private boolean writeStudy(VariantSource study) {
        String timeStamp = new SimpleDateFormat("dd/mm/yyyy").format(Calendar.getInstance().getTime());
        BasicDBObject studyMongo = new BasicDBObject("name", study.getName())
                .append("alias", study.getAlias())
                .append("date", timeStamp)
                .append("authors", study.getAuthors())
                .append("samples", study.getSamples())
                .append("description", study.getDescription())
                .append("files", study.getSources());

        BasicDBObject cts = new BasicDBObject();

        for (Map.Entry<String, Integer> entry : conseqTypes.entrySet()) {
            cts.append(entry.getKey(), entry.getValue());
        }

        VariantGlobalStats global = study.getStats();
        if (global != null) {
            DBObject globalStats = new BasicDBObject("samplesCount", global.getSamplesCount())
                    .append("variantsCount", global.getVariantsCount())
                    .append("snpCount", global.getSnpsCount())
                    .append("indelCount", global.getIndelsCount())
                    .append("passCount", global.getPassCount())
                    .append("transitionsCount", global.getTransitionsCount())
                    .append("transversionsCount", global.getTransversionsCount())
//                    .append("biallelicsCount", global.getBiallelicsCount())
//                    .append("multiallelicsCount", global.getMultiallelicsCount())
                    .append("accumulatedQuality", global.getAccumQuality()).append("consequenceTypes", cts);

            studyMongo = studyMongo.append("globalStats", globalStats);
        } else {
            // TODO Notify?
            studyMongo.append("globalStats", new BasicDBObject("consequenceTypes", cts));

        }

        // TODO Save pedigree information
        Map<String, String> meta = study.getMetadata();
        DBObject metadataMongo = new BasicDBObjectBuilder()
                .add("header", meta.get("variantFileHeader"))
                .get();
        studyMongo = studyMongo.append("metadata", metadataMongo);

        DBObject query = new BasicDBObject("name", study.getName());
        WriteResult wr = filesCollection.update(query, studyMongo, true, false);

        filesCollection.ensureIndex(new BasicDBObject("name", 1));

        return wr.getLastError().ok(); // TODO Is this a proper return statement?
    }

    @Override
    public boolean post() {
        writeStudy(file);
        return true;
    }

    @Override
    public boolean close() {
        mongoClient.close();
//        try {
//            writer.close();
//        } catch (IOException ex) {
//            Logger.getLogger(VariantVcfMongoDataWriter.class.getName()).log(Level.SEVERE, null, ex);
//        }
        return true;
    }

    private void addConsequenceType(String ct) {
        int ctCount = conseqTypes.containsKey(ct) ? conseqTypes.get(ct) : 1;
        conseqTypes.put(ct, ctCount);
    }

    private String buildRowkey(Variant v) {
        StringBuilder builder = new StringBuilder(v.getChromosome());
        builder.append("_");
        builder.append(v.getStart());
        builder.append("_");
        builder.append(v.getReference());
        builder.append("_");
        builder.append(v.getAlternate());
        return builder.toString();
    }
    
    @Override
    public final void includeStats(boolean b) {
        this.includeStats = b;
    }

    @Override
    public final void includeSamples(boolean b) {
        this.includeSamples = b;
    }

    @Override
    public final void includeEffect(boolean b) {
        this.includeEffect = b;
    }

}
