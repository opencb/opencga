package org.opencb.opencga.storage.variant;

import com.mongodb.*;
import org.opencb.opencga.lib.auth.MongoCredentials;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.effect.VariantEffect;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.biodata.models.variant.stats.VariantStats;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantVcfMongoDataWriter extends VariantDBWriter {

    private VariantSource file;

    private MongoClient mongoClient;
    private DB db;
    private DBCollection filesCollection;
    private DBCollection variantCollection;
    private Map<String, BasicDBObject> mongoMap;

    private MongoCredentials credentials;

    private boolean includeStats;
    private boolean includeEffect;
    private boolean includeSamples;

    private Map<String, Integer> conseqTypes;

    public VariantVcfMongoDataWriter(VariantSource file, String species, MongoCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.file = file;
        this.credentials = credentials;
        this.mongoMap = new HashMap<>();

        conseqTypes = new LinkedHashMap<>();
    }

    @Override
    public boolean open() {
        try {
            // Mongo configuration
            ServerAddress address = new ServerAddress(credentials.getMongoHost(), credentials.getMongoPort());
            mongoClient = new MongoClient(address, Arrays.asList(credentials.getMongoCredentials()));
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
        if (this.includeStats) {
            buildStatsRaw(data);
        }
        if (this.includeEffect) {
            buildEffectRaw(data);
        }
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
            BasicDBObject mongoFile = new BasicDBObject("fileName", file.getName()).append("fileId", file.getAlias());

            // Attributes
            if (v.getAttributes().size() > 0) {
                BasicDBObject info = null;
                for (Map.Entry<String, String> entry : v.getAttributes().entrySet()) {
                    if (info == null) {
                        info = new BasicDBObject(entry.getKey(), entry.getValue());
                    } else {
                        info.append(entry.getKey(), entry.getValue());
                    }
                }

                if (info != null) {
                    mongoFile.put("attributes", info);
                }
            }

            // Samples
            if (this.includeSamples && v.getSamplesData().size() > 0) {
                BasicDBObject samples = new BasicDBObject();

                for (Map.Entry<String, Map<String, String>> entry : v.getSamplesData().entrySet()) {
                    BasicDBObject sampleData = new BasicDBObject();
                    for (Map.Entry<String, String> sampleEntry : entry.getValue().entrySet()) {
                        sampleData.put(sampleEntry.getKey(), sampleEntry.getValue());
                    }
                    samples.put(entry.getKey(), sampleData);
                }
                mongoFile.put("samples", samples);
            }
             
            mongoFiles.add(mongoFile);
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
        for (Variant variant : data) {
//            VariantStats v = variant.getStats();
//            if (v == null) {
//                continue;
//            }
//
//            BasicDBObject mongoStudy = mongoMap.get(variant.getHgvs());
//
//            if (mongoStudy == null) {
//                // TODO It means that the same position was already found in this file, so __for now__ it won't be processed again
//                continue;
//            }
//
//            if (!mongoStudy.containsField("stats")) {
//                // Generate genotype counts
//                BasicDBObject genotypes = new BasicDBObject();
//
//                for (Genotype g : v.getGenotypes()) {
//                    String count = (g.getAllele1() == null ? -1 : g.getAllele1()) + "/" + (g.getAllele2() == null ? -1 : g.getAllele2());
//                    genotypes.append(count, g.getCount());
//                }
//
//                BasicDBObject mongoStats = new BasicDBObject("maf", v.getMaf());
//                mongoStats.append("mgf", v.getMgf());
//                mongoStats.append("alleleMaf", v.getMafAllele());
//                mongoStats.append("genotypeMaf", v.getMgfAllele());
//                mongoStats.append("missAllele", v.getMissingAlleles());
//                mongoStats.append("missGenotypes", v.getMissingGenotypes());
//                mongoStats.append("mendelErr", v.getMendelinanErrors());
//                mongoStats.append("casesPercentDominant", v.getCasesPercentDominant());
//                mongoStats.append("controlsPercentDominant", v.getControlsPercentDominant());
//                mongoStats.append("casesPercentRecessive", v.getCasesPercentRecessive());
//                mongoStats.append("controlsPercentRecessive", v.getControlsPercentRecessive());
//                mongoStats.append("genotypeCount", genotypes);
//
//                mongoStudy.put("stats", mongoStats);
//            } else {
//                // TODO aaleman: What if there are stats already?
//            }
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
        if ("exon".equalsIgnoreCase(effect.getFeatureType())) {
            object.append("geneName", effect.getGeneName());
        }
        return object;
    }
    
    @Override
    boolean buildBatchIndex(List<Variant> data) {
        variantCollection.ensureIndex(new BasicDBObject("chr", 1).append("start", 1));
        variantCollection.ensureIndex(new BasicDBObject("files.fileId", 1));
        return true;
    }

    @Override
    boolean writeBatch(List<Variant> data) {
        for (Variant v : data) {
            String rowkey = buildRowkey(v);
            BasicDBObject mongoVariant = mongoMap.get(rowkey);
            BasicDBObject query = new BasicDBObject("_id", rowkey);
            WriteResult wr;
            
            if (mongoVariant.containsField("chr")) { // Was fully built in this run because it didn't exist, and must be inserted
                wr = variantCollection.insert(mongoVariant);
            } else { // It existed previously, was not fully built in this run and only files need to be updated
                BasicDBObject file = (BasicDBObject) ((BasicDBList) mongoVariant.get("files")).get(0);
                BasicDBObject changes = new BasicDBObject().append("$push", new BasicDBObject("files", file));
                wr = variantCollection.update(query, changes, true, false);
            }
            
            if (!wr.getLastError().ok()) {
                // TODO If not correct, retry?
                return false;
            }
        }

        mongoMap.clear();

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
                    .append("biallelicsCount", global.getBiallelicsCount())
                    .append("multiallelicsCount", global.getMultiallelicsCount())
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
