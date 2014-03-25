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

//        this.includeEffect(false);
        this.includeStats(false);
        this.includeSamples(false);

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
//        if (this.includeEffect) {
            buildEffectRaw(data);
//        }
        buildBatchIndex(data);
        return writeBatch(data);
    }

    @Override
    boolean buildBatchRaw(List<Variant> data) {
        for (Variant v : data) {
            // Check that this relationship was not established yet
            BasicDBObject query = new BasicDBObject("hgvs", v.getHgvs());
            query.append("files.fileId", file.getAlias());

            if (variantCollection.count(query) == 0) {
                BasicDBObject mongoStudy = new BasicDBObject("fileName", file.getName()).append("fileId", file.getAlias());
                
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
                        mongoStudy.put("attributes", info);
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
                    mongoStudy.put("samples", samples);
                }

                mongoMap.put(v.getHgvs(), mongoStudy);

            } else {
                // TODO What if there is the same position already?
                System.out.println("Variant " + v.getChromosome() + ":" + v.getStart() + "-" + v.getEnd() + " already found");
            }
        }

        return true;
    }

    @Override
    boolean buildStatsRaw(List<Variant> data) {
        for (Variant variant : data) {
            VariantStats v = variant.getStats();
            if (v == null) {
                continue;
            }

            BasicDBObject mongoStudy = mongoMap.get(variant.getHgvs());

            if (mongoStudy == null) {
                // TODO It means that the same position was already found in this file, so __for now__ it won't be processed again
                continue;
            }

            if (!mongoStudy.containsField("stats")) {
                // Generate genotype counts
                BasicDBObject genotypes = new BasicDBObject();

                for (Genotype g : v.getGenotypes()) {
                    String count = (g.getAllele1() == null ? -1 : g.getAllele1()) + "/" + (g.getAllele2() == null ? -1 : g.getAllele2());
                    genotypes.append(count, g.getCount());
                }

                BasicDBObject mongoStats = new BasicDBObject("maf", v.getMaf());
                mongoStats.append("mgf", v.getMgf());
                mongoStats.append("alleleMaf", v.getMafAllele());
                mongoStats.append("genotypeMaf", v.getMgfAllele());
                mongoStats.append("missAllele", v.getMissingAlleles());
                mongoStats.append("missGenotypes", v.getMissingGenotypes());
                mongoStats.append("mendelErr", v.getMendelinanErrors());
                mongoStats.append("casesPercentDominant", v.getCasesPercentDominant());
                mongoStats.append("controlsPercentDominant", v.getControlsPercentDominant());
                mongoStats.append("casesPercentRecessive", v.getCasesPercentRecessive());
                mongoStats.append("controlsPercentRecessive", v.getControlsPercentRecessive());
                mongoStats.append("genotypeCount", genotypes);

                mongoStudy.put("stats", mongoStats);
            } else {
                // TODO aaleman: What if there are stats already?
            }
        }

        return true;
    }

    @Override
    boolean buildEffectRaw(List<Variant> variants) {
        for (Variant v : variants) {
            BasicDBObject mongoStudy = mongoMap.get(v.getHgvs());

            if (mongoStudy == null) {
                // TODO It means that the same position was already found in this file, so __for now__ it won't be processed again
                continue;
            }

            // Add effects to file
            if (!v.getEffect().isEmpty()) {
                Set<String> effectsSet = new HashSet<>();
                Set<String> genesSet = new HashSet<>();

                for (VariantEffect effect : v.getEffect()) {
                    effectsSet.add(effect.getConsequenceTypeObo());
                    addConsequenceType(effect.getConsequenceTypeObo());
                    if (effect.getFeatureType() != null && effect.getFeatureType().equalsIgnoreCase("exon")) {
                        genesSet.add(effect.getGeneName());
                    }
                }
                
                mongoStudy.put("effects", effectsSet);
                mongoStudy.put("genes", genesSet);
            }
        }

        return false;
    }

    @Override
    boolean buildBatchIndex(List<Variant> data) {
        variantCollection.ensureIndex(new BasicDBObject("chr", 1).append("start", 1));
        variantCollection.ensureIndex(new BasicDBObject("hgvs", 1));
        variantCollection.ensureIndex(new BasicDBObject("files.fileId", 1));
        return true;
    }

    @Override
    boolean writeBatch(List<Variant> data) {
        for (Variant v : data) {
            BasicDBObject mongoStudy = mongoMap.get(v.getHgvs());

            if (mongoStudy == null) {
                // TODO It means that the same position was already found in this file, so __for now__ it won't be processed again
                continue;
            }
            
            BasicDBObject mongoVariant = new BasicDBObject().append("$push", new BasicDBObject("files", mongoStudy));

            BasicDBObject query = new BasicDBObject("hgvs", v.getHgvs()).append("id", v.getId());
            query.append("chr", v.getChromosome()).append("start", v.getStart()).append("end", v.getStart());
            query.append("length", v.getLength()).append("ref", v.getReference()).append("alt", v.getAlternate());
            query.append("type", v.getType().name());
            WriteResult wr = variantCollection.update(query, mongoVariant, true, false);

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
