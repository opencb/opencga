package org.opencb.opencga.storage.variant;

import com.mongodb.*;
import org.opencb.commons.bioformats.feature.Genotype;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantGlobalStats;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.VariantWriter;
import org.opencb.opencga.lib.auth.MongoCredentials;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class VariantVcfMongoDataWriter implements VariantWriter {

    private VariantStudy study;

    private MongoClient mongoClient;
    private DB db;
    private DBCollection studyCollection;
    private DBCollection variantCollection;

    private MongoCredentials credentials;

    private boolean includeStats;
    private boolean includeEffect;
    private boolean includeSamples;

    public VariantVcfMongoDataWriter(VariantStudy study, String species, MongoCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.study = study;
        this.credentials = credentials;

        this.includeEffect(false);
        this.includeStats(false);
        this.includeSamples(false);
    }

    @Override
    public boolean open() {
        try {
            // Mongo configuration
            mongoClient = new MongoClient(credentials.getMongoHost());
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
        studyCollection = db.getCollection("studies");
        variantCollection = db.getCollection("variants");

        return variantCollection != null && studyCollection != null;
    }

    private boolean writeBatch(List<Variant> data) {
        // Generate the Put objects


        for (Variant v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));

            // Check that this relationship was not established yet
            BasicDBObject query = new BasicDBObject("position", rowkey);
            query.put("studies.studyId", study.getName());

            if (variantCollection.count(query) == 0) {
//                System.out.println("no hay" + rowkey + " - " + v.getChromosome() + " - " + v.getPosition());
                // Insert relationship variant-study in Mongo
                BasicDBObject mongoStudy = new BasicDBObject("studyId", study.getName()).append("ref", v.getReference()).append("alt", v.getAltAlleles()).append("snpId", v.getId());
                BasicDBObject mongoVariant = new BasicDBObject().append("$addToSet", new BasicDBObject("studies", mongoStudy));
                BasicDBObject query2 = new BasicDBObject("position", rowkey);
                query2.put("chr", v.getChromosome());
                query2.put("pos", v.getPosition());


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


                WriteResult wr = variantCollection.update(query2, mongoVariant, true, false);
                if (!wr.getLastError().ok()) {
                    // TODO If not correct, retry?
                    return false;
                }


            }
        }

        BasicDBObject indexChrPos = new BasicDBObject("chr", 1);
        indexChrPos.put("pos", 1);
        variantCollection.ensureIndex(new BasicDBObject("position", 1));
        variantCollection.ensureIndex(indexChrPos);

        return true;
    }

    private boolean writeVariantStats(List<Variant> data) {
        for (Variant variant : data) {
            VariantStats v = variant.getStats();
            if (v == null) {
                continue;
            }
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));

            // Generate genotype counts
            BasicDBObject genotypes = new BasicDBObject();

            for (Genotype g : v.getGenotypes()) {
                String count = (g.getAllele1() == null ? -1 : g.getAllele1()) + "/" + (g.getAllele2() == null ? -1 : g.getAllele2());
                genotypes.append(count, g.getCount());
            }


            //db.variants.aggregate({$match : {position : '01_0000100000'}},
            //                      {$unwind: "$studies"},
            //                      { $match : {"studies.studyId": "testStudy1", "studies.stats" : { $exists : true}}})

            // Search for already existing study
            DBObject match = new BasicDBObject("$match", new BasicDBObject("position", rowkey));
            DBObject unwind = new BasicDBObject("$unwind", "$studies");
            DBObject match2_fields = new BasicDBObject("studies.studyId", study.getName());
            match2_fields.put("studies.stats", new BasicDBObject("$exists", true));
            DBObject match2 = new BasicDBObject("$match", match2_fields);

            AggregationOutput agg_output = variantCollection.aggregate(match, unwind, match2);

            if (!agg_output.results().iterator().hasNext()) {
                // Add stats to study
                BasicDBObject mongoStats = new BasicDBObject("maf", v.getMaf()).append("alleleMaf", v.getMafAllele()).append(
                        "missing", v.getMissingGenotypes()).append("genotypeCount", genotypes);
                BasicDBObject item = new BasicDBObject("studies.$.stats", mongoStats);
                BasicDBObject action = new BasicDBObject("$set", item);

                BasicDBObject query = new BasicDBObject("position", rowkey);
                query.put("studies.studyId", study.getName());

                WriteResult wr = variantCollection.update(query, action, true, false);
                if (!wr.getLastError().ok()) {
                    // TODO If not correct, retry?
                    return false;
                }

            }

        }

        return true;
    }

    private boolean writeVariantEffect(List<Variant> variants) {
        Map<String, Set<String>> mongoPutMap = new HashMap<>();

        for (Variant variant : variants) {
            for (VariantEffect variantEffect : variant.getEffect()) {
                String rowkey = buildRowkey(variantEffect.getChromosome(), String.valueOf(variantEffect.getPosition()));

                // Insert in the map for Mongo storage
                Set<String> positionSet = mongoPutMap.get(rowkey);
                if (positionSet == null) {
                    positionSet = new HashSet<>();
                    mongoPutMap.put(rowkey, positionSet);
                }
                positionSet.add(variantEffect.getConsequenceTypeObo());
            }
        }

        // TODO Insert in Mongo
        saveEffectMongo(mongoPutMap);

        return true;
    }

    private boolean writeStudy(VariantStudy study) {
        String timeStamp = new SimpleDateFormat("dd/mm/yyyy").format(Calendar.getInstance().getTime());
        BasicDBObject studyMongo = new BasicDBObject("name", study.getName())
                .append("alias", study.getAlias())
                .append("date", timeStamp)
                .append("authors", study.getAuthors())
                .append("samples", study.getSamples())
                .append("description", study.getDescription())
                .append("sources", study.getSources());

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
                    .append("accumulatedQuality", global.getAccumQuality());
            studyMongo = studyMongo.append("globalStats", globalStats);
        } else {
            // TODO Notify?
        }

        // TODO Save pedigree information

        Map<String, String> meta = study.getMetadata();
        DBObject metadataMongo = new BasicDBObjectBuilder()
                .add("header", meta.get("variantFileHeader"))
                .get();
        studyMongo = studyMongo.append("metadata", metadataMongo);

        DBObject query = new BasicDBObject("name", study.getName());
        WriteResult wr = studyCollection.update(query, studyMongo, true, false);
        return wr.getLastError().ok(); // TODO Is this a proper return statement?
    }

    @Override
    public boolean post() {

        writeStudy(study);
        return true;
    }

    @Override
    public boolean write(Variant variant) {
        return write(Arrays.asList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
        boolean res = writeBatch(data);
        if (res && this.includeStats) {
            res &= writeVariantStats(data);
        }
        if (res && this.includeEffect) {
            res &= writeVariantEffect(data);
        }
        return res;
    }

    @Override
    public boolean close() {

        return true;
    }

    private String buildRowkey(String chromosome, String position) {
        if (chromosome.length() > 2) {
            if (chromosome.substring(0, 2).equals("chr")) {
                chromosome = chromosome.substring(2);
            }
        }
        if (chromosome.length() < 2) {
            chromosome = "0" + chromosome;
        }
        if (position.length() < 10) {
            while (position.length() < 10) {
                position = "0" + position;
            }
        }
        return chromosome + "_" + position;
    }

    private void saveEffectMongo(Map<String, Set<String>> putMap) {
        for (Map.Entry<String, Set<String>> entry : putMap.entrySet()) {
            BasicDBObject query = new BasicDBObject("position", entry.getKey());
            query.put("studies.studyId", study.getName());

            DBObject item = new BasicDBObject("studies.$.effects", entry.getValue());
            BasicDBObject action = new BasicDBObject("$set", item);

            variantCollection.update(query, action, true, false);
        }
        putMap.clear();
    }

    @Override
    public void includeStats(boolean b) {
        this.includeStats = b;
    }

    @Override
    public void includeSamples(boolean b) {
        this.includeSamples = b;
    }

    @Override
    public void includeEffect(boolean b) {
        this.includeEffect = b;
    }
}
