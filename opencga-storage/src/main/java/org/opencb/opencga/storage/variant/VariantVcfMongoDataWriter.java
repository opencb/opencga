package org.opencb.opencga.storage.variant;

import com.mongodb.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.commons.bioformats.feature.Genotype;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.*;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;
import org.opencb.opencga.lib.auth.MongoCredentials;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class VariantVcfMongoDataWriter implements VariantDBWriter<VcfRecord> {

    private final byte[] infoColumnFamily = "i".getBytes();
    private final byte[] dataColumnFamily = "d".getBytes();
    private String tableName;
    private String studyName;


    private MongoClient mongoClient;
    private DB db;
    private DBCollection studyCollection;
    private DBCollection variantCollection;

    private MongoCredentials credentials;


    public VariantVcfMongoDataWriter(String study, String species, MongoCredentials credentials) {
        if (credentials == null) {
            throw new IllegalArgumentException("Credentials for accessing the database must be specified");
        }
        this.studyName = study;
        this.tableName = species;
        this.credentials = credentials;
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

    @Override
    public boolean writeHeader(String string) {
        return true;
    }

    @Override
    public boolean writeBatch(List<VcfRecord> data) {
        // Generate the Put objects
        Put auxPut;
        for (VcfRecord v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));

            // Check that this relationship was not established yet
            BasicDBObject query = new BasicDBObject("position", rowkey);
            query.put("studies.studyId", studyName);

            if (variantCollection.count(query) == 0) {
                // Insert relationship variant-study in Mongo
                BasicDBObject mongoStudy = new BasicDBObject("studyId", studyName).append("ref", v.getReference()).append("alt", v.getAltAlleles());
                BasicDBObject mongoVariant = new BasicDBObject().append("$addToSet", new BasicDBObject("studies", mongoStudy));
                BasicDBObject query2 = new BasicDBObject("position", rowkey);
                query2.put("chr", v.getChromosome()); // TODO aaleman: change this code in the MonBase Version
                query2.put("pos", v.getPosition());
                WriteResult wr = variantCollection.update(query2, mongoVariant, true, false);
                if (!wr.getLastError().ok()) {
                    // TODO If not correct, retry?
                    return false;
                }

            }
        }

        DBObject indexPosition = new BasicDBObject("position", 1);
        DBObject indexChrPos = new BasicDBObject("chr", 1);
        indexChrPos.put("pos", 1);

        variantCollection.ensureIndex(indexPosition);
        variantCollection.ensureIndex(indexChrPos);

        return true;
    }

    @Override
    public boolean writeVariantStats(List<VariantStats> data) {
        for (VariantStats v : data) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));



            // Generate genotype counts
            BasicDBObject genotypeCounts = new BasicDBObject(); // TODO aaleman: change this code in the MonBase Version
            for (Genotype g : v.getGenotypes()) {
                String count = (g.getAllele1() == null ? -1 : g.getAllele1()) + "/" + (g.getAllele2() == null ? -1 : g.getAllele2());
                genotypeCounts.append(count, g.getCount());
            }


            //db.variants.aggregate({$match : {position : '01_0000100000'}},
            //                      {$unwind: "$studies"},
            //                      { $match : {"studies.studyId": "testStudy1", "studies.stats" : { $exists : true}}})

            // Search for already existing study
            DBObject match = new BasicDBObject("$match", new BasicDBObject("position", rowkey));
            DBObject unwind = new BasicDBObject("$unwind", "$studies");
            DBObject match2_fields = new BasicDBObject("studies.studyId", studyName);
            match2_fields.put("studies.stats", new BasicDBObject("$exists", true));
            DBObject match2 = new BasicDBObject("$match", match2_fields);

            AggregationOutput agg_output = variantCollection.aggregate(match, unwind, match2);

            if (!agg_output.results().iterator().hasNext()) {
                // Add stats to study
                BasicDBObject mongoStats = new BasicDBObject("maf", v.getMaf()).append("alleleMaf", v.getMafAllele()).append(
                        "missing", v.getMissingGenotypes()).append("genotypeCount", genotypeCounts);
                BasicDBObject item = new BasicDBObject("studies.$.stats", mongoStats);
                BasicDBObject action = new BasicDBObject("$set", item);

                BasicDBObject query = new BasicDBObject("position", rowkey);
                query.put("studies.studyId", studyName);

                WriteResult wr = variantCollection.update(query, action, true, false);
                if (!wr.getLastError().ok()) {
                    // TODO If not correct, retry?
                    return false;
                }

            }

        }

        // Save results in HBase

        return true;
    }

    @Override
    public boolean writeGlobalStats(VariantGlobalStats vgs) {
        return true; //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeSampleStats(VariantSampleStats vss) {
        return true; //throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeSampleGroupStats(VariantSampleGroupStats vsgs) throws IOException {
        return true;//throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeVariantGroupStats(VariantGroupStats vvgs) throws IOException {
        return true;//throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean writeVariantEffect(List<VariantEffect> list) {
        Map<String, Set<String>> mongoPutMap = new HashMap<>();

        for (VariantEffect v : list) {
            String rowkey = buildRowkey(v.getChromosome(), String.valueOf(v.getPosition()));
            // TODO Insert in the map for HBase storage
            // Insert in the map for Mongo storage
            Set<String> positionSet = mongoPutMap.get(rowkey);
            if (positionSet == null) {
                positionSet = new HashSet<>();
                mongoPutMap.put(rowkey, positionSet);
            }
            positionSet.add(v.getConsequenceTypeObo());
        }

        // TODO Insert in Mongo
        saveEffectMongo(variantCollection, mongoPutMap);

        return true;
    }

    @Override
    public boolean writeStudy(VariantStudy study) {
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

        return true;
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



    private void saveEffectMongo(DBCollection collection, Map<String, Set<String>> putMap) {
        for (Map.Entry<String, Set<String>> entry : putMap.entrySet()) {
            BasicDBObject query = new BasicDBObject("position", entry.getKey());
            query.put("studies.studyId", studyName);

            DBObject item = new BasicDBObject("studies.$.effects", entry.getValue());
            BasicDBObject action = new BasicDBObject("$set", item);

            WriteResult wr = variantCollection.update(query, action, true, false);
        }
        putMap.clear();
    }
}
