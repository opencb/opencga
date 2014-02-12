package org.opencb.opencga.storage.variant;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantGlobalStats;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MongoCredentials;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class VariantVcfMongoDataWriterTest {

    private static final String tableName = "test_VariantVcfMongoDataWriterTest";
    private static final String studyName = "testStudy1";
    private static MongoCredentials credentials;
    private static VariantVcfMongoDataWriter writer;

    @BeforeClass
    public static void testConstructorAndOpen() {
        try {
            // Credentials for the writer
            credentials = new MongoCredentials("localhost", 27017, tableName, "aleman", "aleman");

            // Monbase writer
            writer = new VariantVcfMongoDataWriter(studyName, tableName, credentials);
            assertTrue(writer.open());
        } catch (IllegalOpenCGACredentialsException e) {
            fail(e.getMessage());
        }

        assertNotNull("Mongo credentials must be not null", credentials);
        assertNotNull("Mongo writer must be not null", writer);
    }

    @Test
    public void testPre() {
        assertTrue("Table creation could not be performed", writer.pre());
    }

    @Test
    public void testWriteBatch() throws IOException, InterruptedException, ClassNotFoundException {
        // Save 5 records
        List<String> sampleNames = Arrays.asList("NA001", "NA002", "NA003");
        VcfRecord rec1 = new VcfRecord(new String[]{"1", "100000", "rs1100000", "C", "T,G", "40", "PASS",
                "DP=5;AP=10;H2", "GT:DP", "1/1:4", "1/0:2", "0/0:3"}, sampleNames);
        VcfRecord rec2 = new VcfRecord(new String[]{"1", "200000", "rs1200000", "G", "T", "30", "LowQual",
                "DP=2;AP=5", "GT:DP", "1/1:3", "1/1:1", "0/0:5"}, sampleNames);
        VcfRecord rec3 = new VcfRecord(new String[]{"1", "300000", "rs1300000", "C", "T", "50", "PASS",
                "DP=1;AP=6", "GT:DP", "1/0:3", "0/1:1", "0/0:5"}, sampleNames);
        VcfRecord rec4 = new VcfRecord(new String[]{"2", "100000", "rs2100000", "G", "A", "60", "STD_FILTER",
                "DP=3;AP=8", "GT:DP", "1/1:3", "1/0:1", "0/0:5"}, sampleNames);
        VcfRecord rec5 = new VcfRecord(new String[]{"3", "200000", "rs3200000", "G", "C", "80", "LowQual;STD_FILTER",
                "DP=2;AP=6", "GT:DP", "1/0:3", "1/1:1", "0/1:5"}, sampleNames);

        List<VcfRecord> records = Arrays.asList(new VcfRecord[]{rec1, rec2, rec3, rec4, rec5});
        writer.writeBatch(records);
        writer.post();

    }

    @Test
    public void testWriteVariantStats() throws IOException {
        VariantStats stats1 = new VariantStats("1", 100000, "A", "T,G", 996, 0.30, "A", "A/T", 2, 0, 1, true, 0.02, 0.10, 0.30, 0.15);
        VariantStats stats2 = new VariantStats("1", 200000, "G", "T", 0.05, 0.20, "T", "T/T", 1, 1, 0, true, 0.05, 0.30, 0.30, 0.10);
        List<VariantStats> stats = Arrays.asList(stats1, stats2);

        assertTrue(writer.writeVariantStats(stats));
        writer.post();

//        // Query studyStats inserted in HBase
//        HTable table = new HTable(config, tableName);
//        Scan regionScan = new Scan("01_0000100000".getBytes(), "01_0000200001".getBytes());
//        ResultScanner variantScanner = table.getScanner(regionScan);
//        Result result1 = variantScanner.next();
//        Result result2 = variantScanner.next();
//
//        NavigableMap<byte[], byte[]> infoMap = result1.getFamilyMap("i".getBytes());
//        byte[] byteStats = infoMap.get((studyName + "_stats").getBytes());
//        VariantFieldsProtos.VariantStats protoInfo = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
//        assertEquals(stats1.getMaf(), protoInfo.getMaf(), 0.001);
//        assertEquals(stats1.getCasesPercentDominant(), protoInfo.getCasesPercentDominant(), 0.001);
//        assertArrayEquals(stats1.getAltAlleles(), new String[]{"T", "G"});
//
//        infoMap = result2.getFamilyMap("i".getBytes());
//        byteStats = infoMap.get((studyName + "_stats").getBytes());
//        protoInfo = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
//        assertEquals(stats2.getMaf(), protoInfo.getMaf(), 0.001);
//        assertEquals(stats2.getCasesPercentDominant(), protoInfo.getCasesPercentDominant(), 0.001);
//        assertArrayEquals(stats2.getAltAlleles(), new String[]{"T"});
//
//
//        // Query studyStats inserted in Mongo
//        MongoClient mongoClient = new MongoClient(credentials.getMongoHost());
//        DB db = mongoClient.getDB(credentials.getMongoDbName());
//        DBCollection variantsCollection = db.getCollection("variants");
//
//        DBObject query = new BasicDBObject("position", "01_0000100000");
//        query.put("studies.studyId", studyName);
//        DBObject returnValues = new BasicDBObject("studies.stats", 1);
//        DBObject variantsInStudy = variantsCollection.findOne(query, returnValues);
//        assertNotNull(variantsInStudy);
//
//        BasicDBList studiesDbObject = (BasicDBList) variantsInStudy.get("studies");
//        DBObject studyObj = (DBObject) studiesDbObject.get(0);
//        DBObject statsObj = (BasicDBObject) studyObj.get("stats");
//        double maf = ((Double) statsObj.get("maf")).doubleValue();
//        String alleleMaf = statsObj.get("alleleMaf").toString();
//        int missing = ((Integer) statsObj.get("missing")).intValue();
//
//        assertEquals(stats1.getMaf(), maf, 0.001);
//        assertEquals(stats1.getMafAllele(), alleleMaf);
//        assertEquals(stats1.getMissingGenotypes(), missing);
//
//        query = new BasicDBObject("position", "01_0000200000");
//        query.put("studies.studyId", studyName);
//        returnValues = new BasicDBObject("studies.stats", 1);
//        variantsInStudy = variantsCollection.findOne(query, returnValues);
//        assertNotNull(variantsInStudy);
//
//        studiesDbObject = (BasicDBList) variantsInStudy.get("studies");
//        studyObj = (DBObject) studiesDbObject.get(0);
//        statsObj = (BasicDBObject) studyObj.get("stats");
//        maf = ((Double) statsObj.get("maf")).doubleValue();
//        alleleMaf = statsObj.get("alleleMaf").toString();
//        missing = ((Integer) statsObj.get("missing")).intValue();
//
//        assertEquals(stats2.getMaf(), maf, 0.001);
//        assertEquals(stats2.getMafAllele(), alleleMaf);
//        assertEquals(stats2.getMissingGenotypes(), missing);
//
//        mongoClient.close();
    }

    @Test
    public void testWriteVariantEffect() throws IOException, InterruptedException, ClassNotFoundException {
        VariantEffect eff1 = new VariantEffect("1", 100000, "A", "T", "", "RP11-206L10.6",
                "intron", "processed_transcript", "1", 714473, 739298, "1", "", "", "",
                "ENSG00000237491", "ENST00000429505", "RP11-206L10.6", "SO:0001627",
                "intron_variant", "In intron", "feature", -1, "", "");
        VariantEffect eff2 = new VariantEffect("1", 100000, "A", "T", "ENST00000358533", "AL669831.1",
                "downstream", "protein_coding", "1", 722513, 727513, "1", "", "", "",
                "ENSG00000197049", "ENST00000358533", "AL669831.1", "SO:0001633",
                "5KB_downstream_variant", "Within 5 kb downstream of the 3 prime end of a transcript", "feature", -1, "", "");
        VariantEffect eff3 = new VariantEffect("1", 100000, "C", "A", "ENST00000434264", "RP11-206L10.7",
                "downstream", "lincRNA", "1", 720070, 725070, "1", "", "", "",
                "ENSG00000242937", "ENST00000434264", "RP11-206L10.7", "SO:0001633",
                "5KB_downstream_variant", "Within 5 kb downstream of the 3 prime end of a transcript", "feature", -1, "", "");
        List<VariantEffect> effects = Arrays.asList(eff1, eff2, eff3);

        writer.writeVariantEffect(effects);
        writer.post();

//        // TODO Query number of inserted records in HBase
//        Job job = RowCounter.createSubmittableJob(config, new String[] { tableName + "effect" } );
//        job.waitForCompletion(true);
//        assertTrue(job.isSuccessful());
//        // How to count in HBase test suite: http://searchcode.com/codesearch/view/25291904
//        Counter counter = job.getCounters().findCounter("org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters", "ROWS");
//        assertEquals("The number of inserted effects is incorrect", 3, counter.getValue());

//        // Query effects inserted in Mongo
//        MongoClient mongoClient = new MongoClient(credentials.getMongoHost());
//        DB db = mongoClient.getDB(credentials.getMongoDbName());
//        DBCollection variantsCollection = db.getCollection("variants");
//
//        DBObject query = new BasicDBObject("position", "01_0000100000");
//        query.put("studies.studyId", studyName);
//        DBObject returnValues = new BasicDBObject("studies.effects", 1);
//        DBObject variantsInStudy = variantsCollection.findOne(query, returnValues);
//        assertNotNull(variantsInStudy);
//
//        BasicDBList studiesDbObject = (BasicDBList) variantsInStudy.get("studies");
//        DBObject studyObj = (DBObject) studiesDbObject.get(0);
//        Set<String> effectsObj = new HashSet<>((List<String>) studyObj.get("effects"));
//        Set<String> oboList = new HashSet<>(Arrays.asList("intron_variant", "5KB_downstream_variant"));
//
//        assertEquals(oboList, effectsObj);
//
//        mongoClient.close();
    }

    @Test
    public void testWriteStudy() throws UnknownHostException {
        VariantStudy study = new VariantStudy(studyName, "s1", "Study created for testing purposes",
                Arrays.asList("Cristina", "Alex", "Jesus"), Arrays.asList("vcf", "ped"));
        VariantGlobalStats studyStats = new VariantGlobalStats();
        studyStats.setVariantsCount(5);
        studyStats.setSnpsCount(3);
        studyStats.setAccumQuality(45.0f);
        study.setStats(studyStats);

        assertTrue(writer.writeStudy(study));
        writer.post();

//        // Query study inserted in Mongo
//        MongoClient mongoClient = new MongoClient(credentials.getMongoHost());
//        DB db = mongoClient.getDB(credentials.getMongoDbName());
//        DBCollection variantsCollection = db.getCollection("studies");
//
//        DBObject query = new BasicDBObject("name", studyName);
//        DBObject studyObj = variantsCollection.findOne(query);
//        assertNotNull(studyObj);
//
//        String alias = studyObj.get("alias").toString();
//        List<String> authors = (List<String>) studyObj.get("authors");
//        DBObject stats = (DBObject) studyObj.get("globalStats");
//        int variantsCount = ((Integer) stats.get("variantsCount")).intValue();
//        float accumQuality = ((Double) stats.get("accumulatedQuality")).floatValue();
//
//        assertEquals(study.getAlias(), alias);
//        assertEquals(study.getAuthors(), authors);
//        assertEquals(studyStats.getVariantsCount(), variantsCount);
//        assertEquals(studyStats.getAccumQuality(), accumQuality, 1e-6);
    }

    @AfterClass
    public static void deleteTables() throws IOException {
        // Delete HBase tables

        // Delete Mongo collection
//        MongoClient mongoClient = new MongoClient(credentials.getMongoHost());
//        DB db = mongoClient.getDB(credentials.getMongoDbName());
//        db.dropDatabase();
//        mongoClient.close();
    }
}
