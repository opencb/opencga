package org.opencb.opencga.storage.variant;

import org.opencb.opencga.storage.variant.monbase.VariantHbaseWriter;
import com.mongodb.*;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class VariantHbaseWriterTest {

    private static final String tableName = "test_VariantVcfMonbaseDataWriterTest";
    private static VariantSource study = new VariantSource("testStudy", "testAlias", "testStudy", null, null);
    private static MonbaseCredentials credentials;
    private static Configuration config;
    private static VariantHbaseWriter writer;
    private static List<Variant> variants;
    
    @BeforeClass
    public static void testConstructorAndOpen() {
        try {
            // Credentials for the writer
            credentials = new MonbaseCredentials("172.24.79.30", 60010, "172.24.79.30", 2181, "localhost", 9999, tableName, "cgonzalez", "cgonzalez");

            // HBase configuration with the active credentials
            config = HBaseConfiguration.create();
            config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
            config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
            config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));

            // Monbase writer
            writer = new VariantHbaseWriter(study, tableName, credentials);
            assertTrue(writer.open());
        } catch (IllegalOpenCGACredentialsException e) {
            fail(e.getMessage());
        }

        assertNotNull("Monbase credentials must be not null", credentials);
        assertNotNull("Monbase writer must be not null", writer);
    }

    @BeforeClass
    public static void initializeDataToInsert() {
        List<String> sampleNames = Arrays.asList( "NA001", "NA002", "NA003" );
        String[] fields1 = new String[] { "1", "100000", "rs1100000", "A", "T,G", "40", "PASS", 
            "DP=5;AP=10;H2", "GT:DP", "1/1:4", "1/0:2", "0/0:3" };
        String[] fields2 = new String[] {"1", "200000", "rs1200000", "G", "T", "30", "LowQual", 
            "DP=2;AP=5", "GT:DP", "1/1:3", "1/1:1", "0/0:5" };
        String[] fields3 = new String[] {"1", "300000", "rs1300000", "C", "T", "50", "PASS", 
            "DP=1;AP=6", "GT:DP", "1/0:3", "0/1:1", "0/0:5" };
        String[] fields4 = new String[] {"2", "100000", "rs2100000", "G", "A", "60", "STD_FILTER", 
            "DP=3;AP=8", "GT:DP", "1/1:3", "1/0:1", "0/0:5" };
        String[] fields5 = new String[] {"3", "200000", "rs3200000", "G", "C", "80", "LowQual;STD_FILTER", 
            "DP=2;AP=6", "GT:DP", "1/0:3", "1/1:1", "0/1:5" };
        Variant rec1 = VariantFactory.createVariantFromVcf(sampleNames, fields1).get(0);
        Variant rec2 = VariantFactory.createVariantFromVcf(sampleNames, fields2).get(0);
        Variant rec3 = VariantFactory.createVariantFromVcf(sampleNames, fields3).get(0);
        Variant rec4 = VariantFactory.createVariantFromVcf(sampleNames, fields4).get(0);
        Variant rec5 = VariantFactory.createVariantFromVcf(sampleNames, fields5).get(0);
        
//        VariantStats stats1 = new VariantStats("1", 100000, "A", "T,G", 0.01, 0.30, "A", "A/T", 2, 0, 1, true, 0.02, 0.10, 0.30, 0.15);
//        VariantStats stats2 = new VariantStats("1", 200000, "G", "T", 0.05, 0.20, "T", "T/T", 1, 1, 0, true, 0.05, 0.30, 0.30, 0.10);
//        rec1.setStats(stats1);
//        rec2.setStats(stats2);
        
        variants = Arrays.asList(rec1, rec2, rec3, rec4, rec5);
    }
            
    
    @Test
    public void testPre() {
        assertTrue("Table creation could not be performed", writer.pre());
    }

//    @Test
//    public void testWriteBatch() throws IOException, InterruptedException, ClassNotFoundException {
//        HBaseAdmin admin = new HBaseAdmin(config);
//        assertTrue(admin.tableExists(tableName));
//        writer.writeBatch(variants);
//        writer.post();
//
//        Variant rec1 = variants.get(0);
//        Variant rec4 = variants.get(3);
//        
//        // Query number of inserted records
//        Job job = RowCounter.createSubmittableJob(config, new String[] { tableName } );
//        job.waitForCompletion(true);
//        assertTrue(job.isSuccessful());
//        // How to count in HBase test suite: http://searchcode.com/codesearch/view/25291904
//        Counter counter = job.getCounters().findCounter("org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters", "ROWS");
//        assertEquals("The number of inserted records is incorrect", 5, counter.getValue());
//        
//        // Query information from a couple records
//        HTable table = new HTable(config, tableName);
//        Scan regionScan = new Scan("01_0000100000".getBytes(), "03_0000200001".getBytes());
//        ResultScanner variantScanner = table.getScanner(regionScan);
//        Result[] results = variantScanner.next((int) counter.getValue());
//        Result result1 = results[0];
//        Result result4 = results[3];
//        
//        // Get basic variant fields from Protocol Buffers message
//        NavigableMap<byte[], byte[]> infoMap = result1.getFamilyMap("i".getBytes());
//        byte[] byteInfo = infoMap.get((studyName + "_data").getBytes());
//        VariantFieldsProtos.VariantInfo protoInfo = VariantFieldsProtos.VariantInfo.parseFrom(byteInfo);
//        assertEquals("rec1 reference must be A", rec1.getReference(), protoInfo.getReference());
//        assertEquals("rec1 alternate must be T", rec1.getAlternate(), StringUtils.join(protoInfo.getAlternateList(), ","));
//        assertEquals("rec1 format must be GT:DP", rec1.getFormat(), StringUtils.join(protoInfo.getFormatList(), ":"));
//
//        // Get samples
//        NavigableMap<byte[], byte[]> sampleMap = result1.getFamilyMap("d".getBytes());
//        
//        for (Map.Entry<byte[], byte[]> entry : sampleMap.entrySet()) {
//            String name = (new String(entry.getKey(), Charset.forName("UTF-8"))).replaceAll(studyName + "_", "");
//            VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(entry.getValue());
//            switch (name) {
//                case "NA001":
//                    assertEquals("Record 1, sample NA001 must be 1/1:4", "1/1:4", sample.getSample());
//                    break;
//                case "NA002":
//                    assertEquals("Record 1, sample NA002 must be 1/1:4", "1/0:2", sample.getSample());
//                    break;
//                case "NA003":
//                    assertEquals("Record 1, sample NA002 must be 1/1:4", "0/0:3", sample.getSample());
//            }
//        }
//        
//        // Get basic variant fields from Protocol Buffers message
//        infoMap = result4.getFamilyMap("i".getBytes());
//        byteInfo = infoMap.get((studyName + "_data").getBytes());
//        protoInfo = VariantFieldsProtos.VariantInfo.parseFrom(byteInfo);
//        assertEquals("rec4 reference must be A", rec4.getReference(), protoInfo.getReference());
//        assertEquals("rec4 alternate must be T", rec4.getAlternate(), StringUtils.join(protoInfo.getAlternateList(), ","));
//        assertEquals("rec4 format must be GT:DP", rec4.getFormat(), StringUtils.join(protoInfo.getFormatList(), ":"));
//
//        // Get samples
//        sampleMap = result4.getFamilyMap("d".getBytes());
//        
//        for (Map.Entry<byte[], byte[]> entry : sampleMap.entrySet()) {
//            String name = (new String(entry.getKey(), Charset.forName("UTF-8"))).replaceAll(studyName + "_", "");
//            VariantFieldsProtos.VariantSample sample = VariantFieldsProtos.VariantSample.parseFrom(entry.getValue());
//            switch (name) {
//                case "NA004":
//                    assertEquals("Record 4, sample NA001 must be 1/1:3", "1/1:3", sample.getSample());
//                    break;
//                case "NA002":
//                    assertEquals("Record 4, sample NA002 must be 1/0:1", "1/0:1", sample.getSample());
//                    break;
//                case "NA003":
//                    assertEquals("Record 4, sample NA002 must be 0/0:5", "0/0:5", sample.getSample());
//            }
//        }
//    }
//
//    @Test
//    public void testWriteVariantStats() throws UnknownHostException, IOException {
//        VariantStats stats1 = new VariantStats("1", 100000, "A", "T,G", 0.01, 0.30, "A", "A/T", 2, 0, 1, true, 0.02, 0.10, 0.30, 0.15);
//        VariantStats stats2 = new VariantStats("1", 200000, "G", "T", 0.05, 0.20, "T", "T/T", 1, 1, 0, true, 0.05, 0.30, 0.30, 0.10);
//        
//        variants.get(0).setStats(stats1);
//        variants.get(1).setStats(stats2);
//        
//        assertTrue(writer.writeVariantStats(variants));
//        writer.post();
//        
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
//        assertArrayEquals(stats1.getAltAlleles(), new String[] { "T", "G" });
//        
//        infoMap = result2.getFamilyMap("i".getBytes());
//        byteStats = infoMap.get((studyName + "_stats").getBytes());
//        protoInfo = VariantFieldsProtos.VariantStats.parseFrom(byteStats);
//        assertEquals(stats2.getMaf(), protoInfo.getMaf(), 0.001);
//        assertEquals(stats2.getCasesPercentDominant(), protoInfo.getCasesPercentDominant(), 0.001);
//        assertArrayEquals(stats2.getAltAlleles(), new String[] { "T" });
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
//    }
//
//    @Test
//    public void testWriteVariantEffect() throws IOException, InterruptedException, ClassNotFoundException {
//        VariantEffect eff1 = new VariantEffect("1", 100000, "A", "T", "", "RP11-206L10.6",
//                "intron", "processed_transcript", "1", 714473, 739298, "1", "", "", "",
//                "ENSG00000237491", "ENST00000429505", "RP11-206L10.6", "SO:0001627",
//                "intron_variant", "In intron", "feature", -1, "", "");
//        VariantEffect eff2 = new VariantEffect("1", 100000, "A", "T", "ENST00000358533", "AL669831.1",
//                "downstream", "protein_coding", "1", 722513, 727513, "1", "", "", "",
//                "ENSG00000197049", "ENST00000358533", "AL669831.1", "SO:0001633",
//                "5KB_downstream_variant", "Within 5 kb downstream of the 3 prime end of a transcript", "feature", -1, "", "");
//        VariantEffect eff3 = new VariantEffect("1", 100000, "C", "A", "ENST00000434264", "RP11-206L10.7",
//                "downstream", "lincRNA", "1", 720070, 725070, "1", "", "", "",
//                "ENSG00000242937", "ENST00000434264", "RP11-206L10.7", "SO:0001633",
//                "5KB_downstream_variant", "Within 5 kb downstream of the 3 prime end of a transcript", "feature", -1, "", "");
//        
//        variants.get(0).addEffect(eff1);
//        variants.get(0).addEffect(eff2);
//        variants.get(0).addEffect(eff3);
//        
//        assertTrue(writer.writeVariantEffect(variants));
//        writer.post();
//        
////        // TODO Query number of inserted records in HBase
////        Job job = RowCounter.createSubmittableJob(config, new String[] { tableName + "effect" } );
////        job.waitForCompletion(true);
////        assertTrue(job.isSuccessful());
////        // How to count in HBase test suite: http://searchcode.com/codesearch/view/25291904
////        Counter counter = job.getCounters().findCounter("org.apache.hadoop.hbase.mapreduce.RowCounter$RowCounterMapper$Counters", "ROWS");
////        assertEquals("The number of inserted effects is incorrect", 3, counter.getValue());
//        
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
//    }

//    @Test
//    public void testWriteStudy() throws UnknownHostException {
//        VariantStudy study = new VariantStudy(studyName, "s1", "Study created for testing purposes", 
//                Arrays.asList("Cristina", "Alex", "Jesus"), Arrays.asList("vcf", "ped"));
//        VariantGlobalStats studyStats = new VariantGlobalStats();
//        studyStats.setVariantsCount(5);
//        studyStats.setSnpsCount(3);
//        studyStats.setAccumQuality(45.0f);
//        study.setStats(studyStats);
//        
////        assertTrue(writer.writeStudy(study));
//        writer.post();
//        
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
//    }

    @AfterClass
    public static void deleteTables() throws IOException {
        // Delete HBase tables
        HBaseAdmin admin = new HBaseAdmin(config);
        admin.disableTables(tableName);
        admin.deleteTables(tableName);

        // Delete Mongo collection
        MongoClient mongoClient = new MongoClient(credentials.getMongoHost());
        DB db = mongoClient.getDB(credentials.getMongoDbName());
        db.dropDatabase();
        mongoClient.close();
    }
}
