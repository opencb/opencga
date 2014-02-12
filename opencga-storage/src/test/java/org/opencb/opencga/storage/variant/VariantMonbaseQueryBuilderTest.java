package org.opencb.opencga.storage.variant;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.commons.bioformats.variant.vcf4.VcfRecord;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MonbaseCredentials;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 */
public class VariantMonbaseQueryBuilderTest {

    private static final String tableName = "test_VariantMonbaseQueryBuilderTest";
    private static final String studyName = "testStudy";
    private static MonbaseCredentials credentials;
    private static org.apache.hadoop.conf.Configuration config;
    private static VariantVcfMonbaseDataWriter writer;
    private static VariantMonbaseQueryBuilder queryBuilder;

    @BeforeClass
    public static void testConstructorAndOpen() throws MasterNotRunningException, ZooKeeperConnectionException, UnknownHostException {
        try {
            // Credentials for the query builder
            credentials = new MonbaseCredentials("172.24.79.30", 60010, "172.24.79.30", 2181, "localhost", 9999, tableName, "cgonzalez", "cgonzalez");

            // HBase configuration with the active credentials
            config = HBaseConfiguration.create();
            config.set("hbase.master", credentials.getHbaseMasterHost() + ":" + credentials.getHbaseMasterPort());
            config.set("hbase.zookeeper.quorum", credentials.getHbaseZookeeperQuorum());
            config.set("hbase.zookeeper.property.clientPort", String.valueOf(credentials.getHbaseZookeeperClientPort()));

            // Monbase writer saves 5 records
            writer = new VariantVcfMonbaseDataWriter(studyName, tableName, credentials);
            assertTrue(writer.open());
            List<String> sampleNames = Arrays.asList( "NA001", "NA002", "NA003" );
            VcfRecord rec1 = new VcfRecord(new String[] { "1", "100000", "rs1100000", "A", "T,G", "40", "PASS", 
                "DP=5;AP=10;H2", "GT:DP", "1/1:4", "1/0:2", "0/0:3" }, sampleNames);
            VcfRecord rec2 = new VcfRecord(new String[] {"1", "200000", "rs1200000", "G", "T", "30", "LowQual", 
                "DP=2;AP=5", "GT:DP", "1/1:3", "1/1:1", "0/0:5" }, sampleNames);
            VcfRecord rec3 = new VcfRecord(new String[] {"1", "300000", "rs1300000", "C", "T", "50", "PASS", 
                "DP=1;AP=6", "GT:DP", "1/0:3", "0/1:1", "0/0:5" }, sampleNames);
            VcfRecord rec4 = new VcfRecord(new String[] {"2", "100000", "rs2100000", "G", "A", "60", "STD_FILTER", 
                "DP=3;AP=8", "GT:DP", "1/1:3", "1/0:1", "0/0:5" }, sampleNames);
            VcfRecord rec5 = new VcfRecord(new String[] {"3", "200000", "rs3200000", "G", "C", "80", "LowQual;STD_FILTER", 
                "DP=2;AP=6", "GT:DP", "1/0:3", "1/1:1", "0/1:5" }, sampleNames);
            VariantStats stats1 = new VariantStats("1", 100000, "A", "T,G", 0.01, 0.30, "A", "A/T", 2, 0, 1, true, 0.02, 0.10, 0.30, 0.15);
            VariantStats stats2 = new VariantStats("1", 200000, "G", "T", 0.05, 0.20, "T", "T/T", 1, 1, 0, true, 0.05, 0.20, 0.20, 0.10);
            VariantStats stats3 = new VariantStats("1", 300000, "G", "T", 0.06, 0.20, "T", "T/G", 1, 1, 0, true, 0.08, 0.30, 0.30, 0.20);
            VariantStats stats4 = new VariantStats("2", 100000, "G", "T", 0.05, 0.30, "C", "T/T", 1, 1, 0, true, 0.04, 0.20, 0.10, 0.10);
            VariantStats stats5 = new VariantStats("3", 200000, "G", "T", 0.02, 0.40, "C", "C/C", 1, 1, 0, true, 0.01, 0.40, 0.20, 0.15);
            VariantEffect eff1 = new VariantEffect("1", 100000, "A", "T", "", "RP11-206L10.6",
                    "intron", "processed_transcript", "1", 714473, 739298, "1", "", "", "",
                    "ENSG00000237491", "ENST00000429505", "RP11-206L10.6", "SO:0001627",
                    "intron_variant", "In intron", "feature", -1, "", "");
            VariantEffect eff2 = new VariantEffect("1", 100000, "A", "T", "ENST00000358533", "AL669831.1",
                    "downstream", "protein_coding", "1", 722513, 727513, "1", "", "", "",
                    "ENSG00000197049", "ENST00000358533", "AL669831.1", "SO:0001633",
                    "5KB_downstream_variant", "Within 5 kb downstream of the 3 prime end of a transcript", "feature", -1, "", "");
            VariantEffect eff3 = new VariantEffect("1", 200000, "C", "A", "ENST00000434264", "RP11-206L10.7",
                    "downstream", "lincRNA", "1", 720070, 725070, "1", "", "", "",
                    "ENSG00000242937", "ENST00000434264", "RP11-206L10.7", "SO:0001633",
                    "5KB_downstream_variant", "Within 5 kb downstream of the 3 prime end of a transcript", "feature", -1, "", "");

            List<VcfRecord> records = Arrays.asList(rec1, rec2, rec3, rec4, rec5);
            List<VariantStats> stats = Arrays.asList(stats1, stats2, stats3, stats4, stats5);
            List<VariantEffect> effects = Arrays.asList(eff1, eff2, eff3);
            assertTrue("Table creation could not be performed", writer.pre());
            assertTrue("Variants could not be written", writer.writeBatch(records));
            assertTrue("Stats could not be written", writer.writeVariantStats(stats));
            assertTrue("Effects could not be written", writer.writeVariantEffect(effects));
            writer.post();
            // Monbase query builder
            queryBuilder = new VariantMonbaseQueryBuilder(tableName, credentials);
        } catch (IllegalOpenCGACredentialsException e) {
            fail(e.getMessage());
        }

        assertNotNull("Monbase credentials must be not null", credentials);
        assertNotNull("Monbase writer must be not null", queryBuilder);
    }

    @Test
    public void testGetAllVariantsByRegion() {
        Region region = new Region("1", 0, 100000000);
        
        // Test query with stats and samples included
        QueryOptions options = new QueryOptions();
        options.put("stats", true);
        options.put("samples", true);
        QueryResult queryResult = queryBuilder.getAllVariantsByRegion(region, studyName, options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());
        
        Variant var1 = result.get(0);
        Map<String, String> sampleNA002 = new HashMap<>();
        sampleNA002.put("GT", "1/0");
        sampleNA002.put("DP", "2");
        
        Variant var2 = result.get(1);
        Variant var3 = result.get(2);
        
        assertEquals("1", var1.getChromosome());
        assertEquals(100000, var1.getPosition());
        assertEquals("T,G", var1.getAlternate());
        assertNotNull(var1.getSampleData());
        assertEquals(3, var1.getSampleData().size());
        assertNotNull(var1.getStats());
        assertEquals(0.01, var1.getStats().getMaf(), 1e-6);
        assertEquals(2, var1.getStats().getMissingAlleles());
        assertEquals(sampleNA002, var1.getSampleData().get("NA002"));
        
        assertEquals("1", var2.getChromosome());
        assertEquals(200000, var2.getPosition());
        assertEquals("T", var2.getAlternate());
        assertNotNull(var2.getSampleData());
        assertEquals(3, var2.getSampleData().size());
        assertNotNull(var2.getStats());
        assertEquals(0.05, var2.getStats().getMaf(), 1e-6);
        assertEquals(1, var2.getStats().getMissingAlleles());
        
        assertEquals("1", var3.getChromosome());
        assertEquals(300000, var3.getPosition());
        assertEquals("T", var3.getAlternate());
        assertNotNull(var3.getSampleData());
        assertEquals(3, var3.getSampleData().size());
        assertNotNull(var3.getStats());
        assertEquals(0.06, var3.getStats().getMaf(), 1e-6);
        assertEquals(1, var3.getStats().getMissingAlleles());
        
    }
    
    @Test
    public void testGetAllVariantsByRegionSamples() {
        // Test query only with samples
        Region region = new Region("1", 0, 100000000);
        QueryOptions options = new QueryOptions();
        options.put("samples", true);
        QueryResult queryResult = queryBuilder.getAllVariantsByRegion(region, studyName, options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());
        
        Variant var1 = result.get(0);
        Map<String, String> sampleNA002 = new HashMap<>();
        sampleNA002.put("GT", "1/0");
        sampleNA002.put("DP", "2");
        
        assertEquals("1", var1.getChromosome());
        assertEquals(100000, var1.getPosition());
        assertEquals("T,G", var1.getAlternate());
        assertNotNull(var1.getSampleData());
        assertEquals(3, var1.getSampleData().size());
        assertNull(var1.getStats());
        assertEquals(sampleNA002, var1.getSampleData().get("NA002"));
    }
    
    @Test
    public void testGetAllVariantsByRegionStats() {
        // Test query only with stats
        Region region = new Region("1", 0, 100000000);
        QueryOptions options = new QueryOptions();
        options.put("stats", true);
        QueryResult queryResult = queryBuilder.getAllVariantsByRegion(region, studyName, options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());
        
        Variant var1 = result.get(0);
        
        assertEquals("1", var1.getChromosome());
        assertEquals(100000, var1.getPosition());
        assertEquals("T,G", var1.getAlternate());
        assertNull(var1.getSampleData());
        assertNotNull(var1.getStats());
        assertEquals(0.01, var1.getStats().getMaf(), 1e-6);
        assertEquals(2, var1.getStats().getMissingAlleles());
    }
    
    @Test
    public void testGetSimpleVariantsByRegion() {
        Region region = new Region("1", 0, 100000000);
        
        // Test query with stats and samples included
        QueryOptions options = new QueryOptions();
        options.put("stats", true);
        options.put("effects", true);
        QueryResult queryResult = queryBuilder.getSimpleVariantsByRegion(region, studyName, options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());
        
        for (Variant v : result) {
            switch ((int) v.getPosition()) {
                case 100000:
                    assertEquals("1", v.getChromosome());
                    assertEquals("T,G", v.getAlternate());
                    assertNull(v.getSampleData());
                    assertNotNull(v.getStats());
                    assertEquals(0.01, v.getStats().getMaf(), 1e-6);
                    assertEquals(0, v.getStats().getMissingGenotypes());
                    assertNotNull(v.getEffect());
                    assertEquals(2, v.getEffect().size());
                    break;
                case 200000:
                    assertEquals("1", v.getChromosome());
                    assertEquals("T", v.getAlternate());
                    assertNull(v.getSampleData());
                    assertNotNull(v.getStats());
                    assertEquals(0.05, v.getStats().getMaf(), 1e-6);
                    assertEquals(1, v.getStats().getMissingGenotypes());
                    assertNotNull(v.getEffect());
                    assertEquals(1, v.getEffect().size());
                    break;
                case 300000:
                    assertEquals("1", v.getChromosome());
                    assertEquals("T", v.getAlternate());
                    assertNull(v.getSampleData());
                    assertNotNull(v.getStats());
                    assertEquals(0.06, v.getStats().getMaf(), 1e-6);
                    assertEquals(1, v.getStats().getMissingGenotypes());
                    assertNotNull(v.getEffect());
                    assertEquals(0, v.getEffect().size());
            }
        }
    }
    
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