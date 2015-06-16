/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.hadoop.variant;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.effect.VariantEffect;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.auth.MonbaseCredentials;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 * @author Jesus Rodriguez <jesusrodrc@gmail.com>
 */
public class VariantHbaseDBAdaptorTest {

    private static final String tableName = "test_VariantMonbaseQueryBuilderTest";
    private static VariantSource study = new VariantSource("testStudy", "testAlias", "testStudy", null, null);
    private static MonbaseCredentials credentials;
    private static org.apache.hadoop.conf.Configuration config;
    private static VariantHbaseWriter writer;
    private static VariantHbaseDBAdaptor queryBuilder;

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
            writer = new VariantHbaseWriter(study, tableName, credentials);
            assertTrue(writer.open());
            List<String> sampleNames = Arrays.asList("NA001", "NA002", "NA003");
            String[] fields1 = new String[]{"1", "100000", "rs1100000", "A", "T,G", "40", "PASS",
                "DP=5;AP=10;H2", "GT:DP", "1/1:4", "1/0:2", "0/0:3"};
            String[] fields2 = new String[]{"1", "200000", "rs1200000", "G", "T", "30", "LowQual",
                "DP=2;AP=5", "GT:DP", "1/1:3", "1/1:1", "0/0:5"};
            String[] fields3 = new String[]{"1", "300000", "rs1300000", "C", "T", "50", "PASS",
                "DP=1;AP=6", "GT:DP", "1/0:3", "0/1:1", "0/0:5"};
            String[] fields4 = new String[]{"2", "100000", "rs2100000", "G", "A", "60", "STD_FILTER",
                "DP=3;AP=8", "GT:DP", "1/1:3", "1/0:1", "0/0:5"};
            String[] fields5 = new String[]{"3", "200000", "rs3200000", "G", "C", "80", "LowQual;STD_FILTER",
                "DP=2;AP=6", "GT:DP", "1/0:3", "1/1:1", "0/1:5"};
            Variant rec1 = VariantFactory.createVariantFromVcf(sampleNames, fields1).get(0);
            Variant rec2 = VariantFactory.createVariantFromVcf(sampleNames, fields2).get(0);
            Variant rec3 = VariantFactory.createVariantFromVcf(sampleNames, fields3).get(0);
            Variant rec4 = VariantFactory.createVariantFromVcf(sampleNames, fields4).get(0);
            Variant rec5 = VariantFactory.createVariantFromVcf(sampleNames, fields5).get(0);

            VariantStats stats1 = new VariantStats("1", 100000, "A", "T,G", 0.01, 0.30, "A", "A/T", 2, 0, 1, true, 0.02, 0.10, 0.30, 0.15);
            VariantStats stats2 = new VariantStats("1", 200000, "G", "T", 0.05, 0.20, "T", "T/T", 1, 1, 0, true, 0.05, 0.20, 0.20, 0.10);
            VariantStats stats3 = new VariantStats("1", 300000, "G", "T", 0.06, 0.20, "T", "T/G", 1, 1, 0, true, 0.08, 0.30, 0.30, 0.20);
            VariantStats stats4 = new VariantStats("2", 100000, "G", "T", 0.05, 0.30, "C", "T/T", 1, 1, 0, true, 0.04, 0.20, 0.10, 0.10);
            VariantStats stats5 = new VariantStats("3", 200000, "G", "T", 0.02, 0.40, "C", "C/C", 1, 1, 0, true, 0.01, 0.40, 0.20, 0.15);
            rec1.setStats(stats1);
            rec2.setStats(stats2);
            rec3.setStats(stats3);
            rec4.setStats(stats4);
            rec5.setStats(stats5);

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
            rec1.setEffect(Arrays.asList(eff1, eff2));
            rec2.setEffect(Arrays.asList(eff3));

            List<Variant> records = Arrays.asList(rec1, rec2, rec3, rec4, rec5);
            assertTrue("Table creation could not be performed", writer.pre());
            assertTrue("Variants could not be written", writer.write(records));
//            assertTrue("Variants could not be written", writer.writeBatch(records));
//            assertTrue("Stats could not be written", writer.writeVariantStats(records));
//            assertTrue("Effects could not be written", writer.writeVariantEffect(records));
            writer.post();
            // Monbase query builder
            queryBuilder = new VariantHbaseDBAdaptor(tableName, credentials);
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
        QueryResult queryResult = queryBuilder.getAllVariantsByRegionAndStudy(region, study.getFileId(), options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());

        Variant var1 = result.get(0);
        Map<String, String> sampleNA002 = new HashMap<>();
        sampleNA002.put("GT", "1/0");
        sampleNA002.put("DP", "2");

        Variant var2 = result.get(1);
        Variant var3 = result.get(2);

        assertEquals("1", var1.getChromosome());
        assertEquals(100000, var1.getStart());
        assertEquals("T,G", var1.getAlternate());
        assertEquals(3, var1.getSamplesData().size());
        assertNotNull(var1.getStats());
        assertEquals(0.01, var1.getStats().getMaf(), 1e-6);
        assertEquals(2, var1.getStats().getMissingAlleles());
        assertEquals(sampleNA002, var1.getSampleData("NA002"));

        assertEquals("1", var2.getChromosome());
        assertEquals(200000, var2.getStart());
        assertEquals("T", var2.getAlternate());
        assertEquals(3, var2.getSamplesData().size());
        assertNotNull(var2.getStats());
        assertEquals(0.05, var2.getStats().getMaf(), 1e-6);
        assertEquals(1, var2.getStats().getMissingAlleles());

        assertEquals("1", var3.getChromosome());
        assertEquals(300000, var3.getStart());
        assertEquals("T", var3.getAlternate());
        assertEquals(3, var3.getSamplesData().size());
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
        QueryResult queryResult = queryBuilder.getAllVariantsByRegionAndStudy(region, study.getFileId(), options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());

        Variant var1 = result.get(0);
        Map<String, String> sampleNA002 = new HashMap<>();
        sampleNA002.put("GT", "1/0");
        sampleNA002.put("DP", "2");

        assertEquals("1", var1.getChromosome());
        assertEquals(100000, var1.getStart());
        assertEquals("T,G", var1.getAlternate());
        assertEquals(3, var1.getSamplesData().size());
        assertNull(var1.getStats());
        assertEquals(sampleNA002, var1.getSampleData("NA002"));
    }

    @Test
    public void testGetAllVariantsByRegionStats() {
        // Test query only with stats
        Region region = new Region("1", 0, 100000000);
        QueryOptions options = new QueryOptions();
        options.put("stats", true);
        QueryResult queryResult = queryBuilder.getAllVariantsByRegionAndStudy(region, study.getFileId(), options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());

        Variant var1 = result.get(0);

        assertEquals("1", var1.getChromosome());
        assertEquals(100000, var1.getStart());
        assertEquals("T,G", var1.getAlternate());
        assertTrue(var1.getSamplesData().isEmpty());
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
        QueryResult queryResult = queryBuilder.getSimpleVariantsByRegion(region, study.getFileId(), options);
        List<Variant> result = queryResult.getResult();
        assertEquals(3, result.size());

        for (Variant v : result) {
            switch ((int) v.getStart()) {
                case 100000:
                    assertEquals("1", v.getChromosome());
                    assertEquals("T,G", v.getAlternate());
                    assertTrue(v.getSamplesData().isEmpty());
                    assertNotNull(v.getStats());
                    assertEquals(0.01, v.getStats().getMaf(), 1e-6);
                    assertEquals(0, v.getStats().getMissingGenotypes());
                    assertNotNull(v.getEffect());
                    assertEquals(2, v.getEffect().size());
                    break;
                case 200000:
                    assertEquals("1", v.getChromosome());
                    assertEquals("T", v.getAlternate());
                    assertTrue(v.getSamplesData().isEmpty());
                    assertNotNull(v.getStats());
                    assertEquals(0.05, v.getStats().getMaf(), 1e-6);
                    assertEquals(1, v.getStats().getMissingGenotypes());
                    assertNotNull(v.getEffect());
                    assertEquals(1, v.getEffect().size());
                    break;
                case 300000:
                    assertEquals("1", v.getChromosome());
                    assertEquals("T", v.getAlternate());
                    assertTrue(v.getSamplesData().isEmpty());
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
