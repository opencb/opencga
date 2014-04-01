package org.opencb.opencga.storage.variant;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.junit.*;
import static org.junit.Assert.assertEquals;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantWriter;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantEffectTask;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoQueryBuilderTest extends GenericTest {

    private static String inputFile = VariantVcfMongoDataWriterTest.class.getResource("/variant-test-file.vcf.gz").getFile();
    private static VariantSource study = new VariantSource("testStudy", "testAlias", "Study for testing purposes", null, null);
    private static MongoCredentials credentials;
    private static VariantQueryBuilder vqb;

    @BeforeClass
    public static void initialize() throws IOException {
        // Initialize connection properties
        Properties properties = new Properties();
        properties.put("mongo_host", "localhost");
        properties.put("mongo_port", "27017");
        properties.put("mongo_db_name", "VariantMongoQueryBuilderTest_db");
//        properties.put("mongo_user", "user");
//        properties.put("mongo_password", "pass");
        credentials = new MongoCredentials(properties);
        
        // Initialize dataset to query
        VariantVcfReader reader = new VariantVcfReader(inputFile, inputFile, study.getName());
        VariantVcfMongoDataWriter vdw = new VariantVcfMongoDataWriter(study, "hsapiens", (MongoCredentials) credentials);
        List<VariantWriter> writers = new LinkedList<>(); writers.add(vdw);
        VariantRunner vr = new VariantRunner(study, reader, null, writers,  Arrays.asList(new VariantEffectTask(), new VariantStatsTask(reader, study)));
        vr.run();
        
        // Initialize query builder
        vqb = new VariantMongoQueryBuilder(credentials);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        // Close query builder
        vqb.close();
        
        // Delete Mongo collection
        MongoClient mongoClient = new MongoClient(credentials.getMongoHost());
        DB db = mongoClient.getDB(credentials.getMongoDbName());
        db.dropDatabase();
        mongoClient.close();
    }
    
    @Test
    public void testGetAllVariantsByRegionAndStudy() {
        QueryResult queryResult;
        
        // Basic queries
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("1:1000000-2000000"), study.getAlias(), null);
        assertEquals(3, queryResult.getResult().size());
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("1:10000000-20000000"), study.getAlias(), null);
        assertEquals(11, queryResult.getResult().size());
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("3:1-200000000"), study.getAlias(), null);
        assertEquals(50, queryResult.getResult().size());
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("X:1-200000000"), study.getAlias(), null);
        assertEquals(11, queryResult.getResult().size());
        
        // Exactly in the limits
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("20:238441-7980390"), study.getAlias(), null);
        assertEquals(5, queryResult.getResult().size());
        
        // Just inside the limits
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("20:238440-7980391"), study.getAlias(), null);
        assertEquals(5, queryResult.getResult().size());
        
        // Just outside the limits
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("20:238441-7980389"), study.getAlias(), null);
        assertEquals(4, queryResult.getResult().size());
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("20:238442-7980390"), study.getAlias(), null);
        assertEquals(4, queryResult.getResult().size());
        queryResult = vqb.getAllVariantsByRegionAndStudy(new Region("20:238442-7980389"), study.getAlias(), null);
        assertEquals(3, queryResult.getResult().size());
    }

//    @Test
//    public void testGetRecords() throws Exception {
//
//        Map<String, String> opts = new HashMap<>();
//        opts.put("studyId", "aaleman_-_XOidGTJMUq1Cr1J");
////        opts.put("region_list", "6:1-15021068");
////        opts.put("sampleGT_D801[]", "1/1,0/1");
////        opts.put("sampleGT_muestra_B[]", "0/1");
////        opts.put("conseq_type[]", "non_synonymous_codon,intron_variant");
////        opts.put("mend_error", "1");
////        opts.put("option_mend_error", ">=");
////        opts.put("maf", "0.1");
////        opts.put("option_maf", "<=");
//
//        MutableInt count = new MutableInt(-1);
//
//        QueryResult<VariantInfo> records = ((VariantMongoQueryBuilder) vqb).getRecordsMongo(1, 0, 25, count, opts);
////
//        System.out.println(records.getResult().get(0).getSampleGenotypes());
//    }
//
//    @Test
//    public void testAnalysisInfo() throws Exception {
//
//        QueryResult<VariantAnalysisInfo> res = ((VariantMongoQueryBuilder) vqb).getAnalysisInfo("aaleman_-_XOidGTJMUq1Cr1J");
//        VariantAnalysisInfo vi = res.getResult().get(0);
//
//        System.out.println("vi.getSamples() = " + vi.getSamples());
//        System.out.println("vi.getConsequenceTypes() = " + vi.getConsequenceTypes());
//        System.out.println("vi.getGlobalStats() = " + vi.getGlobalStats());
//
//
//    }
}