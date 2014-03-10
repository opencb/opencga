package org.opencb.opencga.storage.variant;

import org.apache.commons.lang.mutable.Mutable;
import org.apache.commons.lang.mutable.MutableInt;
import org.junit.*;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.MongoCredentials;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
public class VariantMongoQueryBuilderTest extends GenericTest {

    private static Properties properties;
    private static String inputFile = VariantVcfMongoDataWriterTest.class.getResource("/variant-test-file.vcf.gz").getFile();
    private static MongoCredentials credentials;
    private static VariantStudy study = new VariantStudy("testStudy", "testAlias", "testStudy", null, null);
    private static VariantQueryBuilder vqb;


    @BeforeClass
    public static void initMongo() throws IOException {
        properties = new Properties();
        properties.put("mongo_host", "localhost");
        properties.put("mongo_port", 27017);
        properties.put("mongo_db_name", "cibererStudies");
        properties.put("mongo_user", "user");
        properties.put("mongo_password", "pass");
        credentials = new MongoCredentials(properties);


    }


//    @Ignore
//    @BeforeClass
//    public static void initMongo() throws IOException {
//
//        properties = new Properties();
//        properties.put("mongo_host", "localhost");
//        properties.put("mongo_port", 27017);
//        properties.put("mongo_db_name", "testIndex");
//        properties.put("mongo_user", "user");
//        properties.put("mongo_password", "pass");
//
//        credentials = new MongoCredentials(properties);
//
//        MongoClient mc = new MongoClient(credentials.getMongoHost());
//        DB db = mc.getDB(credentials.getMongoDbName());
//        db.dropDatabase();
//
//        List<Task<Variant>> taskList = new SortedList<>();
//        List<VariantWriter> writers = new ArrayList<>();
//
//        VariantReader reader;
//        reader = new VariantVcfReader(inputFile);
//        writers.add(new VariantVcfMongoDataWriter(study, "opencga-hsapiens", credentials));
//
//        for (VariantWriter vw : writers) {
//            vw.includeStats(true);
//            vw.includeSamples(true);
////            vw.includeEffect(true);
//        }
//
//        taskList.add(new VariantStatsTask(reader, study));
////        taskList.add(new VariantEffectTask());
//
//        for (int i = 0; i < 10; i++) {
//            study.setName("test" + i);
//            VariantRunner vr = new VariantRunner(study, reader, null, writers, taskList);
//            vr.run();
//
//        }
//
//
//        vqb = new VariantMongoQueryBuilder(new MongoCredentials(properties));
//    }


    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        vqb = new VariantMongoQueryBuilder(credentials);

    }

    @Override
    @After
    public void tearDown() throws Exception {
        vqb.close();
        super.tearDown();
    }

    @Test
    public void testGetRecords() throws Exception {

        Map<String, String> opts = new HashMap<>();
        opts.put("studyId", "aaleman_-_XOidGTJMUq1Cr1J");
//        opts.put("region_list", "6:1-15021068");
//        opts.put("sampleGT_D801[]", "1/1,0/1");
//        opts.put("sampleGT_muestra_B[]", "0/1");
//        opts.put("conseq_type[]", "non_synonymous_codon,intron_variant");
//        opts.put("mend_error", "1");
//        opts.put("option_mend_error", ">=");
//        opts.put("maf", "0.1");
//        opts.put("option_maf", "<=");

        MutableInt count = new MutableInt(-1);

        QueryResult<VariantInfo> records = ((VariantMongoQueryBuilder) vqb).getRecordsMongo(1, 0, 25, count, opts);
//
        System.out.println(records.getResult().get(0).getSampleGenotypes());
    }

    @Test
    public void testAnalysisInfo() throws Exception {

        QueryResult<VariantAnalysisInfo> res = ((VariantMongoQueryBuilder) vqb).getAnalysisInfo("aaleman_-_XOidGTJMUq1Cr1J");
        VariantAnalysisInfo vi = res.getResult().get(0);

        System.out.println("vi.getSamples() = " + vi.getSamples());
        System.out.println("vi.getConsequenceTypes() = " + vi.getConsequenceTypes());
        System.out.println("vi.getGlobalStats() = " + vi.getGlobalStats());


    }
}