package org.opencb.opencga.storage.variant;

import com.mongodb.BasicDBList;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.junit.*;
import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantReader;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantVcfReader;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.VariantWriter;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.containers.map.QueryOptions;
import org.opencb.commons.run.Task;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

import java.io.IOException;
import java.util.*;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
public class VariantMongoQueryBuilderTest extends GenericTest {

    private static Properties properties;
    private static String inputFile = VariantVcfMongoDataWriterTest.class.getResource("/variant-test-file.vcf.gz").getFile();
    private static MongoCredentials credentials;
    private static VariantStudy study = new VariantStudy("testStudy", "testAlias", "testStudy", null, null);
    private static VariantQueryBuilder vqb;


    private String userName = "TEST";

    @BeforeClass
    public static void initMongo() throws IOException {
        properties = new Properties();
        properties.put("mongo_host", "localhost");
        properties.put("mongo_port", 27017);
        properties.put("mongo_db_name", "aleman");
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
        opts.put("studyId", "TEST5");

        List<VariantInfo> records = vqb.getRecords(opts);

        System.out.println(records.size());


    }
}
