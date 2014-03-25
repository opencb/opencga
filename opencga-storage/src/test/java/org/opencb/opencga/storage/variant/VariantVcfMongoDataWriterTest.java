package org.opencb.opencga.storage.variant;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.commons.test.GenericTest;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.opencb.biodata.formats.variant.vcf4.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
public class VariantVcfMongoDataWriterTest extends GenericTest {

    private static Properties properties;
    private static String inputFile = VariantVcfMongoDataWriterTest.class.getResource("/variant-test-file.vcf.gz").getFile();
    private static MongoCredentials credentials;
    private static VariantSource study = new VariantSource("testStudy", "testAlias", "testStudy", null, null);


    @BeforeClass
    public static void initMongo() throws IOException {

        properties = new Properties();
        properties.put("mongo_host", "localhost");
        properties.put("mongo_port", 27017);
        properties.put("mongo_db_name", "testIndex");
        properties.put("mongo_user", "user");
        properties.put("mongo_password", "pass");

        credentials = new MongoCredentials(properties);

        MongoClient mc = new MongoClient(credentials.getMongoHost());
        DB db = mc.getDB(credentials.getMongoDbName());
        db.dropDatabase();

        List<Task<Variant>> taskList = new SortedList<>();
        List<VariantWriter> writers = new ArrayList<>();

        VariantReader reader;
        reader = new VariantVcfReader(inputFile);
        writers.add(new VariantVcfMongoDataWriter(study, "opencga-hsapiens", credentials));

        for (VariantWriter vw : writers) {
            vw.includeStats(true);
//            vw.includeSamples(true);
//            vw.includeEffect(true);
        }

        taskList.add(new VariantStatsTask(reader, study));
//        taskList.add(new VariantEffectTask());


        VariantRunner vr = new VariantRunner(study, reader, null, writers, taskList);
        vr.run();

        study.setName("testStudy2");
        vr.run();


    }

    @Test
    public void testName() throws Exception {


    }
}
