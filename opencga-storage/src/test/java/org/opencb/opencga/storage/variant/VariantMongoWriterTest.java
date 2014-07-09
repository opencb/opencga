package org.opencb.opencga.storage.variant;

import org.opencb.opencga.storage.variant.mongodb.VariantMongoWriter;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoWriterTest {

    private static Properties properties;
    private static String inputFile = VariantMongoWriterTest.class.getResource("/variant-test-file.vcf.gz").getFile();
    private static MongoCredentials credentials;
    private static VariantSource study1 = new VariantSource(inputFile, "testAlias", "testStudy1", "Test Study #1");
    private static VariantSource study2 = new VariantSource(inputFile, "testAlias2", "testStudy2", "Test Study #2");

    @BeforeClass
    public static void setUp() throws IOException {
        properties = new Properties();
        properties.put("mongo_host", "localhost");
        properties.put("mongo_port", "27017");
        properties.put("mongo_db_name", "VariantMongoWriterTest_db");

        credentials = new MongoCredentials(properties);
    }

    @AfterClass
    public static void shutdown() throws Exception {
        // Delete Mongo collection
        MongoClient mongoClient = new MongoClient(credentials.getMongoHost());
        DB db = mongoClient.getDB(credentials.getMongoDbName());
        db.dropDatabase();
        mongoClient.close();
    }

    @Test
    public void test() throws IOException {
        VariantReader reader = new VariantVcfReader(study1, inputFile);

        List<Task<Variant>> taskList = new SortedList<>();
        List<VariantWriter> writers = new ArrayList<>();

        writers.add(new VariantMongoWriter(study1, credentials));

        for (VariantWriter vw : writers) {
            vw.includeStats(true);
//            vw.includeSamples(true);
//            vw.includeEffect(true);
        }

        taskList.add(new VariantStatsTask(reader, study1));
//        taskList.add(new VariantEffectTask());

        VariantRunner vr = new VariantRunner(study1, reader, null, writers, taskList);
        vr.run();

        reader = new VariantVcfReader(study2, inputFile);
        vr = new VariantRunner(study2, reader, null, writers, taskList);
        vr.run();
    }
}
