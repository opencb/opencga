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

package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantAggregatedVcfFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.biodata.tools.variant.tasks.VariantStatsTask;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.datastore.mongodb.MongoDataStore;
import org.opencb.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import static junit.framework.Assert.assertEquals;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
@Ignore
public class VariantMongoDBWriterTest {

    private static Properties properties;
    private static String inputFile = VariantMongoDBWriterTest.class.getResource("/variant-test-file.vcf.gz").getFile();
    private static String firstAggregatedInputFile = VariantMongoDBWriterTest.class.getResource("/aggregated_example_1.vcf.gz").getFile();
    private static String secondAggregatedInputFile = VariantMongoDBWriterTest.class.getResource("/aggregated_example_2.vcf.gz").getFile();
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
        MongoClient mongoClient = new MongoClient("localhost");
        DB db = mongoClient.getDB(credentials.getMongoDbName());
        db.dropDatabase();
        mongoClient.close();
    }

    @Test
    public void test() throws IOException {
        VariantReader reader = new VariantVcfReader(study1, inputFile);

        List<Task<Variant>> taskList = new SortedList<>();
        List<VariantWriter> writers = new ArrayList<>();

//        writers.add(new VariantMongoDBWriter(study1, credentials));

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
        taskList.clear();
        taskList.add(new VariantStatsTask(reader, study2));
        writers.clear();
        writers.add(new VariantMongoDBWriter(study2, credentials));
        vr = new VariantRunner(study2, reader, null, writers, taskList);
        vr.run();
    }

    /**
     * tests whether the indexing of two aggregated files in the same database overwrites data or not.
     * Specifically, the most dangerous risks, are:
     * * already present sources entries being overwritten or new source entries not written.
     * * the same with stats.
     * @throws IOException
     */
    @Test
    public void aggregatedTest() throws IOException {

        //Reader
        VariantReader reader = new VariantVcfReader(study1, firstAggregatedInputFile, new VariantAggregatedVcfFactory());

        //Writers

        String variantsCollection = "variants";
        VariantMongoDBWriter dbWriter = new VariantMongoDBWriter(study1, credentials, variantsCollection, "files", false, true, false);

        List<VariantWriter> writers = Collections.<VariantWriter>singletonList(dbWriter);

        //Runner
        VariantRunner vr = new VariantRunner(study1, reader, null, writers, Collections.EMPTY_LIST, 40);
        vr.run();

        // second runner, we can reuse the writer because it is the
        reader = new VariantVcfReader(study1, secondAggregatedInputFile, new VariantAggregatedVcfFactory());
        vr = new VariantRunner(study1, reader, null, writers, Collections.EMPTY_LIST, 40);
        vr.run();

        // test
        MongoDataStoreManager mongoDataStoreManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
        MongoDataStore mongoDataStore = mongoDataStoreManager.get(credentials.getMongoDbName(), credentials.getMongoDBConfiguration());
        DB db = mongoDataStore.getDb();
        DBCollection collection = db.getCollection(variantsCollection);

        BasicDBObject find = new BasicDBObject("_id", "1_664010_A_C")   // appears on firstAggregatedInputFile
                .append("files", new BasicDBObject("$size", 1))
                .append("st", new BasicDBObject("$size", 1));
        assertEquals(1, collection.find(find).count()); // there's only one matching document with 1 element in files and stats


        find = new BasicDBObject("_id", "1_664010_A_T") // appears on secondAggregatedInputFile
                .append("files", new BasicDBObject("$size", 1))
                .append("st", new BasicDBObject("$size", 1));
        assertEquals(1, collection.find(find).count());


        find = new BasicDBObject("_id", "1_701835_T_C") // appears on both files with different values
                .append("files", new BasicDBObject("$size", 2))
                .append("st", new BasicDBObject("$size", 2));
        assertEquals(1, collection.find(find).count());

        // appears on both files with identical values. this represents an updated file, not different files.
        // if different files were to be written, different variant sources should be used
        find = new BasicDBObject("_id", "1_762409_G_C")
                .append("files", new BasicDBObject("$size", 1))
                .append("st", new BasicDBObject("$size", 1));
        assertEquals(1, collection.find(find).count());
    }
}
