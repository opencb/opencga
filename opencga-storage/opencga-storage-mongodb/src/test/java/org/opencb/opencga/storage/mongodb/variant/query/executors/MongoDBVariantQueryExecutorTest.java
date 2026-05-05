package org.opencb.opencga.storage.mongodb.variant.query.executors;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.query.executors.VariantQueryExecutorTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.IOException;

@Category(MediumTests.class)
public class MongoDBVariantQueryExecutorTest extends VariantQueryExecutorTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

    @After
    public void closeMongo() throws IOException {
        closeConnections();
    }
}
