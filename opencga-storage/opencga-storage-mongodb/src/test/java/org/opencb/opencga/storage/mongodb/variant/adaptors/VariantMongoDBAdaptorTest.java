package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

import java.io.IOException;

/**
 * Created on 22/12/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class VariantMongoDBAdaptorTest extends VariantDBAdaptorTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

    @Override
    public void after() throws IOException {
        super.after();
        closeConnections();
    }
}
