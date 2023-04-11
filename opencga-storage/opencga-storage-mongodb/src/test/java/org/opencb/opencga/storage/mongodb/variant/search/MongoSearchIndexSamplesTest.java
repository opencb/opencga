package org.opencb.opencga.storage.mongodb.variant.search;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.search.SearchIndexSamplesTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

/**
 * Created on 20/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Category(ShortTests.class)
public class MongoSearchIndexSamplesTest extends SearchIndexSamplesTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() {
        logLevel("debug");
    }

    @After
    public void resetLoggers() {
        logLevel("info");
    }

}
