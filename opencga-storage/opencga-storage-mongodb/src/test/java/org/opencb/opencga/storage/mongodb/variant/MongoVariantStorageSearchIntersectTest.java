package org.opencb.opencga.storage.mongodb.variant;

import org.junit.After;
import org.junit.Before;
import org.opencb.opencga.storage.core.variant.VariantStorageSearchIntersectTest;

/**
 * Created on 04/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoVariantStorageSearchIntersectTest extends VariantStorageSearchIntersectTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

}
