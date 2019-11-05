package org.opencb.opencga.storage.mongodb.variant.adaptors;

import org.junit.After;
import org.junit.Before;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptorMultiFileSpecificSamplesCollectionTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

/**
 * Created on 23/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantMongoDBAdaptorMultiFileSpecificSamplesCollectionTest extends VariantDBAdaptorMultiFileSpecificSamplesCollectionTest implements MongoDBVariantStorageTest {

    @Before
    public void setUpLoggers() throws Exception {
        logLevel("debug");
    }

    @After
    public void resetLoggers() throws Exception {
        logLevel("info");
    }

    @Override
    public ObjectMap getOptions() {
        return new ObjectMap(VariantStorageOptions.MERGE_MODE.key(), VariantStorageEngine.MergeMode.BASIC);
    }
}