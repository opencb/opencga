package org.opencb.opencga.storage.mongodb.variant.index.sample;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

@Category(LongTests.class)
public class MongodBSampleIndexTest extends SampleIndexTest implements MongoDBVariantStorageTest {

}
