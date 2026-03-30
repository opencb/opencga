package org.opencb.opencga.storage.mongodb.variant;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineDeleteTest;

@Category(MediumTests.class)
public class MongoDBVariantStorageEngineDeleteTest extends VariantStorageEngineDeleteTest implements MongoDBVariantStorageTest {
}
