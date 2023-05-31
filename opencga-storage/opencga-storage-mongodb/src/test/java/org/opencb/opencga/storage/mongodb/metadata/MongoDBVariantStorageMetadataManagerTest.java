package org.opencb.opencga.storage.mongodb.metadata;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManagerTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

@Category(ShortTests.class)
public class MongoDBVariantStorageMetadataManagerTest extends VariantStorageMetadataManagerTest implements MongoDBVariantStorageTest {
}
