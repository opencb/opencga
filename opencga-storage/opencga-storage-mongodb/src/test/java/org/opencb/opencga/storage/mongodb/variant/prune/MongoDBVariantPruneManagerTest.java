package org.opencb.opencga.storage.mongodb.variant.prune;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.prune.VariantPruneManagerTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

@Category(MediumTests.class)
public class MongoDBVariantPruneManagerTest extends VariantPruneManagerTest implements MongoDBVariantStorageTest {
}
