package org.opencb.opencga.storage.mongodb.variant.stats;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerMultiFilesTest;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageTest;

@Category(ShortTests.class)
public class MongoVariantStatisticsManagerMultiFilesTest extends VariantStatisticsManagerMultiFilesTest
        implements MongoDBVariantStorageTest {
}
