package org.opencb.opencga.storage.mongodb.variant;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineNumericSampleTest;

@Category(MediumTests.class)
public class MongoVariantStorageEngineNumericSampleTest extends VariantStorageEngineNumericSampleTest implements MongoDBVariantStorageTest {
}
