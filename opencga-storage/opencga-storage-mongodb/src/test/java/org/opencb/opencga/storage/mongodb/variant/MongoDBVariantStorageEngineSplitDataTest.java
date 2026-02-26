package org.opencb.opencga.storage.mongodb.variant;

import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSplitDataTest;

/**
 * Tests for loading multiple VCF files for the same samples using {@link VariantStorageEngine.SplitData#MULTI}
 * in the MongoDB storage backend.
 */
@Category(MediumTests.class)
public class MongoDBVariantStorageEngineSplitDataTest extends VariantStorageEngineSplitDataTest implements MongoDBVariantStorageTest {

}
