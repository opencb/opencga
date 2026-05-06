package org.opencb.opencga.storage.mongodb.variant;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.StorageEngineTestRegistry;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@Category(ShortTests.class)
public class MongoVariantStorageTestCoverage {

    private static final String MONGODB_TEST_PACKAGE = "org.opencb.opencga.storage.mongodb";

    @Test
    public void testStorageEngineCoverage() {
        Set<Class<?>> coreTests = StorageEngineTestRegistry.findAnnotatedCoreTests();
        Map<Class<?>, Set<Class<?>>> implementationMap = StorageEngineTestRegistry.findImplementations(MONGODB_TEST_PACKAGE);
        Set<Class<?>> missing = StorageEngineTestRegistry.findMissingImplementations(coreTests, implementationMap);
        if (!missing.isEmpty()) {
            System.out.println("MongoDB implementations missing for:");
            int i = 1;
            for (Class<?> aClass : missing) {
                System.out.println(i++ + ". " + aClass.getName());
            }
        }
        assertTrue("Missing MongoDB implementations for: " + missing, missing.isEmpty());
    }
}
