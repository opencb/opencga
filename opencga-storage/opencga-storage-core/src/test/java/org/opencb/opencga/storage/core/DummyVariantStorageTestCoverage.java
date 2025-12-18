package org.opencb.opencga.storage.core;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import java.util.Map;
import java.util.Set;

/**
 * Reports which @StorageEngineTest classes are still missing dummy implementations.
 * This is informational only and will not fail the build.
 */
@Category(ShortTests.class)
public class DummyVariantStorageTestCoverage {

    private static final String DUMMY_TEST_PACKAGE = "org.opencb.opencga.storage.core.variant.dummy";

    @Test
    public void reportMissingDummyImplementations() {
        Set<Class<?>> coreTests = StorageEngineTestRegistry.findAnnotatedCoreTests();
        Map<Class<?>, Set<Class<?>>> implementationMap = StorageEngineTestRegistry.findImplementations(DUMMY_TEST_PACKAGE);
        Set<Class<?>> missing = StorageEngineTestRegistry.findMissingImplementations(coreTests, implementationMap);
        if (!missing.isEmpty()) {
            System.out.println("Dummy implementations missing for:");
            int i = 1;
            for (Class<?> aClass : missing) {
                System.out.println(i++ + ". " + aClass.getName());
            }
        }
    }
}
