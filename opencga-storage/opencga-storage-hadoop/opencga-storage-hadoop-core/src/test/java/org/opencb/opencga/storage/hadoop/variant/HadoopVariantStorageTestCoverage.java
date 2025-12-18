package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;
import org.opencb.opencga.storage.core.StorageEngineTestRegistry;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@Category(ShortTests.class)
public class HadoopVariantStorageTestCoverage {

    private static final String HADOOP_TEST_PACKAGE = "org.opencb.opencga.storage.hadoop";

    @Test
    public void testStorageEngineCoverage() {
        Set<Class<?>> coreTests = StorageEngineTestRegistry.findAnnotatedCoreTests();
        Map<Class<?>, Set<Class<?>>> implementationMap = StorageEngineTestRegistry.findImplementations(HADOOP_TEST_PACKAGE);
        Set<Class<?>> missing = StorageEngineTestRegistry.findMissingImplementations(coreTests, implementationMap);
        if (!missing.isEmpty()) {
            System.out.println("Hadoop implementations missing for:");
            int i = 1;
            for (Class<?> aClass : missing) {
                System.out.println(i++ + ". " + aClass.getName());
            }
        }
        assertTrue("Missing Hadoop implementations for: " + missing, missing.isEmpty());
    }
}
