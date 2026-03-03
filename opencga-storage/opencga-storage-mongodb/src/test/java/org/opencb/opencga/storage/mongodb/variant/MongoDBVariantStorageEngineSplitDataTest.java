package org.opencb.opencga.storage.mongodb.variant;

import org.junit.Assume;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineSplitDataTest;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;

/**
 * Tests for loading multiple VCF files for the same samples using {@link VariantStorageEngine.SplitData#MULTI}
 * in the MongoDB storage backend.
 */
@Category(MediumTests.class)
public class MongoDBVariantStorageEngineSplitDataTest extends VariantStorageEngineSplitDataTest implements MongoDBVariantStorageTest {

    @Override
    public void testLoadAndRemoveSamples() throws Exception {
        Assume.assumeTrue("removeSamples not implemented for MongoDB", false);
    }

    /**
     * MongoDB's {@code index()} bypasses {@code load()} and calls {@code stage()} / {@code directLoad()} directly.
     * Mock those methods instead so that {@code failAtLoadingFile()} tests work correctly.
     */
    @Override
    protected void mockLoadFailure(VariantStoragePipeline mockedPipeline) throws Exception {
        MongoDBVariantStoragePipeline mongoPipeline = (MongoDBVariantStoragePipeline) mockedPipeline;

        Mockito.doAnswer(invocation -> {
            System.out.printf("MOCKED stage(%s, %s)%n", invocation.getArgument(0), invocation.getArgument(1));
            throw new StorageEngineException(MOCKED_EXCEPTION);
        }).when(mongoPipeline).stage(Mockito.any(), Mockito.any());

        Mockito.doAnswer(invocation -> {
            System.out.printf("MOCKED directLoad(%s, %s)%n", invocation.getArgument(0), invocation.getArgument(1));
            throw new StorageEngineException(MOCKED_EXCEPTION);
        }).when(mongoPipeline).directLoad(Mockito.any(), Mockito.any());
    }

}
