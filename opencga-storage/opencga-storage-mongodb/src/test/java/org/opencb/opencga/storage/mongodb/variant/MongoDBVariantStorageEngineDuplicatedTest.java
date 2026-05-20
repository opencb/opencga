package org.opencb.opencga.storage.mongodb.variant;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.testclassification.duration.MediumTests;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngineDuplicatedTest;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.mongodb.variant.load.MongoDBVariantWriteResult;

import java.net.URI;

@Category(MediumTests.class)
public class MongoDBVariantStorageEngineDuplicatedTest extends VariantStorageEngineDuplicatedTest implements MongoDBVariantStorageTest {

    /**
     * Reproduces TASK-8334: when {@code AbstractDuplicatedVariantsResolver.filterNewRefBlocks}
     * silently drops normalizer-generated NO_VARIATION ref blocks, the
     * {@code VariantDeduplicationTask} still counts them as discarded. The pipeline assigns
     * that task count to {@code writeResult.nonInsertedVariants}, double-counting the silent
     * drops as "duplicated variants not inserted" and inflating the post-load consistency
     * mismatch. The invariant: {@code writeResult.nonInsertedVariants} must reflect only the
     * real duplicate discards reported by the resolver itself ({@code loadStats.discardedVariants}).
     */
    @Test
    public void testDuplicatedVariantPostLoadCountersConsistent() throws Exception {
        URI input = getResourceUri("variant-test-duplicated.vcf");
        URI outputUri = newOutputUri();
        StoragePipelineResult result = runETL(getVariantStorageEngine(), input, outputUri, new ObjectMap()
                        .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                        .append(VariantStorageOptions.GVCF.key(), "true")
                        .append(VariantStorageOptions.STATS_CALCULATE.key(), "false")
                        .append(VariantStorageOptions.ANNOTATE.key(), "false")
                        .append(VariantStorageOptions.DEDUPLICATION_POLICY.key(), "maxQual"),
                true, true, true);

        ObjectMap loadStats = result.getLoadStats();
        MongoDBVariantWriteResult writeResult = (MongoDBVariantWriteResult) loadStats.get("writeResult");
        int resolverDiscardedVariants = loadStats.getInt("discardedVariants");

        Assert.assertEquals(
                "writeResult.nonInsertedVariants must equal resolver.discardedVariants. "
                        + "Currently the pipeline assigns task.getDiscardedVariants() which "
                        + "double-counts silent ref-block drops from filterNewRefBlocks.",
                resolverDiscardedVariants, writeResult.getNonInsertedVariants());
    }
}
