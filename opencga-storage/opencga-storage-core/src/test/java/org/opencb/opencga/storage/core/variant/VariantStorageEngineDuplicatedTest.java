package org.opencb.opencga.storage.core.variant;

import org.junit.Assert;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

public abstract class VariantStorageEngineDuplicatedTest extends VariantStorageBaseTest {

    @Test
    public void testDuplicatedVariant() throws Exception {

        URI input = getResourceUri("variant-test-duplicated.vcf");
        URI outputUri = newOutputUri();
        runETL(getVariantStorageEngine(), input, outputUri, new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.STATS_CALCULATE.key(), "false")
                .append(VariantStorageOptions.ANNOTATE.key(), "false")
                .append(VariantStorageOptions.DEDUPLICATION_POLICY.key(), "maxQual"),
                true, true, true
        );

        Assert.assertTrue(Files.exists(Paths.get(outputUri.resolve("variant-test-duplicated.vcf.duplicated.tsv").getPath())));

    }
}
