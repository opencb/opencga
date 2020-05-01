package org.opencb.opencga.storage.core.variant;

import org.junit.Assert;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.FileUtils;

import java.io.BufferedReader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class VariantStorageEngineDuplicatedTest extends VariantStorageBaseTest {

    @Test
    public void testDuplicatedVariant() throws Exception {

        URI input = getResourceUri("variant-test-duplicated.vcf");
        URI outputUri = newOutputUri();
        runETL(getVariantStorageEngine(), input, outputUri, new ObjectMap()
                .append(VariantStorageOptions.STUDY.key(), STUDY_NAME)
                .append(VariantStorageOptions.GVCF.key(), "true")
                .append(VariantStorageOptions.STATS_CALCULATE.key(), "false")
                .append(VariantStorageOptions.ANNOTATE.key(), "false")
                .append(VariantStorageOptions.DEDUPLICATION_POLICY.key(), "maxQual"),
                true, true, true
        );

        Path dupFile = Paths.get(outputUri.resolve("variant-test-duplicated.vcf.duplicated.tsv").getPath());
        Assert.assertTrue(Files.exists(dupFile));
//        System.out.println("dupFile = " + dupFile.toAbsolutePath());
        try (BufferedReader reader = FileUtils.newBufferedReader(dupFile)) {
            String line;
            // Read header
            do {
                line = reader.readLine();
//                System.out.println(line);
            } while (line.startsWith("#"));

            while (line != null) {
//                System.out.println(line);
                Variant v = new Variant(line.split("\t")[0]);
                Assert.assertNotEquals(VariantType.NO_VARIATION, v.getType());
                line = reader.readLine();
            }

        }

    }
}
