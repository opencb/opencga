package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.AnnotationPendingVariantsDescriptor;
import org.opencb.opencga.storage.hadoop.variant.pending.DiscoverPendingVariantsDriver;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsReader;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 25/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantAnnotationManagerTest extends VariantAnnotationManagerTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printVariants(((HadoopVariantStorageEngine) variantStorageEngine).getDBAdaptor(), newOutputUri(getTestName().getMethodName()));
    }

    @Test
    public void incrementalAnnotationTest() throws Exception {
        HadoopVariantStorageEngine engine = getVariantStorageEngine();
        for (int i = 0; i < 3; i++) {
            URI platinumFile = getPlatinumFile(i);

            runDefaultETL(platinumFile, engine, null, new ObjectMap(VariantStorageOptions.ANNOTATE.key(), false)
                    .append(VariantStorageOptions.STATS_CALCULATE.key(), false));

            // Update pending variants
            new TestMRExecutor().run(DiscoverPendingVariantsDriver.class,
                    DiscoverPendingVariantsDriver.buildArgs(engine.getDBAdaptor().getVariantTable(), AnnotationPendingVariantsDescriptor.class, new ObjectMap()),
                    new ObjectMap(), "Prepare variants to annotate");

            long pendingVariantsCount = new PendingVariantsReader(new Query(), new AnnotationPendingVariantsDescriptor(), engine.getDBAdaptor()).stream().count();
            System.out.println("pendingVariants = " + pendingVariantsCount);
            long expectedPendingVariantsCount = engine.count(new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false)).first();
            Assert.assertEquals(expectedPendingVariantsCount, pendingVariantsCount);
            Assert.assertEquals(expectedPendingVariantsCount, engine.annotate(new Query(), new ObjectMap()));


            List<Variant> pendingVariants = new PendingVariantsReader(new Query(), new AnnotationPendingVariantsDescriptor(), engine.getDBAdaptor())
                    .stream()
                    .collect(Collectors.toList());
            expectedPendingVariantsCount = engine.count(new Query(VariantQueryParam.ANNOTATION_EXISTS.key(), false)).first();
            Assert.assertEquals(0, expectedPendingVariantsCount);
            Assert.assertEquals(pendingVariants.toString(), 0, pendingVariants.size());
            Assert.assertNotEquals(0, engine.count(new Query()).first().longValue());
        }

        long variants = engine.count(new Query()).first();
        Assert.assertEquals(0L, engine.annotate(new Query(), new ObjectMap()));
        Assert.assertEquals(variants, engine.annotate(new Query(), new ObjectMap(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), true)));
    }
}
