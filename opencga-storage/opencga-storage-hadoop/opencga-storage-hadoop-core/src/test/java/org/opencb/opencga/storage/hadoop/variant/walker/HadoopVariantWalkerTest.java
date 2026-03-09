package org.opencb.opencga.storage.hadoop.variant.walker;


import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.LongTests;
import org.opencb.opencga.storage.core.variant.walker.VariantWalkerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.mr.StreamVariantMapper;

@Category(LongTests.class)
public class HadoopVariantWalkerTest extends VariantWalkerTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @Before
    public void before() throws Exception {
        HadoopVariantStorageEngine variantStorageManager = externalResource.getVariantStorageEngine();
        boolean preLoaded = loaded;
        super.before();
        if (loaded != preLoaded) {
            VariantHbaseTestUtils.printVariants(variantStorageManager.getDBAdaptor(), newOutputUri());
        }
        variantStorageEngine.getOptions().put(StreamVariantMapper.DOCKER_PRUNE_OPTS, " --filter label!=opencga_scope='test'");
    }

}
