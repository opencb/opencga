package org.opencb.opencga.storage.hadoop.metadata;

import org.junit.After;
import org.junit.ClassRule;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

import java.nio.file.Paths;

public class HadoopVariantStorageMetadataManagerTest extends VariantStorageMetadataManagerTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printMetaTable(getVariantStorageEngine().getDBAdaptor(), Paths.get(newOutputUri()));
    }
}
