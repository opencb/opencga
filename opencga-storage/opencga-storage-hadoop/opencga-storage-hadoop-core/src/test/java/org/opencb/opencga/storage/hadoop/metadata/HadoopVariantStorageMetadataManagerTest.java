package org.opencb.opencga.storage.hadoop.metadata;

import com.google.common.collect.BiMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManagerTest;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;

import java.nio.file.Paths;

public class HadoopVariantStorageMetadataManagerTest extends VariantStorageMetadataManagerTest implements HadoopVariantStorageTest {

    @ClassRule
    public static HadoopExternalResource externalResource = new HadoopExternalResource();

    @After
    public void tearDown() throws Exception {
        VariantHbaseTestUtils.printMetaTable(getVariantStorageEngine().getDBAdaptor(), Paths.get(newOutputUri()));
    }

    @Test
    public void testNoTableCreation() throws Exception {
        HadoopVariantStorageEngine engine = getVariantStorageEngine();
        String table = "my_table_meta";
        HBaseManager hBaseManager = engine.getDBAdaptor().getHBaseManager();
        VariantStorageMetadataManager manager = new VariantStorageMetadataManager(new HBaseVariantStorageMetadataDBAdaptorFactory(hBaseManager, table, engine.getConf()));

        Assert.assertFalse(hBaseManager.tableExists(table));
        ProjectMetadata projectMetadata = manager.getProjectMetadata();
        Assert.assertNull(projectMetadata);
        Assert.assertFalse(hBaseManager.tableExists(table));
        BiMap<String, Integer> studies = manager.getStudies();
        Assert.assertEquals(0, studies.size());
        Assert.assertFalse(hBaseManager.tableExists(table));

        manager.updateProjectMetadata(p -> {
            return new ProjectMetadata().setSpecies("More human than human");
        });

        Assert.assertTrue(hBaseManager.tableExists(table));
    }
}
