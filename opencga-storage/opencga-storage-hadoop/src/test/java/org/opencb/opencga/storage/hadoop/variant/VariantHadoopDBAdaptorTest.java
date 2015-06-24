package org.opencb.opencga.storage.hadoop.variant;

import org.junit.Test;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.IOException;

/**
 * Created by mh719 on 16/06/15.
 */
public class VariantHadoopDBAdaptorTest {

    @Test
    public void myTest() throws IOException {
        StorageConfiguration storageConfiguration = StorageConfiguration.load(this.getClass().getResourceAsStream("storage-configuration.yml"));

        HadoopVariantStorageManager sm = new HadoopVariantStorageManager();
//        VariantHadoopDBAdaptor vdb = new VariantHadoopDBAdaptor(sm.buildCredentials("test-table"));


    }

}
