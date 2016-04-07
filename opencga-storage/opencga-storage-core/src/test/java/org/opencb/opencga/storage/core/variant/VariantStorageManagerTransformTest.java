package org.opencb.opencga.storage.core.variant;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageETLResult;

import java.util.Collections;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantStorageManagerTransformTest extends VariantStorageManagerTestUtils {

    @Test
    public void transformIsolated() throws Exception {

        ObjectMap params = new ObjectMap();

        VariantStorageManager variantStorageManager = getVariantStorageManager();
        variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getDatabase()
                .setHosts(Collections.singletonList("1.1.1.1"));
        StorageETLResult etlResult = runETL(variantStorageManager, smallInputUri, outputUri, params, true, true, false);
        System.out.println("etlResult = " + etlResult);

    }

}
