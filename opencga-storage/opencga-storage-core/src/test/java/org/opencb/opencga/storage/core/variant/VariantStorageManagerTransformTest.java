package org.opencb.opencga.storage.core.variant;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;

import java.util.Collections;

import static org.junit.Assert.*;

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

    /**
     * Corrupted file index. This test must fail
     */
    @Test
    public void corruptedTransformNoFailTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), true);

        thrown.expect(StorageETLException.class);
        try {
            runDefaultETL(corruptedInputUri, getVariantStorageManager(), newStudyConfiguration(), params, true, false);
        } catch (StorageETLException e) {
            assertEquals(1, e.getResults().size());

            System.out.println(e.getResults().get(0));
            assertTrue(e.getResults().get(0).isTransformExecuted());
            assertNotNull(e.getResults().get(0).getTransformError());
            assertTrue(e.getResults().get(0).getTransformTimeMillis() > 0);
            assertFalse(e.getResults().get(0).isLoadExecuted());
            throw e;
        } catch (Exception e) {
            System.out.println("e.getClass().getName() = " + e.getClass().getName());
            throw e;
        }

    }

    @Test
    public void corruptedTransformTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), false);
        runDefaultETL(corruptedInputUri, getVariantStorageManager(), newStudyConfiguration(), params, true, false);

        // TODO: Check error report file
    }


}
