package org.opencb.opencga.storage.core.variant;

import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.exceptions.StorageETLException;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.opencb.opencga.storage.core.variant.io.VariantReaderUtils.MALFORMED_FILE;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public abstract class VariantStorageManagerTransformTest extends VariantStorageManagerTestUtils {

    @Test
    public void transformIsolated() throws Exception {

        ObjectMap params = new ObjectMap();
        URI outputUri = newOutputUri();

        VariantStorageManager variantStorageManager = getVariantStorageManager();
        variantStorageManager.getConfiguration().getStorageEngine(variantStorageManager.getStorageEngineId()).getVariant().getDatabase()
                .setHosts(Collections.singletonList("1.1.1.1"));
        StorageETLResult etlResult = runETL(variantStorageManager, smallInputUri, outputUri, params, true, true, false);
        System.out.println("etlResult = " + etlResult);


        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(0, malformedFiles.length);

    }

    /**
     * Corrupted file index. This test must fail
     */
    @Test
    public void corruptedTransformNoFailTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), true);

        URI outputUri = newOutputUri();

        thrown.expect(StorageETLException.class);
        try {
            runETL(getVariantStorageManager(), corruptedInputUri, outputUri, params, true, true, false);
        } catch (StorageETLException e) {
            assertEquals(1, e.getResults().size());

            System.out.println(e.getResults().get(0));
            assertTrue(e.getResults().get(0).isTransformExecuted());
            assertNotNull(e.getResults().get(0).getTransformError());
            assertTrue(e.getResults().get(0).getTransformTimeMillis() > 0);
            assertFalse(e.getResults().get(0).isLoadExecuted());

            String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
            assertEquals(1, malformedFiles.length);
            throw e;
        } catch (Exception e) {
            System.out.println("e.getClass().getName() = " + e.getClass().getName());
            throw e;
        }

    }

    @Test
    public void corruptedTransformTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FAIL_ON_MALFORMED_VARIANT.key(), false);
        URI outputUri = newOutputUri();
        StorageETLResult result = runETL(getVariantStorageManager(), corruptedInputUri, outputUri, params, true, true, false);

        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(1, malformedFiles.length);
        assertEquals(2, result.getTransformStats().getInt("malformed lines"));
    }


}
