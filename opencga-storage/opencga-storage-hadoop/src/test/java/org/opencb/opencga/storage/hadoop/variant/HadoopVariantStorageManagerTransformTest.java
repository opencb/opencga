package org.opencb.opencga.storage.hadoop.variant;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StorageETLResult;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTransformTest;

import java.net.URI;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.core.variant.io.VariantReaderUtils.MALFORMED_FILE;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageManagerTransformTest extends VariantStorageManagerTransformTest implements HadoopVariantStorageManagerTestUtils {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();


    @Test
    public void protoTransformTest() throws Exception {

        ObjectMap params = new ObjectMap(VariantStorageManager.Options.TRANSFORM_FORMAT.key(), "proto")
                .append("transform.proto.parallel", true);
        URI outputUri = newOutputUri();

        VariantStorageManager variantStorageManager = getVariantStorageManager();
        StorageETLResult etlResult = runETL(variantStorageManager, smallInputUri, outputUri, params, true, true, false);
        System.out.println("etlResult = " + etlResult);


        String[] malformedFiles = Paths.get(outputUri).toFile().list((dir, name) -> name.contains(MALFORMED_FILE));
        assertEquals(0, malformedFiles.length);
    }

}
