package org.opencb.opencga.storage.hadoop.variant;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.storage.core.variant.VariantStorageManagerTransformTest;

/**
 * Created on 01/04/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageManagerTransformTest extends VariantStorageManagerTransformTest implements HadoopVariantStorageManagerTestUtils {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

}
