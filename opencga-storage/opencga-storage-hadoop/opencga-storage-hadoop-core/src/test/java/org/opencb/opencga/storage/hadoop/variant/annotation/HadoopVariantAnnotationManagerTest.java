package org.opencb.opencga.storage.hadoop.variant.annotation;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageTest;
import org.opencb.opencga.storage.hadoop.variant.VariantHbaseTestUtils;

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
}
