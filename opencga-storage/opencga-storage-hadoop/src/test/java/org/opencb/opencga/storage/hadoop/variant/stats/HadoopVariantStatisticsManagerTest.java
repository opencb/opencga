package org.opencb.opencga.storage.hadoop.variant.stats;

import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManagerTest;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageManagerTestUtils;

/**
 * Created on 12/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStatisticsManagerTest extends VariantStatisticsManagerTest implements HadoopVariantStorageManagerTestUtils {

    @Rule
    public ExternalResource externalResource = new HadoopExternalResource();

}
