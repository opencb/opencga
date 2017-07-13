package org.opencb.opencga.storage.hadoop.variant;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.opencb.opencga.storage.core.variant.VariantStorageSearchIntersectTest;

/**
 * Created on 07/07/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantStorageSearchIntersectTest extends VariantStorageSearchIntersectTest implements HadoopVariantStorageTest {

    @ClassRule
    public static ExternalResource externalResource = new HadoopExternalResource();

    @Override
    public void before() throws Exception {
        super.before();
        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
        stderr.setThreshold(Level.DEBUG);
    }

}
