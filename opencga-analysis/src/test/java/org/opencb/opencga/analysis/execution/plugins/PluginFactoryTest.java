package org.opencb.opencga.analysis.execution.plugins;

import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.execution.plugins.test.TestPlugin;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Created on 27/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PluginFactoryTest {

    @Test
    public void testGetPlugin() throws Exception {
        Class<? extends OpenCGAPlugin> testClass = PluginFactory.get().getPluginClass(TestPlugin.PLUGIN_ID);
        assertEquals(TestPlugin.class, testClass);

        OpenCGAPlugin testPlugin = PluginFactory.get().getPlugin(TestPlugin.PLUGIN_ID);
        assertEquals(TestPlugin.class, testPlugin.getClass());
        testPlugin.init(LoggerFactory.getLogger(OpenCGAPlugin.class), new ObjectMap(TestPlugin.PARAM_1, "Hello").append(TestPlugin.ERROR, false), null, null);
        int run = testPlugin.run();
        assertEquals(run, 0);
    }

}