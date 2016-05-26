package org.opencb.opencga.analysis.execution.plugins;

import org.junit.Test;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.beans.Analysis;
import org.opencb.opencga.analysis.execution.plugins.test.TestPlugin;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.Assert.*;

/**
 * Created on 27/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PluginFactoryTest {

    public static class TestPlugin2 extends TestPlugin {

        public static final String ID = "test_plugin2";

        @Override
        public String getIdentifier() {
            return ID;
        }

        @Override
        public Analysis getManifest() {
            try {
                return loadManifest(getIdentifier());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Test
    public void testGetPlugin() throws Exception {
        Class<? extends OpenCGAPlugin> testClass = PluginFactory.get().getPluginClass(TestPlugin.PLUGIN_ID);
        assertEquals(TestPlugin.class, testClass);

        OpenCGAPlugin testPlugin = PluginFactory.get().getPlugin(TestPlugin.PLUGIN_ID);
        assertEquals(TestPlugin.class, testPlugin.getClass());
        testPlugin.init(LoggerFactory.getLogger(OpenCGAPlugin.class), new ObjectMap(TestPlugin.PARAM_1, "Hello").append(TestPlugin.ERROR, false), null, null, -1, null);
        int run = testPlugin.run();
        assertEquals(run, 0);
    }

    @Test
    public void testGetPlugin2() throws Exception {
        Class<? extends OpenCGAPlugin> testClass = PluginFactory.get().getPluginClass(TestPlugin2.ID);
        assertEquals(TestPlugin2.class, testClass);

        OpenCGAPlugin testPlugin = PluginFactory.get().getPlugin(TestPlugin2.ID);
        assertEquals(TestPlugin2.class, testPlugin.getClass());
        testPlugin.init(LoggerFactory.getLogger(OpenCGAPlugin.class), new ObjectMap(TestPlugin.PARAM_1, "Hello World!").append(TestPlugin.ERROR, false), null, null, -1, null);
        int run = testPlugin.run();
        assertEquals(run, 0);
        assertEquals(TestPlugin2.ID, testPlugin.getManifest().getId());
    }

}