package org.opencb.opencga.analysis.execution.plugins;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class scans all the classpath using the library {@link Reflections} and find all the
 * implementations of the plugin interface {@link OpenCGAPlugin}.
 *
 * Implements singleton pattern. Use {@link #get()} to obtain instance
 *
 * Created on 27/11/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PluginFactory {
    private Logger logger = LoggerFactory.getLogger(PluginFactory.class);

    private static PluginFactory pluginFactory;
    private final Reflections reflections;
    private final Map<String, Class<? extends OpenCGAPlugin>> pluginsIdMap = new HashMap<>();

    private PluginFactory() {
//        Reflections.log = null; // Uncomment to skip logs
        reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(new SubTypesScanner()).addUrls(ClasspathHelper.forJavaClassPath())
                .filterInputsBy(input -> input.endsWith(".class"))
        );
        init();
    }

    /**
     * Initialize the pluginsIdMap. Find all the subtypes of {@link OpenCGAPlugin}
     */
    private void init() {
        Set<Class<? extends OpenCGAPlugin>> plugins = reflections.getSubTypesOf(OpenCGAPlugin.class);
        List<String> duplicatedPlugins = new LinkedList<>();
        for (Class<? extends OpenCGAPlugin> pluginClazz : plugins) {
            try {
                OpenCGAPlugin plugin = pluginClazz.newInstance();
                String pluginId = plugin.getIdentifier();
                if (pluginsIdMap.containsKey(pluginId)) {
                    logger.error("Duplicated ID for class {} and {}", pluginClazz, pluginsIdMap.get(pluginId));
                    duplicatedPlugins.add(pluginId);
                    continue;
                }
                pluginsIdMap.put(pluginId, pluginClazz);
            } catch (InstantiationException | IllegalAccessException | RuntimeException e) {
                logger.error("Unable to load class {} ", pluginClazz);
            }
        }
        duplicatedPlugins.forEach(pluginsIdMap::remove);
    }

    /**
     * Singleton accessor method
     *
     * @return Get the singleton instance of {@link PluginFactory}
     */
    public static PluginFactory get() {
        if (pluginFactory == null) {
            pluginFactory = new PluginFactory();
        }
        return pluginFactory;
    }

    /**
     * Get all the found plugin classes
     *
     * @return  Map between plugin id and plugin class
     */
    public Map<String, Class<? extends OpenCGAPlugin>> getAllPlugins() {
        return Collections.unmodifiableMap(pluginsIdMap);
    }

    /**
     * Get the class of a plugin given its id
     *
     * @param id    Plugin id
     * @return      Plugin class
     */
    public Class<? extends OpenCGAPlugin> getPluginClass(String id) {
        return pluginsIdMap.get(id);
    }

    /**
     * Get a new instance of a plugin given its id.
     *
     * @param id    Plugin id
     * @return      New instance of the plugin
     */
    public OpenCGAPlugin getPlugin(String id) {
        try {
            Class<? extends OpenCGAPlugin> pluginClass = getPluginClass(id);
            if (pluginClass == null) {
                return null;
            } else {
                return pluginClass.newInstance();
            }
        } catch (InstantiationException | IllegalAccessException e) {
            //This should never happen. All the plugins in the Map can be instantiated
            throw new IllegalStateException("Error creating new instance");
        }
    }

}
