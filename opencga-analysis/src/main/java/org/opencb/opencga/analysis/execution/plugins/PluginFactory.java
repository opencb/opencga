package org.opencb.opencga.analysis.execution.plugins;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
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

    public static PluginFactory get() {
        if (pluginFactory == null) {
            pluginFactory = new PluginFactory();
        }
        return pluginFactory;
    }

    public Class<? extends OpenCGAPlugin> getPluginClass(String id) {
        return pluginsIdMap.get(id);
    }

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
