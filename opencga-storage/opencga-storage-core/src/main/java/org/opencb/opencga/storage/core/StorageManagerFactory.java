package org.opencb.opencga.storage.core;


import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Creates StorageManagers by reflexion.
 *
 * The StorageManager's className is read from <opencga-home>/conf/storage.properties
 *
 *
 */
public class StorageManagerFactory {

    private static Map<String, AlignmentStorageManager> alignmentStorageManagerMap = new HashMap<>();
    private static Map<String, VariantStorageManager> variantStorageManagerMap = new HashMap<>();

    public static AlignmentStorageManager getAlignmentStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getAlignmentStorageManager(null);
    }

    public static AlignmentStorageManager getAlignmentStorageManager(String name)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if(name == null || name.isEmpty()) {
            name = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.DEFAULT.ALIGNMENT");
        }
        name = name.toUpperCase();
        if(!alignmentStorageManagerMap.containsKey(name)) {
            String key = "OPENCGA.STORAGE." + name + ".ALIGNMENT";
            Properties storageProperties = Config.getStorageProperties();
            String storageManagerName = storageProperties.getProperty(key);
            String propertiesPath = storageProperties.getProperty(key + ".PROPERTIES");

            AlignmentStorageManager alignmentStorageManager = (AlignmentStorageManager) Class.forName(storageManagerName).newInstance();
//            alignmentStorageManager.addPropertiesPath(Paths.get(Config.getGcsaHome(), "conf", "storage.properties"));
            alignmentStorageManager.addPropertiesPath(Paths.get(Config.getGcsaHome(), "conf", propertiesPath));
            alignmentStorageManagerMap.put(name, alignmentStorageManager);
        }
        return alignmentStorageManagerMap.get(name);
    }

    public static VariantStorageManager getVariantStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getVariantStorageManager(null);
    }

    public static VariantStorageManager getVariantStorageManager(String name)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if(name == null || name.isEmpty()) {
            name = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.DEFAULT.ALIGNMENT");
        }
        name = name.toUpperCase();
        if(!variantStorageManagerMap.containsKey(name)) {
            String key = "OPENCGA.STORAGE." + name + ".VARIANT";
            Properties storageProperties = Config.getStorageProperties();
            String storageManagerName = storageProperties.getProperty(key);
            String propertiesPath = storageProperties.getProperty(key + ".PROPERTIES");

            VariantStorageManager variantStorageManager = (VariantStorageManager) Class.forName(storageManagerName).newInstance();
//            variantStorageManager.addPropertiesPath(Paths.get(Config.getGcsaHome(), "conf", "storage.properties"));
            variantStorageManager.addPropertiesPath(Paths.get(Config.getGcsaHome(), "conf", propertiesPath));
            variantStorageManagerMap.put(name, variantStorageManager);
        }
        return variantStorageManagerMap.get(name);
    }
}
