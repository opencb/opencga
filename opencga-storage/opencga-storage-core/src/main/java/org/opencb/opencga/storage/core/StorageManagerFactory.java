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
 * The StorageManager's className is read from <opencga-home>/conf/storage.properties
 */
public class StorageManagerFactory {

    private static Map<String, AlignmentStorageManager> alignmentStorageManagerMap = new HashMap<>();
    private static Map<String, VariantStorageManager> variantStorageManagerMap = new HashMap<>();

    public static AlignmentStorageManager getAlignmentStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getAlignmentStorageManager(null);
    }

    public static AlignmentStorageManager getAlignmentStorageManager(String storageEngineName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        // Get a valid StorageEngine name
        if(storageEngineName == null || storageEngineName.isEmpty()) {
            storageEngineName = getDefaultStorageEngine();
            if(storageEngineName == null || storageEngineName.isEmpty()) {
                return null;
            }
        }
        storageEngineName = storageEngineName.toUpperCase();

        if(!alignmentStorageManagerMap.containsKey(storageEngineName)) {
            String key = "OPENCGA.STORAGE." + storageEngineName;
            Properties storageProperties = Config.getStorageProperties();
            String storageManagerClassName = storageProperties.getProperty(key + ".ALIGNMENT.MANAGER");
            String propertiesPath = storageProperties.getProperty(key + ".CONF");

            AlignmentStorageManager alignmentStorageManager = (AlignmentStorageManager) Class.forName(storageManagerClassName).newInstance();
            alignmentStorageManager.addPropertiesPath(Paths.get(Config.getGcsaHome(), "conf", propertiesPath));

            alignmentStorageManagerMap.put(storageEngineName, alignmentStorageManager);
        }
        return alignmentStorageManagerMap.get(storageEngineName);
    }


    public static VariantStorageManager getVariantStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getVariantStorageManager(null);
    }

    public static VariantStorageManager getVariantStorageManager(String storageEngineName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        // Get a valid StorageEngine name
        if(storageEngineName == null || storageEngineName.isEmpty()) {
            storageEngineName = getDefaultStorageEngine();
            if(storageEngineName == null || storageEngineName.isEmpty()) {
                return null;
            }
        }
        storageEngineName = storageEngineName.toUpperCase();

        // Check if this already has been created
        if(!variantStorageManagerMap.containsKey(storageEngineName)) {
            String key = "OPENCGA.STORAGE." + storageEngineName;
            Properties storageProperties = Config.getStorageProperties();
            String storageManagerClassName = storageProperties.getProperty(key + ".VARIANT.MANAGER");
            String propertiesPath = storageProperties.getProperty(key + ".CONF");

            // Specific VariantStorageManager is created by reflection using the Class name from the properties file.
            // The conf file is passed to the storage engine
            VariantStorageManager variantStorageManager = (VariantStorageManager) Class.forName(storageManagerClassName).newInstance();
            variantStorageManager.addPropertiesPath(Paths.get(Config.getGcsaHome(), "conf", propertiesPath));

            variantStorageManagerMap.put(storageEngineName, variantStorageManager);
        }
        return variantStorageManagerMap.get(storageEngineName);
    }


    private static String getDefaultStorageEngine() {
        String storageEngineNames = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.ENGINES");
        if(storageEngineNames != null && !storageEngineNames.isEmpty()) {
            return storageEngineNames.split(",")[0];
        }
        return null;
    }

}
