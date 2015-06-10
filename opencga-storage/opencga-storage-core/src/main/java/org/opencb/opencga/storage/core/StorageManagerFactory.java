/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core;

import org.opencb.opencga.core.common.Config;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Creates StorageManagers by reflexion.
 * The StorageManager's className is read from <opencga-home>/conf/storage.properties
 */
public class StorageManagerFactory {

    private static StorageConfiguration storageConfiguration;

    private static Map<String, AlignmentStorageManager> alignmentStorageManagerMap = new HashMap<>();
    private static Map<String, VariantStorageManager> variantStorageManagerMap = new HashMap<>();

    public StorageManagerFactory(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public static AlignmentStorageManager getAlignmentStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getAlignmentStorageManager(null);
    }

    public static AlignmentStorageManager getAlignmentStorageManager(String storageEngineName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getStorageManager("ALIGNMENT", storageEngineName, alignmentStorageManagerMap);
    }


    public static VariantStorageManager getVariantStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getVariantStorageManager(null);
    }

    public static VariantStorageManager getVariantStorageManager(String storageEngineName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getStorageManager("VARIANT", storageEngineName, variantStorageManagerMap);
    }


    private static <T extends StorageManager> T getStorageManager(String bioformat, String storageEngineName, Map<String, T> storageManagerMap)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        /*
         * This new block of code use new StorageConfiguration system, it must replace older one
         */
        if(storageConfiguration != null) {
            if(!storageManagerMap.containsKey(storageEngineName)) {
                String clazz = null;
                switch (bioformat.toUpperCase()) {
                    case "ALIGNMENT":
                        clazz = storageConfiguration.getStorageEngine(storageEngineName).getAlignment().getManager();
                        break;
                    case "VARIANT":
                        clazz = storageConfiguration.getStorageEngine(storageEngineName).getVariant().getManager();
                        break;
                }

                T storageManager = (T) Class.forName(clazz).newInstance();
                storageManager.setConfiguration(storageConfiguration, storageEngineName);

                storageManagerMap.put(storageEngineName, storageManager);
            }
            return storageManagerMap.get(storageEngineName);
        }

        // OLD CODE!!!

        // Get a valid StorageEngine name
        storageEngineName = parseStorageEngineName(storageEngineName);
        if(storageEngineName == null) {
            return null;
        }


        // Check if this already has been created
        if(!storageManagerMap.containsKey(storageEngineName)) {
            String key = "OPENCGA.STORAGE." + storageEngineName;
            Properties storageProperties = Config.getStorageProperties();
            String storageManagerClassName = storageProperties.getProperty(key + "." + bioformat + ".MANAGER");
            String propertiesPath = storageProperties.getProperty(key + ".CONF");

            // Specific VariantStorageManager is created by reflection using the Class name from the properties file.
            // The conf file is passed to the storage engine
            T storageManager = (T) Class.forName(storageManagerClassName).newInstance();
            storageManager.addConfigUri(URI.create(Config.getOpenCGAHome() + "/").resolve("conf/").resolve(propertiesPath));

            storageManagerMap.put(storageEngineName, storageManager);
        }
        return storageManagerMap.get(storageEngineName);
    }

    public static String getDefaultStorageManagerName() {
        String[] storageEngineNames = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.ENGINES").split(",");
        return storageEngineNames[0].toUpperCase();
    }

    public static String[] getDefaultStorageManagerNames() {
        return Config.getStorageProperties().getProperty("OPENCGA.STORAGE.ENGINES").split(",");
    }

    private static String parseStorageEngineName(String storageEngineName) {
        String[] storageEngineNames = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.ENGINES").split(",");
        if(storageEngineName == null || storageEngineName.isEmpty()) {
            return storageEngineNames[0].toUpperCase();
        } else {
            storageEngineName = storageEngineName.toUpperCase();
            for (String engineName : storageEngineNames) {
                if(engineName.toUpperCase().equals(storageEngineName)) {
                    return storageEngineName.toUpperCase();
                }
            }
            return null;
        }
    }

}
