/*
 * Copyright 2015-2016 OpenCB
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

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Creates StorageManagers by reflexion.
 * The StorageManager's className is read from <opencga-home>/conf/storage-configuration.yml
 */
public final class StorageManagerFactory {

    private static StorageManagerFactory storageManagerFactory;
    private static StorageConfiguration storageConfigurationDefault;
    private StorageConfiguration storageConfiguration;

    private Map<String, AlignmentStorageManager> alignmentStorageManagerMap = new HashMap<>();
    private Map<String, VariantStorageManager> variantStorageManagerMap = new HashMap<>();
    protected static Logger logger = LoggerFactory.getLogger(StorageConfiguration.class);

    private StorageManagerFactory(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public static void configure(StorageConfiguration configuration) {
        storageConfigurationDefault = configuration;
    }

    @Deprecated
    public static StorageManagerFactory get() {
//        if (storageConfigurationDefault == null) {
//            try {
//                storageConfigurationDefault = StorageConfiguration.load();
//            } catch (IOException e) {
//                logger.error("Unable to get StorageManagerFactory");
//                throw new UncheckedIOException(e);
//            }
//        }
        return get(null);
    }

    public static StorageManagerFactory get(StorageConfiguration storageConfiguration) {
        if (storageManagerFactory == null) {
            if (storageConfiguration != null) {
                configure(storageConfiguration);
            } else {
                storageConfiguration = storageConfigurationDefault;
            }
            Objects.requireNonNull(storageConfiguration, "Storage configuration needed");
            // TODO: Uncomment the line below once variantStorageManager starts needing to know catalog
//            Objects.requireNonNull(catalogManager, "Catalog manager needed");
            storageManagerFactory = new StorageManagerFactory(storageConfiguration);
            return storageManagerFactory;

        }
        return storageManagerFactory;
    }

    public AlignmentStorageManager getAlignmentStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getAlignmentStorageManager(null);
    }

    public AlignmentStorageManager getAlignmentStorageManager(String storageEngineName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getStorageManager("ALIGNMENT", storageEngineName, alignmentStorageManagerMap);
    }


    public VariantStorageManager getVariantStorageManager()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getVariantStorageManager(null);
    }

    public VariantStorageManager getVariantStorageManager(String storageEngineName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getStorageManager("VARIANT", storageEngineName, variantStorageManagerMap);
    }


    private <T extends StorageManager> T getStorageManager(String bioformat, String storageEngineName, Map<String, T> storageManagerMap)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        /*
         * This new block of code use new StorageConfiguration system, it must replace older one
         */
        if (this.storageConfiguration == null) {
            throw new NullPointerException();
        }
        if (StringUtils.isEmpty(storageEngineName)) {
            storageEngineName = getDefaultStorageManagerName();
        }
        if (!storageManagerMap.containsKey(storageEngineName)) {
            String clazz = null;
            switch (bioformat.toUpperCase()) {
                case "ALIGNMENT":
                    clazz = this.storageConfiguration.getStorageEngine(storageEngineName).getAlignment().getManager();
                    break;
                case "VARIANT":
                    clazz = this.storageConfiguration.getStorageEngine(storageEngineName).getVariant().getManager();
                    break;
                default:
                    break;
            }

            T storageManager = (T) Class.forName(clazz).newInstance();
            storageManager.setConfiguration(this.storageConfiguration, storageEngineName);

            storageManagerMap.put(storageEngineName, storageManager);
        }
        return storageManagerMap.get(storageEngineName);
    }

    public String getDefaultStorageManagerName() {
        return storageConfiguration.getDefaultStorageEngineId();
//        String[] storageEngineNames = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.ENGINES").split(",");
//        return storageEngineNames[0].toUpperCase();
    }

    public List<String> getDefaultStorageManagerNames() {
        return storageConfiguration.getStorageEngines().stream()
                .map(StorageEngineConfiguration::getId)
                .collect(Collectors.<String>toList());
//        return Config.getStorageProperties().getProperty("OPENCGA.STORAGE.ENGINES").split(",");
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    //    private static String parseStorageEngineName(String storageEngineName) {
//        String[] storageEngineNames = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.ENGINES").split(",");
//        if(storageEngineName == null || storageEngineName.isEmpty()) {
//            return storageEngineNames[0].toUpperCase();
//        } else {
//            storageEngineName = storageEngineName.toUpperCase();
//            for (String engineName : storageEngineNames) {
//                if(engineName.toUpperCase().equals(storageEngineName)) {
//                    return storageEngineName.toUpperCase();
//                }
//            }
//            return null;
//        }
//    }

    public void registerStorageManager(VariantStorageManager variantStorageManager) {
        variantStorageManagerMap.put(variantStorageManager.getStorageEngineId(), variantStorageManager);
    }

    public VariantStorageManager unregisterVariantStorageManager(String storageEngineId) {
        return variantStorageManagerMap.remove(storageEngineId);
    }

    public void registerStorageManager(AlignmentStorageManager alignmentStorageManager) {
        alignmentStorageManagerMap.put(alignmentStorageManager.getStorageEngineId(), alignmentStorageManager);
    }

    public AlignmentStorageManager unregisterAlignmentStorageManager(String storageEngineId) {
        return alignmentStorageManagerMap.remove(storageEngineId);
    }
}
