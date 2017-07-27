/*
 * Copyright 2015-2017 OpenCB
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
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Creates StorageManagers by reflexion.
 * The StorageEngine's className is read from <opencga-home>/conf/storage-configuration.yml
 */
public final class StorageEngineFactory {

    private static StorageEngineFactory storageEngineFactory;
    private static StorageConfiguration storageConfigurationDefault;
    private StorageConfiguration storageConfiguration;

    private Map<String, AlignmentStorageEngine> alignmentStorageManagerMap = new HashMap<>();
    private Map<String, VariantStorageEngine> variantStorageManagerMap = new HashMap<>();
    protected static Logger logger = LoggerFactory.getLogger(StorageConfiguration.class);

    private StorageEngineFactory(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public static void configure(StorageConfiguration configuration) {
        storageConfigurationDefault = configuration;
    }

    private enum Type {
        VARIANT, ALIGNMENT
    }

    @Deprecated
    public static StorageEngineFactory get() {
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

    public static StorageEngineFactory get(StorageConfiguration storageConfiguration) {
        if (storageEngineFactory == null) {
            if (storageConfiguration != null) {
                configure(storageConfiguration);
            } else {
                storageConfiguration = storageConfigurationDefault;
            }
            Objects.requireNonNull(storageConfiguration, "Storage configuration needed");
            // TODO: Uncomment the line below once variantStorageManager starts needing to know catalog
//            Objects.requireNonNull(catalogManager, "Catalog manager needed");
            storageEngineFactory = new StorageEngineFactory(storageConfiguration);
            return storageEngineFactory;

        }
        return storageEngineFactory;
    }

    public AlignmentStorageEngine getAlignmentStorageEngine()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getAlignmentStorageEngine(null);
    }

    public AlignmentStorageEngine getAlignmentStorageEngine(String storageEngineName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getAlignmentStorageEngine(storageEngineName, "");
    }

    public AlignmentStorageEngine getAlignmentStorageEngine(String storageEngineName, String dbName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getStorageManager(Type.ALIGNMENT, storageEngineName, alignmentStorageManagerMap, dbName);
    }

    public VariantStorageEngine getVariantStorageEngine()
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        return getVariantStorageEngine(null, "");
    }

    public VariantStorageEngine getVariantStorageEngine(String storageEngineName, String dbName)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getStorageManager(Type.VARIANT, storageEngineName, variantStorageManagerMap, dbName);
    }

    private <T extends StorageEngine> T getStorageManager(Type type, String storageEngineName, Map<String, T> storageManagerMap,
                                                          String dbName)
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
        if (dbName == null) {
            dbName = "";
        }
        String key = buildStorageEngineKey(storageEngineName, dbName);
        if (!storageManagerMap.containsKey(key)) {
            String clazz;
            switch (type) {
                case ALIGNMENT:
                    clazz = this.storageConfiguration.getStorageEngine(storageEngineName).getAlignment().getManager();
                    break;
                case VARIANT:
                    clazz = this.storageConfiguration.getStorageEngine(storageEngineName).getVariant().getManager();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }

            T storageEngine = (T) Class.forName(clazz).newInstance();
            storageEngine.setConfiguration(this.storageConfiguration, storageEngineName, dbName);

            storageManagerMap.put(key, storageEngine);
            return storageEngine;
        } else {
            return storageManagerMap.get(key);
        }
    }

    private String buildStorageEngineKey(String storageEngineName, String dbName) {
        return storageEngineName + '_' + dbName;
    }

    public String getDefaultStorageManagerName() {
        return storageConfiguration.getDefaultStorageEngineId();
    }

    public List<String> getDefaultStorageManagerNames() {
        return storageConfiguration.getStorageEngines().stream()
                .map(StorageEngineConfiguration::getId)
                .collect(Collectors.<String>toList());
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public void registerStorageManager(VariantStorageEngine variantStorageEngine) {
        String key = buildStorageEngineKey(variantStorageEngine.getStorageEngineId(), variantStorageEngine.dbName);
        variantStorageManagerMap.put(key, variantStorageEngine);
    }

    public void unregisterVariantStorageManager(String storageEngineId) {
        Map<String, VariantStorageEngine> map = this.variantStorageManagerMap;
        unregister(storageEngineId, map);
    }

    public void registerStorageManager(AlignmentStorageEngine alignmentStorageEngine) {
        String key = buildStorageEngineKey(alignmentStorageEngine.getStorageEngineId(), alignmentStorageEngine.dbName);
        alignmentStorageManagerMap.put(key, alignmentStorageEngine);
    }

    public void unregisterAlignmentStorageManager(String storageEngineId) {
        unregister(storageEngineId, alignmentStorageManagerMap);
    }

    private <T extends StorageEngine> void unregister(String storageEngineId, Map<String, T> map) {
        for (Iterator<Map.Entry<String, T>> iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, T> entry = iterator.next();
            if (entry.getKey().startsWith(storageEngineId + '_')) {
                iterator.remove();
            }
        }
    }
}
