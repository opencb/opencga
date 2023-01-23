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

import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.alignment.local.LocalAlignmentStorageEngine;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Creates StorageEngines by reflexion.
 * The StorageEngine's className is read from <opencga-home>/conf/storage-configuration.yml
 */
public final class StorageEngineFactory {

    private static StorageEngineFactory storageEngineFactory;
    private static StorageConfiguration storageConfigurationDefault;
    private StorageConfiguration storageConfiguration;

    private Map<String, AlignmentStorageEngine> alignmentStorageEngineMap = new ConcurrentHashMap<>();
    private Map<String, VariantStorageEngine> variantStorageEngineMap = new ConcurrentHashMap<>();
    protected static Logger logger = LoggerFactory.getLogger(StorageConfiguration.class);

    private StorageEngineFactory(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public static void configure(StorageConfiguration configuration) {
        storageConfigurationDefault = configuration;
        if (storageEngineFactory != null) {
            storageEngineFactory.storageConfiguration = configuration;
        }
    }

    private enum Type {
        ALIGNMENT,
        VARIANT
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
            storageEngineFactory = new StorageEngineFactory(storageConfiguration);
            return storageEngineFactory;

        }
        return storageEngineFactory;
    }

    public AlignmentStorageEngine getAlignmentStorageEngine(String dbName)
            throws StorageEngineException {
        return getStorageEngine(Type.ALIGNMENT, null, AlignmentStorageEngine.class, alignmentStorageEngineMap, dbName, null);
    }

    public VariantStorageEngine getVariantStorageEngine() throws StorageEngineException {
        return getVariantStorageEngine(null, "");
    }

    public VariantStorageEngine getVariantStorageEngine(String storageEngineName, String dbName)
            throws StorageEngineException {
        return getVariantStorageEngine(storageEngineName, dbName, null);
    }

    public VariantStorageEngine getVariantStorageEngine(String storageEngineName, String dbName, String engineAlias)
            throws StorageEngineException {
        return getStorageEngine(Type.VARIANT, storageEngineName, VariantStorageEngine.class, variantStorageEngineMap, dbName, engineAlias);
    }

    private synchronized <T extends StorageEngine> T getStorageEngine(Type type, String storageEngineId, Class<T> superClass,
                                                                      Map<String, T> storageEnginesMap, String dbName, String engineAlias)
            throws StorageEngineException {
        /*
         * This new block of code use new StorageConfiguration system, it must replace older one
         */
        if (this.storageConfiguration == null) {
            throw new NullPointerException();
        }
        if (StringUtils.isEmpty(storageEngineId)) {
            storageEngineId = getDefaultStorageEngineId();
        }
        if (dbName == null) {
            dbName = "";
        }
        String key = buildStorageEngineKey(storageEngineId, dbName, engineAlias);
        if (!storageEnginesMap.containsKey(key)) {
            String clazz;
            switch (type) {
                case ALIGNMENT:
                    clazz = LocalAlignmentStorageEngine.class.getName();
                    break;
                case VARIANT:
                    clazz = this.storageConfiguration.getVariantEngine(storageEngineId).getEngine();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown type " + type);
            }

            try {
                T storageEngine = Class.forName(clazz).asSubclass(superClass).newInstance();
                StorageConfiguration storageConfiguration = JacksonUtils.getDefaultObjectMapper()
                        .updateValue(new StorageConfiguration(), this.storageConfiguration);
                storageEngine.setConfiguration(storageConfiguration, storageEngineId, dbName);

                storageEnginesMap.put(key, storageEngine);
                return storageEngine;
            } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | JsonMappingException e) {
                throw new StorageEngineException("Error instantiating StorageEngine '" + clazz + "'", e);
            }
        } else {
            return storageEnginesMap.get(key);
        }
    }

    private String buildStorageEngineKey(String storageEngineName, String dbName, String alias) {
        return storageEngineName + '_' + dbName + (StringUtils.isEmpty(alias) ? "" : ('_' + alias));
    }

    public String getDefaultStorageEngineId() {
        return storageConfiguration.getVariant().getDefaultEngine();
    }

    public StorageConfiguration getStorageConfiguration() {
        return storageConfiguration;
    }

    public void registerVariantStorageEngine(VariantStorageEngine variantStorageEngine) {
        registerVariantStorageEngine(variantStorageEngine, variantStorageEngine.dbName, null);
    }

    public void registerVariantStorageEngine(VariantStorageEngine variantStorageEngine, String dbName, String alias) {
        String key = buildStorageEngineKey(variantStorageEngine.getStorageEngineId(), dbName, alias);
        variantStorageEngineMap.put(key, variantStorageEngine);
    }

    public void unregisterVariantStorageEngine(String storageEngineId) {
        Map<String, VariantStorageEngine> map = this.variantStorageEngineMap;
        unregister(storageEngineId, map);
    }

    public void registerAlignmentStorageEngine(AlignmentStorageEngine alignmentStorageEngine) {
        String key = buildStorageEngineKey(alignmentStorageEngine.getStorageEngineId(), alignmentStorageEngine.dbName, null);
        alignmentStorageEngineMap.put(key, alignmentStorageEngine);
    }

    public void unregisterAlignmentStorageManager(String storageEngineId) {
        unregister(storageEngineId, alignmentStorageEngineMap);
    }

    private <T extends StorageEngine> void unregister(String storageEngineId, Map<String, T> map) {
        for (Iterator<Map.Entry<String, T>> iterator = map.entrySet().iterator(); iterator.hasNext();) {
            Map.Entry<String, T> entry = iterator.next();
            if (entry.getKey().startsWith(storageEngineId + '_')) {
                iterator.remove();
            }
        }
    }

    public synchronized void close() throws IOException {
        List<IOException> ioExceptions = new ArrayList<>();
        for (VariantStorageEngine value : variantStorageEngineMap.values()) {
            try {
                value.close();
            } catch (IOException e) {
                logger.error("Error closing variant storage engine on db '" + value.getDBName() + "'");
                ioExceptions.add(e);
            }
        }
        variantStorageEngineMap.clear();
        if (!ioExceptions.isEmpty()) {
            if (ioExceptions.size() == 1) {
                throw ioExceptions.get(0);
            } else {
                IOException e = new IOException("Error closing VariantStorageEngines");
                for (IOException ioException : ioExceptions) {
                    e.addSuppressed(ioException);
                }
                throw e;
            }
        }
    }
}
