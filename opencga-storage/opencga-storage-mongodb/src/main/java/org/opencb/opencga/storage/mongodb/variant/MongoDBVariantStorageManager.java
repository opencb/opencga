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

package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.FileStudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.mongodb.utils.MongoCredentials;

import java.net.UnknownHostException;
import java.util.Collections;

import static org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager.MongoDBVariantOptions.*;

/**
 * Created by imedina on 13/08/14.
 */
public class MongoDBVariantStorageManager extends VariantStorageManager {

    /*
     * This field defaultValue must be the same that the one at storage-configuration.yml
     */
    public static final String STORAGE_ENGINE_ID = "mongodb";

    public enum MongoDBVariantOptions {
        COLLECTION_VARIANTS("collection.variants", "variants"),
        COLLECTION_FILES("collection.files", "file"),
        COLLECTION_STUDIES("collection.studies",  "studies"),
        COLLECTION_STAGE("collection.stage",  "stage"),
        BULK_SIZE("bulkSize",  100),
        DEFAULT_GENOTYPE("defaultGenotype", Collections.singleton("0/0")),
        ALREADY_LOADED_VARIANTS("alreadyLoadedVariants", 0);

        private final String key;
        private final Object value;

        MongoDBVariantOptions(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        @SuppressWarnings("unchecked")
        public <T> T defaultValue() {
            return (T) value;
        }
    }


    @Override
    public void testConnection() throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();
        String dbName = options.getString(VariantStorageManager.Options.DB_NAME.key());
        MongoCredentials credentials = getMongoCredentials(dbName);

        if (!credentials.check()) {
            logger.error("Connection to database '{}' failed", dbName);
            throw new StorageManagerException("Database connection test failed");
        }
    }

    @Override
    public MongoDBVariantStorageETL newStorageETL(boolean connected) throws StorageManagerException {
        VariantMongoDBAdaptor dbAdaptor = connected ? getDBAdaptor(null) : null;
        return new MongoDBVariantStorageETL(configuration, STORAGE_ENGINE_ID, dbAdaptor);
    }

    @Override
    public void dropFile(String study, int fileId) throws StorageManagerException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        getDBAdaptor().deleteFile(study, Integer.toString(fileId), new QueryOptions(options));
    }

    @Override
    public void dropStudy(String studyName) throws StorageManagerException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        getDBAdaptor().deleteStudy(studyName, new QueryOptions(options));
    }

    public VariantMongoDBAdaptor getDBAdaptor() throws StorageManagerException {
        return getDBAdaptor(null);
    }

    @Override
    public VariantMongoDBAdaptor getDBAdaptor(String dbName) throws StorageManagerException {
        MongoCredentials credentials = getMongoCredentials(dbName);
        VariantMongoDBAdaptor variantMongoDBAdaptor;
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        if (dbName != null && !dbName.isEmpty()) {
            options.append(VariantStorageManager.Options.DB_NAME.key(), dbName);
        }

        String variantsCollection = options.getString(COLLECTION_VARIANTS.key(), COLLECTION_VARIANTS.defaultValue());
        String filesCollection = options.getString(COLLECTION_FILES.key(), COLLECTION_FILES.defaultValue());
        try {
            StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager(options);
            variantMongoDBAdaptor = new VariantMongoDBAdaptor(credentials, variantsCollection, filesCollection,
                    studyConfigurationManager, configuration.getStorageEngine(STORAGE_ENGINE_ID));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }

        logger.debug("getting DBAdaptor to db: {}", credentials.getMongoDbName());
        return variantMongoDBAdaptor;
    }

    MongoCredentials getMongoCredentials(String dbName) {
        ObjectMap options = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions();

        // If no database name is provided, read from the configuration file
        if (dbName == null || dbName.isEmpty()) {
            dbName = options.getString(VariantStorageManager.Options.DB_NAME.key(), VariantStorageManager.Options.DB_NAME.defaultValue());
        }

        DatabaseCredentials database = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getDatabase();

        try {
            return new MongoCredentials(database, dbName);
        } catch (IllegalOpenCGACredentialsException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) throws StorageManagerException {
        if (options != null && !options.getString(FileStudyConfigurationManager.STUDY_CONFIGURATION_PATH, "").isEmpty()) {
            return super.buildStudyConfigurationManager(options);
        } else {
            String dbName = options == null ? null : options.getString(VariantStorageManager.Options.DB_NAME.key());
            String collectionName = options == null ? null : options.getString(COLLECTION_STUDIES.key(), COLLECTION_STUDIES.defaultValue());
            try {
                return new MongoDBStudyConfigurationManager(getMongoCredentials(dbName), collectionName);
//                return getDBAdaptor(dbName).getStudyConfigurationManager();
            } catch (UnknownHostException e) {
                throw new StorageManagerException("Unable to build MongoStorageConfigurationManager", e);
            }
        }
    }
}
