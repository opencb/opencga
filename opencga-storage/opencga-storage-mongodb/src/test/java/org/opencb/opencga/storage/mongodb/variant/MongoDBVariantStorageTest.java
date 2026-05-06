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

package org.opencb.opencga.storage.mongodb.variant;

import com.mongodb.client.model.Sorts;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.bson.Document;
import org.junit.rules.ExternalResource;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.commons.datastore.mongodb.test.EmbeddedMongoDBManager;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageTest;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorTest;
import org.opencb.opencga.storage.mongodb.auth.MongoCredentials;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.opencb.opencga.storage.core.variant.VariantStorageBaseTest.DB_NAME;

/**
 * Created by hpccoll1 on 01/06/15.
 */
public interface MongoDBVariantStorageTest extends VariantStorageTest {

    Logger logger = LoggerFactory.getLogger(MongoDBVariantStorageTest.class);
    AtomicReference<MongoDBVariantStorageEngine> manager = new AtomicReference<>(null);
    List<MongoDBVariantStorageEngine> managers = Collections.synchronizedList(new ArrayList<>());

    default StorageConfiguration loadStorageConfiguration() throws Exception {
        InputStream is = MongoDBVariantStorageTest.class.getClassLoader().getResourceAsStream("storage-configuration.yml");
        StorageConfiguration storageConfiguration = StorageConfiguration.load(is);

        EmbeddedMongoDBManager embeddedMongo = EmbeddedMongoDBManager.getInstance();
        if (embeddedMongo.isEnabled()) {
            if (!embeddedMongo.isRunning()) {
                embeddedMongo.start();
            }
            storageConfiguration.getVariant().getEngines().stream()
                    .filter(e -> e.getId().equals(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID))
                    .forEach(e -> e.getDatabase().setHosts(
                            Collections.singletonList(embeddedMongo.getConnectionString())
                    ));
        }
        return storageConfiguration;
    }

    default MongoDBVariantStorageEngine getVariantStorageEngine() throws Exception {
        synchronized (manager) {
            MongoDBVariantStorageEngine storageManager = manager.get();
            if (storageManager == null) {
                storageManager = new MongoDBVariantStorageEngine();
                manager.set(storageManager);
            }
            StorageConfiguration storageConfiguration = loadStorageConfiguration();
            storageManager.setConfiguration(storageConfiguration, MongoDBVariantStorageEngine.STORAGE_ENGINE_ID, DB_NAME);
            storageManager.getOptions().put(VariantStorageOptions.ANNOTATOR.key(), VariantAnnotatorFactory.AnnotationEngine.OTHER);
            storageManager.getOptions().put(VariantStorageOptions.ANNOTATOR_CLASS.key(),
                    VariantAnnotatorTest.TestCachedCellBaseRestVariantAnnotator.class.getName());
            storageManager.getOptions().put(VariantStorageOptions.SPECIES.key(), "hsapiens");
            storageManager.getOptions().put(VariantStorageOptions.ASSEMBLY.key(), "GRCh37");
            storageManager.getConfiguration().getCellbase().setDataRelease("1");

            return storageManager;
        }
    }

    default MongoDBVariantStorageEngine newVariantStorageEngine() throws Exception {
        synchronized (managers) {
            MongoDBVariantStorageEngine storageManager = new MongoDBVariantStorageEngine();
            StorageConfiguration storageConfiguration = loadStorageConfiguration();
            storageManager.setConfiguration(storageConfiguration, MongoDBVariantStorageEngine.STORAGE_ENGINE_ID, DB_NAME);
            managers.add(storageManager);
            return storageManager;
        }
    }

    /**
     * Builds a fresh MongoDBVariantStorageEngine with all collections renamed using the given
     * suffix, so it can coexist with the main test engine as an independent "expected" instance.
     * Used for cross-engine document comparisons.
     */
    default MongoDBVariantStorageEngine getVariantStorageEngine(String collectionSuffix) throws Exception {
        MongoDBVariantStorageEngine variantStorageEngine = newVariantStorageEngine();
        org.opencb.commons.datastore.core.ObjectMap renameCollections = new org.opencb.commons.datastore.core.ObjectMap()
                .append(MongoDBVariantStorageOptions.COLLECTION_VARIANTS.key(),
                        MongoDBVariantStorageOptions.COLLECTION_VARIANTS.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_PROJECT.key(),
                        MongoDBVariantStorageOptions.COLLECTION_PROJECT.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_STUDIES.key(),
                        MongoDBVariantStorageOptions.COLLECTION_STUDIES.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_FILES.key(),
                        MongoDBVariantStorageOptions.COLLECTION_FILES.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_SAMPLES.key(),
                        MongoDBVariantStorageOptions.COLLECTION_SAMPLES.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_TASKS.key(),
                        MongoDBVariantStorageOptions.COLLECTION_TASKS.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_COHORTS.key(),
                        MongoDBVariantStorageOptions.COLLECTION_COHORTS.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_STAGE.key(),
                        MongoDBVariantStorageOptions.COLLECTION_STAGE.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_ANNOTATION.key(),
                        MongoDBVariantStorageOptions.COLLECTION_ANNOTATION.defaultValue() + collectionSuffix)
                .append(MongoDBVariantStorageOptions.COLLECTION_TRASH.key(),
                        MongoDBVariantStorageOptions.COLLECTION_TRASH.defaultValue() + collectionSuffix);

        variantStorageEngine.getOptions().putAll(renameCollections);
        // Also propagate to the StorageConfiguration so that VariantMongoDBAdaptor.getStageCollection()
        // and other adaptor methods resolve the correct (renamed) collection names.
        variantStorageEngine.getConfiguration()
                .getVariantEngine(MongoDBVariantStorageEngine.STORAGE_ENGINE_ID)
                .getOptions().putAll(renameCollections);
        return variantStorageEngine;
    }

    default long compareCollections(MongoDBCollection expectedCollection, MongoDBCollection actualCollection) {
        return compareCollections(expectedCollection, actualCollection, d -> d);
    }

    default long compareCollections(MongoDBCollection expectedCollection, MongoDBCollection actualCollection,
                                    Function<Document, Document> map) {
        QueryOptions options = new QueryOptions(QueryOptions.SORT, Sorts.ascending("_id"))
                .append(QueryOptions.EXCLUDE, DocumentToVariantConverter.INDEX_FIELD);

        System.out.println("Comparing " + expectedCollection + " vs " + actualCollection);
        assertNotEquals(expectedCollection.toString(), actualCollection.toString());
        assertEquals(expectedCollection.count().getNumMatches(), actualCollection.count().getNumMatches());
        assertNotEquals(0L, expectedCollection.count().getNumMatches());

        Iterator<Document> actualIterator = actualCollection.nativeQuery().find(new Document(), options);
        Iterator<Document> expectedIterator = expectedCollection.nativeQuery().find(new Document(), options);

        long c = 0;
        while (actualIterator.hasNext() && expectedIterator.hasNext()) {
            c++;
            Document actual = map.apply(actualIterator.next());
            Document expected = map.apply(expectedIterator.next());
            assertEquals(expected, actual);
        }
        assertFalse(actualIterator.hasNext());
        assertFalse(expectedIterator.hasNext());
        return c;
    }

    default void closeConnections() throws IOException {
        System.out.println("Closing MongoDBVariantStorageEngine");
        for (MongoDBVariantStorageEngine manager : managers) {
            System.out.println("closing manager = " + manager);
            manager.close();
        }
        managers.clear();
        if (manager.get() != null) {
            manager.get().close();
        }
    }

    default void clearDB(String dbName) throws Exception {
        MongoCredentials credentials = getVariantStorageEngine().getMongoCredentials();
//        String mongoDbName = credentials.getMongoDbName();
        String mongoDbName = dbName;
        logger.info("Cleaning MongoDB {}", mongoDbName);
        try (MongoDataStoreManager mongoManager = new MongoDataStoreManager(credentials.getDataStoreServerAddresses())) {
            mongoManager.get(mongoDbName, credentials.getMongoDBConfiguration());
            mongoManager.drop(mongoDbName);
        }
    }

    @Override
    default void close() throws Exception {
        closeConnections();
    }

    default MongoDataStoreManager getMongoDataStoreManager(String dbName) throws Exception {
        MongoCredentials credentials = getVariantStorageEngine().getMongoCredentials();
        return new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
    }

    default void logLevel(String level) {
//        ConsoleAppender stderr = (ConsoleAppender) LogManager.getRootLogger().getAppender("stderr");
//        stderr.setThreshold(Level.toLevel(level));
        Configurator.setRootLevel(Level.toLevel(level));
        Configurator.setLevel("org.mongodb.driver.cluster", Level.WARN);
        Configurator.setLevel("org.mongodb.driver.connection", Level.WARN);
        Configurator.setLevel("org.mongodb.driver.protocol.update", Level.WARN);
        Configurator.setLevel("org.mongodb.driver.protocol.command", Level.WARN);
        Configurator.setLevel("org.mongodb.driver.protocol.query", Level.WARN);
        Configurator.setLevel("org.mongodb.driver.protocol.getmore", Level.WARN);
    }

    /**
     * JUnit rule mirroring {@code HadoopVariantStorageTest.HadoopExternalResource}: starts the
     * process-singleton embedded mongod in {@link #before()} and delegates to the
     * {@link MongoDBVariantStorageTest} defaults for engine access and cleanup.
     */
    class MongoDBExternalResource extends ExternalResource implements MongoDBVariantStorageTest {

        @Override
        public void before() throws Exception {
            // Silence noisy mongo driver loggers once per class (mirrors logLevel()).
            Configurator.setLevel("org.mongodb.driver.cluster", Level.WARN);
            Configurator.setLevel("org.mongodb.driver.connection", Level.WARN);
            Configurator.setLevel("org.mongodb.driver.protocol.command", Level.WARN);
            EmbeddedMongoDBManager.getInstance().start();
        }

        @Override
        public void after() {
            try {
                closeConnections();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            // Note: the embedded mongod is a JVM-wide singleton owned by EmbeddedMongoDBManager;
            // its shutdown hook stops the process on JVM exit.
        }

        /**
         * Drops every database on the embedded mongod whose name starts with {@code opencga_}.
         * Used by the analysis test harness to reset state between runs.
         */
        public void clearAllDBs() throws Exception {
            MongoCredentials credentials = getVariantStorageEngine().getMongoCredentials();
            try (MongoDataStoreManager mongoManager =
                         new MongoDataStoreManager(credentials.getDataStoreServerAddresses());
                 com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(
                         "mongodb://" + EmbeddedMongoDBManager.getInstance().getConnectionString())) {
                for (String dbName : client.listDatabaseNames()) {
                    if (dbName.startsWith("opencga_")) {
                        mongoManager.drop(dbName);
                    }
                }
            }
        }
    }
}
