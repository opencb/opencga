package org.opencb.opencga.storage.core.benchmark;

import org.opencb.datastore.core.Query;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by pawan on 06/01/16.
 */
public class CliBenchmarkRunner extends BenchmarkRunner {

    public CliBenchmarkRunner(StorageConfiguration storageConfiguration) throws IllegalAccessException, ClassNotFoundException,
            InstantiationException, StorageManagerException {
        this(storageConfiguration.getDefaultStorageEngineId(), storageConfiguration);
    }

    public CliBenchmarkRunner(String storageEngine, StorageConfiguration storageConfiguration)
            throws IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException {
        this.storageEngine = storageEngine;
        this.storageConfiguration = storageConfiguration;
        logger = LoggerFactory.getLogger(this.getClass());
        init(storageEngine);
    }

    private void init(String storageEngine)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, StorageManagerException {
        StorageManagerFactory storageManagerFactory = new StorageManagerFactory(storageConfiguration);
        VariantStorageManager variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        variantDBAdaptor = variantStorageManager.getDBAdaptor(storageConfiguration.getBenchmark().getDatabaseName());
    }

    @Override
    public BenchmarkStats convert() {
        return null;
    }

    @Override
    public BenchmarkStats insert() {
        return null;
    }

    @Override
    public BenchmarkStats query() throws ExecutionException, InterruptedException {
        return query(3, new LinkedHashSet<>());
    }

    @Override
    public BenchmarkStats query(int numRepetitions, Query query) throws ExecutionException, InterruptedException {
        return null;
    }

    @Override
    public BenchmarkStats query(int numRepetitions, Set<String> benchmarkTests) throws ExecutionException, InterruptedException {
        return null;
    }


}
