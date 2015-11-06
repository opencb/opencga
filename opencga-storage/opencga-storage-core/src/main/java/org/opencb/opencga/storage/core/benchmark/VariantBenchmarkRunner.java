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

package org.opencb.opencga.storage.core.benchmark;

import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.LoggerFactory;

/**
 * Created by imedina on 16/06/15.
 */
public class VariantBenchmarkRunner extends BenchmarkRunner {

    public VariantBenchmarkRunner(StorageConfiguration storageConfiguration) throws IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException {
        this(storageConfiguration.getDefaultStorageEngineId(), storageConfiguration);
    }

    public VariantBenchmarkRunner(String storageEngine, StorageConfiguration storageConfiguration) throws IllegalAccessException, ClassNotFoundException, InstantiationException, StorageManagerException {
        this.storageEngine = storageEngine;
        this.storageConfiguration = storageConfiguration;

        logger = LoggerFactory.getLogger(this.getClass());
        init(storageEngine);
    }

    private void init(String storageEngine) throws IllegalAccessException, InstantiationException, ClassNotFoundException, StorageManagerException {
        StorageManagerFactory storageManagerFactory = new StorageManagerFactory(storageConfiguration);
        VariantStorageManager variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        variantDBAdaptor = variantStorageManager.getDBAdaptor(storageConfiguration
                .getStorageEngine(storageEngine).getVariant().getOptions().get("database.name").toString());
    }


    @Override
    public BenchmarkStats insert() {
        return null;
    }


    @Override
    public BenchmarkStats query() {
        return query(3);
    }

    @Override
    public BenchmarkStats query(int numRepetitions) {

//        List<BenchmarkStats> benchmarkStatsList = new ArrayList<>(numRepetitions);
        BenchmarkStats benchmarkStats = new BenchmarkStats();
        int ms;
        int countTime = 0;
        int queryTime = 0;
        for (int i = 0; i < numRepetitions; i++) {
            countTime += count();

            queryTime += queryByRegion();


//            benchmarkStatsList.add(benchmarkStats);
        }

//        benchmarkStats.put("count", countTime/numRepetitions);
//        benchmarkStats.put("query", queryTime/numRepetitions);

        return null;
    }

    private int count() {
        Query query = new Query();
        QueryResult<Long> count = variantDBAdaptor.count(query);
        System.out.println(count.getDbTime());
        return count.getDbTime();
    }

    private int queryByRegion() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), "1:333-116666");

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResult = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResult.getDbTime());
        return variantQueryResult.getDbTime();
    }

}
