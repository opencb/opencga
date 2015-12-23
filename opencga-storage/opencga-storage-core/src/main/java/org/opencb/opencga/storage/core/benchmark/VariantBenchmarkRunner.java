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

import java.util.*;

/**
 * Created by imedina on 16/06/15.
 */
public class VariantBenchmarkRunner extends BenchmarkRunner {

    String[] queryType;
    String queryParams;

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
    public BenchmarkStats convert() {
        return null;
    }

    @Override
    public BenchmarkStats insert() {
        return null;
    }

    @Override
    public BenchmarkStats query() {
        return query(3, new HashSet<>(Arrays.asList("count", "queryByRegion")));
    }

    @Override
    public BenchmarkStats query(int numRepetitions, Set<String> benchmarkTests) {
        System.out.println("numRepetitions = " + numRepetitions);

        int executionTime = 0;

        BenchmarkStats benchmarkStats = new BenchmarkStats();
        for (int i = 0; i < numRepetitions; i++) {
            Iterator<String> iterator = benchmarkTests.iterator();
            while (iterator.hasNext()) {
                String next = iterator.next();
                queryType = next.split("-");

                if (queryType.length >= 2) {
                    queryParams = queryType[1];
                }

//                System.out.println("queryType = " + queryType[0]);
//                System.out.println("queryParams = " + queryParams);
                switch (queryType[0]) {
                    case "count":
                        executionTime = count();
                        break;
//                    case "distinct":
//                        executionTime = distinct();
//                        break;
                    case "queryById":
                        executionTime = queryById();
                        break;
                    case "queryByRegion":
                        executionTime = queryByRegion();
                        break;
                    case "queryByChromosome":
                        executionTime = queryByChromosome();
                        break;
                    case "queryByGene":
                        executionTime = queryByGene();
                        break;
                    case "queryByType":
                        executionTime = queryByType();
                        break;
                    case "queryByReference":
                        executionTime = queryByReference();
                        break;
                    case "queryByAlternate":
                        executionTime = queryByAlternate();
                        break;
                    case "queryByStudies":
                        executionTime = queryByStudies();
                        break;
                    default:
                        break;
                }
                benchmarkStats.addExecutionTime(next, executionTime);
            }
        }
        benchmarkStats.printSummary();
        return benchmarkStats;
    }

    private int count() {
        Query query = new Query();
        QueryResult<Long> count = variantDBAdaptor.count(query);
        System.out.println(count.getDbTime());
        return count.getDbTime();
    }

//    private int distinct() {
//        Query query = new Query();
//        QueryResult distinct = variantDBAdaptor.distinct(query, queryParams);
//        System.out.println(distinct.getDbTime());
//        return distinct.getDbTime();
//    }

    private int queryById() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.ID.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByRegion = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByRegion.getDbTime());
        return variantQueryResultByRegion.getDbTime();
    }

    private int queryByRegion() {
        Query query = new Query();
//        query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), "1:333-116666");
        query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByRegion = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByRegion.getDbTime());
        return variantQueryResultByRegion.getDbTime();
    }

    private int queryByChromosome() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByChr.getDbTime());
        return variantQueryResultByChr.getDbTime();
    }

    private int queryByGene() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByChr.getDbTime());
        return variantQueryResultByChr.getDbTime();
    }

    private int queryByType() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByChr.getDbTime());
        return variantQueryResultByChr.getDbTime();
    }

    private int queryByReference() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByChr.getDbTime());
        return variantQueryResultByChr.getDbTime();
    }

    private int queryByAlternate() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByChr.getDbTime());
        return variantQueryResultByChr.getDbTime();
    }

    private int queryByStudies() {
        Query query = new Query();
        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), queryParams);

        QueryOptions queryOptions = new QueryOptions();
        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);

        System.out.println(variantQueryResultByChr.getDbTime());
        return variantQueryResultByChr.getDbTime();
    }



}