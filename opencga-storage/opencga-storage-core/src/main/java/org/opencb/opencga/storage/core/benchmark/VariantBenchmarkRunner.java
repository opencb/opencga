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
import java.util.concurrent.*;

/**
 * Created by imedina on 16/06/15.
 */
public class VariantBenchmarkRunner extends BenchmarkRunner {

    private String[] queryType;
    private String queryParams;
    private BenchmarkStats benchmarkStats;

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
        return query(3, new HashSet<>(Arrays.asList("count", "queryByRegion")));
    }

    @Override
    public BenchmarkStats query(int numRepetitions, Set<String> benchmarkTests) throws ExecutionException, InterruptedException {
        System.out.println("numRepetitions = " + numRepetitions);

        int executionTime = 0;

        benchmarkStats = new BenchmarkStats();
        for (int i = 0; i < numRepetitions; i++) {
            Iterator<String> iterator = benchmarkTests.iterator();
            while (iterator.hasNext()) {
                String next = iterator.next();

                queryType = next.split("-");
                if (queryType.length >= 2) {
                    queryParams = queryType[1];
                }

                Query query = new Query();
                QueryOptions queryOptions = new QueryOptions();

                switch (queryType[0]) {
                    case "count":
                        executeThreads(queryType[0], () -> variantDBAdaptor.count(new Query()));
//                        executionTime = count();
                        break;
                    case "distinct":
                        executeThreads(queryType[0], () -> variantDBAdaptor.distinct(new Query(), ""));
                        //executionTime = distinct();
                        break;
                    case "queryById":
                        query.put(VariantDBAdaptor.VariantQueryParams.ID.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
                        //executionTime = queryById();
                        break;
                    case "queryByRegion":
                        query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
                        //executionTime = queryByRegion();
                        break;
                    case "queryByChromosome":
                        query.put(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
                        //executionTime = queryByChromosome();
                        break;
                    case "queryByGene":
                        query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
                        //executionTime = queryByGene();
                        break;
                    case "queryByType":
                        query.put(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
                        //executionTime = queryByType();
                        break;
                    case "queryByReference":
                        query.put(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
                        //executionTime = queryByReference();
                        break;
                    case "queryByAlternate":
                        query.put(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
                        //executionTime = queryByAlternate();
                        break;
                    case "queryByStudies":
                        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), queryParams);
                        executeThreads(queryType[0], () -> variantDBAdaptor.get(query, queryOptions));
//                        //executionTime = queryByStudies();
//                        break;
                    default:
                        break;
                }
//                benchmarkStats.addExecutionTime(next, executionTime);
            }
        }
        benchmarkStats.printSummary();
        return benchmarkStats;
    }
    private <T> List<Future<T>> executeThreads(String test, Callable<T> task) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(10); // storageConfiguration.getBenchmark().getConcurrent()
        List<Future<T>> futureList = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            futureList.add(executorService.submit(task));
        }

        int totalTime = 0;
        for (Future<T> queryResultFuture : futureList) {
            totalTime += ((QueryResult) queryResultFuture.get()).getDbTime();
//            System.out.println("queryResultFuture.get().getDbTime() = " + queryResultFuture.get().getDbTime());
//            System.out.println("queryResultFuture.get().getResult().size() = " + queryResultFuture.get().getResult().get(0));
        }

        benchmarkStats.addExecutionTime(test, totalTime);
        benchmarkStats.addStdDeviation(test, 0.2);

        return futureList;
    }


//    private int count() throws ExecutionException, InterruptedException {
//        List<Future<QueryResult<Long>>> futures = executeThreads("count", () -> variantDBAdaptor.count(new Query()));
////        for (Future<QueryResult<Long>> queryResultFuture : futures) {
////            System.out.println("queryResultFuture.get().getDbTime() = " + queryResultFuture.get().getDbTime());
////            System.out.println("queryResultFuture.get().getResult().size() = " + queryResultFuture.get().getResult().get(0));
////        }
////        QueryResult<Long> count = variantDBAdaptor.count(query);
////        return count.getDbTime();
//        return 2;
//    }

//    private int distinct() {
//        QueryResult distinct = variantDBAdaptor.distinct(new Query(), "gene");
//        System.out.println(distinct.getDbTime());
//        System.out.println("distinct: " + distinct.getResult().size());
//        return distinct.getDbTime();
//    }
//
//    private int queryById() {
//        Query query = new Query();
//        query.put(VariantDBAdaptor.VariantQueryParams.ID.key(), queryParams);
//
//        QueryOptions queryOptions = new QueryOptions();
//        QueryResult<Variant> variantQueryResultByRegion = variantDBAdaptor.get(query, queryOptions);
//
//        System.out.println(variantQueryResultByRegion.getDbTime());
//        return variantQueryResultByRegion.getDbTime();
//    }
//
//    private int queryByRegion() {
//        Query query = new Query();
////        query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), "1:333-116666");
//        query.put(VariantDBAdaptor.VariantQueryParams.REGION.key(), queryParams);
//
//        QueryOptions queryOptions = new QueryOptions();
//        QueryResult<Variant> variantQueryResultByRegion = variantDBAdaptor.get(query, queryOptions);
//
//        System.out.println(variantQueryResultByRegion.getDbTime());
//        return variantQueryResultByRegion.getDbTime();
//    }
//
//    private int queryByChromosome() {
//        Query query = new Query();
//        query.put(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), queryParams);
//
//        QueryOptions queryOptions = new QueryOptions();
//        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);
//
//        System.out.println(variantQueryResultByChr.getDbTime());
//        return variantQueryResultByChr.getDbTime();
//    }
//
//    private int queryByGene() {
//        Query query = new Query();
//        query.put(VariantDBAdaptor.VariantQueryParams.GENE.key(), queryParams);
//
//        QueryOptions queryOptions = new QueryOptions();
//        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);
//
//        System.out.println(variantQueryResultByChr.getDbTime());
//        return variantQueryResultByChr.getDbTime();
//    }
//
//    private int queryByType() {
//        Query query = new Query();
//        query.put(VariantDBAdaptor.VariantQueryParams.TYPE.key(), queryParams);
//
//        QueryOptions queryOptions = new QueryOptions();
//        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);
//
//        System.out.println(variantQueryResultByChr.getDbTime());
//        return variantQueryResultByChr.getDbTime();
//    }
//
//    private int queryByReference() {
//        Query query = new Query();
//        query.put(VariantDBAdaptor.VariantQueryParams.REFERENCE.key(), queryParams);
//
//        QueryOptions queryOptions = new QueryOptions();
//        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);
//
//        System.out.println(variantQueryResultByChr.getDbTime());
//        return variantQueryResultByChr.getDbTime();
//    }
//
//    private int queryByAlternate() {
//        Query query = new Query(VariantDBAdaptor.VariantQueryParams.ALTERNATE.key(), queryParams);
//        QueryOptions queryOptions = new QueryOptions();
//
//        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);
//        logger.debug("queryByAlternate: {}", variantQueryResultByChr.getDbTime());
//        return variantQueryResultByChr.getDbTime();
//    }
//
//    private int queryByStudies() {
//        Query query = new Query(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), queryParams);
//        QueryOptions queryOptions = new QueryOptions();
//
//        QueryResult<Variant> variantQueryResultByChr = variantDBAdaptor.get(query, queryOptions);
//
//        System.out.println(variantQueryResultByChr.getDbTime());
//        return variantQueryResultByChr.getDbTime();
//    }

}