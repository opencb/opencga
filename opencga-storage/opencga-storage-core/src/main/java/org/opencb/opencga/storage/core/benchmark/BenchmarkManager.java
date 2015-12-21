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

import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

/**
 * Created by imedina on 08/10/15.
 */
public class BenchmarkManager {

    private StorageConfiguration storageConfiguration;
    String load = null;
    String numOfRepetition = null;
    String tableName = null;
    String query = null;
    boolean isLoadRequired;

    public BenchmarkManager() {

    }

    public BenchmarkStats variantBenchmark(StorageConfiguration storageConfiguration, String numOfRepetition, String dbTableName,
                                           String dbQuery) throws ClassNotFoundException, StorageManagerException,
            InstantiationException, IllegalAccessException {

        BenchmarkStats benchmarkStats;
        BenchmarkRunner benchmarkRunner = new VariantBenchmarkRunner(storageConfiguration);

        benchmarkStats = benchmarkRunner.query(Integer.parseInt(numOfRepetition), dbQuery);

        return benchmarkStats;
    }

    //Load data if user provide the loading option with file path
    private void loadDataToHBase(String filePath) {
        //TODO : If user wants to load the data to HBase and then benchmarking
    }

    public void run(String[] args) throws ClassNotFoundException, StorageManagerException, InstantiationException, IllegalAccessException {

        load = args[0];
        numOfRepetition = args[1];
        tableName = args[2];
        query = args[3];
        isLoadRequired = Boolean.parseBoolean(args[4]);

        System.out.println("load :: " + load);
        System.out.println("numOfRepetition :: " + numOfRepetition);
        System.out.println("tableName :: " + tableName);
        System.out.println("query :: " + query);
        System.out.println("isLoadRequired :: " + isLoadRequired);

        String dbTableName = null;
        if (isLoadRequired) {
            //use the file path and table name provided by user
            //loadDataToHBase(loadFilePath);
            //dbTableName = tableName;
        } else {
            // use the default HBase tables for query
            dbTableName = tableName;
        }

        StorageConfiguration storageConfiguration = new StorageConfiguration();
        variantBenchmark(storageConfiguration, numOfRepetition, dbTableName, query);

    }

}