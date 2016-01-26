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

import java.util.HashSet;
import java.util.concurrent.ExecutionException;

/**
 * Created by imedina on 08/10/15.
 */
public class BenchmarkManager {

    private StorageConfiguration storageConfiguration;

    public BenchmarkManager(StorageConfiguration storageConfiguration) {
        this.storageConfiguration = storageConfiguration;
    }

    public BenchmarkStats variantBenchmark() throws ClassNotFoundException, StorageManagerException, InstantiationException,
            IllegalAccessException, ExecutionException, InterruptedException {

        BenchmarkRunner benchmarkRunner = new VariantBenchmarkRunner(storageConfiguration);
        BenchmarkStats benchmarkStats = benchmarkRunner.query(storageConfiguration.getBenchmark().getNumRepetitions(),
                new HashSet<>(storageConfiguration.getBenchmark().getQueries()));

        return benchmarkStats;
    }

    //Load data if user provide the loading option with file path
    private void loadDataToHBase(String filePath) {
        //TODO : If user wants to load the data to HBase and then benchmarking
    }
}
