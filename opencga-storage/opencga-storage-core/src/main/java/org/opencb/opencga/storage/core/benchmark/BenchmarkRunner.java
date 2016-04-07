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

import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Created by imedina on 16/06/15.
 */
public abstract class BenchmarkRunner {

    protected String storageEngine;
    protected StorageConfiguration storageConfiguration;

    protected VariantDBAdaptor variantDBAdaptor;

    protected Logger logger;

    public BenchmarkRunner() {
    }


    public abstract BenchmarkStats convert();

    public abstract BenchmarkStats insert();

    public abstract BenchmarkStats query() throws ExecutionException, InterruptedException;

    public abstract BenchmarkStats query(int numRepetitions, Set<String> queries) throws ExecutionException, InterruptedException;

}
