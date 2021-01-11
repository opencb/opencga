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

package org.opencb.opencga.storage.benchmark.variant;

import com.beust.jcommander.MissingCommandException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.benchmark.BenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.FixedQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.generators.MultiQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.generators.QueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQuery;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineDirectSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineRestSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineSampler;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created on 06/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantBenchmarkRunner extends BenchmarkRunner {

    public VariantBenchmarkRunner(StorageConfiguration storageConfiguration, Path jmeterHome, Path outdir) throws IOException {
        super(storageConfiguration, jmeterHome, outdir);
    }

    public void addThreadGroup(ConnectionType type, ExecutionMode mode, Path dataDir, Map<String, String> baseQuery, String queryFile,
                               String queries, QueryOptions queryOptions) {

        List<String> queriesList = new ArrayList<>();
        if (mode.equals(ExecutionMode.FIXED)) {
            if (StringUtils.isEmpty(queries)) {
                queriesList = readFixedQueriesIdsFromFile(dataDir, queryFile);
            } else {
                queriesList = Arrays.asList(queries.replaceAll(";", ",").split(","));
                if (!readFixedQueriesIdsFromFile(dataDir, queryFile).containsAll(queriesList)) {
                    throw new IllegalArgumentException("Query id(s) does not exist in config file : " + queriesList);
                }
            }
        } else if (mode.equals(ExecutionMode.RANDOM)) {
            if (StringUtils.isEmpty(queries)) {
                throw new MissingCommandException("Please provide execution queries for dynamic mode.");
            }
            queriesList = Arrays.asList(queries.split(";"));
        }

        List<VariantStorageEngineSampler> samplers = new ArrayList<>();
        for (String query : queriesList) {
            VariantStorageEngineSampler variantStorageSampler = newVariantStorageEngineSampler(type);

            variantStorageSampler.setStorageEngine(storageEngine);
            variantStorageSampler.setDBName(dbName);
            variantStorageSampler.setLimit(queryOptions.getInt(QueryOptions.LIMIT, -1));
            variantStorageSampler.setCount(queryOptions.getBoolean(QueryOptions.COUNT, false));
            variantStorageSampler.setQueryGeneratorConfig(FixedQueryGenerator.FILE, queryFile);
            variantStorageSampler.setQueryGeneratorConfig(FixedQueryGenerator.OUT_DIR, outdir.toString());
            setBaseQueryFromCommandLine(variantStorageSampler, baseQuery);

            if (mode.equals(ExecutionMode.FIXED)) {
                variantStorageSampler.setQueryGenerator(FixedQueryGenerator.class);
                variantStorageSampler.setQueryGeneratorConfig(FixedQueryGenerator.DATA_DIR, dataDir.toString());
                variantStorageSampler.setQueryGeneratorConfig(FixedQueryGenerator.FIXED_QUERY, query);
            } else if (mode.equals(ExecutionMode.RANDOM)) {
                variantStorageSampler.setQueryGenerator(MultiQueryGenerator.class);
                variantStorageSampler.setQueryGeneratorConfig(MultiQueryGenerator.DATA_DIR, dataDir.toString());
                variantStorageSampler.setQueryGeneratorConfig(MultiQueryGenerator.MULTI_QUERY, query);
            }
            variantStorageSampler.setName(query);

            samplers.add(variantStorageSampler);
        }

        addThreadGroup(samplers);
    }

    public VariantStorageEngineSampler newVariantStorageEngineSampler(ConnectionType type) {
        switch (type) {
            case REST:
                URI rest = storageConfiguration.getBenchmark().getRest();
                return new VariantStorageEngineRestSampler(rest.getHost(), rest.getPath(), rest.getPort());
            case DIRECT:
                return new VariantStorageEngineDirectSampler();
            case GRPC:
                throw new UnsupportedOperationException("Unsupported type " + ConnectionType.GRPC);
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private List<String> readFixedQueriesIdsFromFile(Path dataDir, String queryFile) {

        Path queryFilePath;
        FixedQueries fixedQueries;
        List<String> queryList = new ArrayList<>();

        if (StringUtils.isEmpty(queryFile)) {
            queryFilePath = Paths.get(dataDir.toString(), FixedQueryGenerator.FIXED_QUERIES_FILE);
        } else {
            queryFilePath = Paths.get(queryFile);
        }
        try (FileInputStream inputStream = new FileInputStream(queryFilePath.toFile())) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            fixedQueries = objectMapper.readValue(inputStream, FixedQueries.class);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        for (FixedQuery fixedQuery : fixedQueries.getQueries()) {
            queryList.add(fixedQuery.getId());
        }

        return queryList;
    }

    private void setBaseQueryFromCommandLine(VariantStorageEngineSampler variantStorageEngineSampler, Map<String, String> baseQuery) {
        if (Objects.nonNull(baseQuery)) {
            for (String key : baseQuery.keySet()) {
                variantStorageEngineSampler.setQueryGeneratorConfig(QueryGenerator.BASE_QUERY_REFIX + key, baseQuery.get(key));
            }
        }
    }
}
