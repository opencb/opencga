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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Throwables;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.protocol.http.control.AuthManager;
import org.apache.jmeter.protocol.http.control.Authorization;
import org.apache.jmeter.protocol.http.control.Header;
import org.apache.jmeter.protocol.http.control.HeaderManager;
import org.apache.jmeter.protocol.http.sampler.HTTPSampler;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.benchmark.BenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.FixedQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.generators.MultiQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.generators.QueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.FixedQuery;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineDirectSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineRestSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageManagerRestSampler;
import org.opencb.opencga.storage.core.io.plain.StringDataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Logger logger = LoggerFactory.getLogger(getClass());

    public VariantBenchmarkRunner(StorageConfiguration storageConfiguration, Path jmeterHome, Path outdir) throws IOException {
        super(storageConfiguration, jmeterHome, outdir);
    }

    public void addThreadGroup(ConnectionType type, ExecutionMode mode, Path dataDir, Map<String, String> baseQuery, String queryFile,
                               String queries, QueryOptions queryOptions) {
        addThreadGroup(type, mode, dataDir, baseQuery, queryFile, queries, queryOptions, Collections.emptyMap(), (Authorization) null);
    }

    public void addThreadGroup(ConnectionType type, ExecutionMode mode, Path dataDir, Map<String, String> baseQuery, String queryFile,
                               String queries, QueryOptions queryOptions, Map<String, String> httpHeader, String token) {
        addThreadGroup(type, mode, dataDir, baseQuery, queryFile, queries, queryOptions, httpHeader,
                new VariantBenchmarkRunner.BearerAuthorization(token));
    }

    public void addThreadGroup(ConnectionType type, ExecutionMode mode, Path dataDir, Map<String, String> baseQuery, String queryFile,
                               String queries, QueryOptions queryOptions, Map<String, String> httpHeader, Authorization authorization) {

        final List<String> queriesList;
        switch (mode) {
            case FIXED:
                if (StringUtils.isEmpty(queries)) {
                    queriesList = readFixedQueriesIdsFromFile(dataDir, queryFile);
                } else {
                    queriesList = Arrays.asList(queries.replaceAll(";", ",").split(","));
                    if (!readFixedQueriesIdsFromFile(dataDir, queryFile).containsAll(queriesList)) {
                        throw new IllegalArgumentException("Query id(s) does not exist in config file : " + queriesList);
                    }
                }
                break;
            case RANDOM:
                if (StringUtils.isEmpty(queries)) {
                    queriesList = readRandomQueriesFromFile(dataDir, queryFile);
                } else {
                    queriesList = Arrays.asList(queries.split(";"));
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown mode " + mode);
        }

        List<VariantStorageEngineSampler> samplers = new ArrayList<>();
        for (String query : queriesList) {
            VariantStorageEngineSampler variantStorageSampler = newVariantStorageEngineSampler(type);

            variantStorageSampler.setStorageEngine(storageEngine)
                    .setDBName(dbName)
                    .setLimit(queryOptions.getInt(QueryOptions.LIMIT, 10))
                    .setCount(queryOptions.getBoolean(QueryOptions.COUNT, true))
                    .setQueryGeneratorConfig(FixedQueryGenerator.FILE, queryFile)
                    .setQueryGeneratorConfig(FixedQueryGenerator.OUT_DIR, outdir.toString());
            setBaseQueryFromCommandLine(variantStorageSampler, baseQuery);

            if (variantStorageSampler instanceof HTTPSampler) {
                HeaderManager headerManager = new HeaderManager();
                httpHeader.forEach((key, value) -> {
                    headerManager.add(new Header(key, value));
                });
                ((HTTPSampler) variantStorageSampler).setHeaderManager(headerManager);

                if (authorization != null) {
                    authorization.setURL(storageConfiguration.getBenchmark().getRest().toString());

                    AuthManager authManager = new AuthManager();
                    authManager.addAuth(authorization);
                    ((HTTPSampler) variantStorageSampler).setAuthManager(authManager);
                }
            }

            switch (mode) {
                case FIXED:
                    variantStorageSampler.setQueryGenerator(FixedQueryGenerator.class);
                    variantStorageSampler.setQueryGeneratorConfig(FixedQueryGenerator.DATA_DIR, dataDir.toString());
                    variantStorageSampler.setQueryGeneratorConfig(FixedQueryGenerator.FIXED_QUERY, query);
                    break;
                case RANDOM:
                    variantStorageSampler.setQueryGenerator(MultiQueryGenerator.class);
                    variantStorageSampler.setQueryGeneratorConfig(MultiQueryGenerator.DATA_DIR, dataDir.toString());
                    variantStorageSampler.setQueryGeneratorConfig(MultiQueryGenerator.MULTI_QUERY, query);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown mode " + mode);
            }
            variantStorageSampler.setName(query);

            samplers.add(variantStorageSampler);
        }

        addThreadGroup(samplers);
    }

    public VariantStorageEngineSampler newVariantStorageEngineSampler(ConnectionType type) {
        switch (type) {
            case REST:
                URI uri = storageConfiguration.getBenchmark().getRest();
                if (uri.getPath().endsWith(VariantStorageEngineRestSampler.STORAGE_ENGINE_REST_PATH)) {
                    return new VariantStorageEngineRestSampler(uri);
                } else {
                    return new VariantStorageManagerRestSampler(uri);
                }
            case DIRECT:
                return new VariantStorageEngineDirectSampler();
            case GRPC:
                throw new UnsupportedOperationException("Unsupported type " + ConnectionType.GRPC);
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private List<String> readRandomQueriesFromFile(Path dataDir, String queryFile) {
        Path queryFilePath;
        if (StringUtils.isEmpty(queryFile)) {
            queryFilePath = Paths.get(dataDir.toString(), MultiQueryGenerator.RANDOM_QUERIES_DEFAULT_FILE);
        } else {
            queryFilePath = Paths.get(queryFile);
        }

        List<String> queries = new ArrayList<>();
        for (String line : new StringDataReader(queryFilePath)) {
            queries.add(line);
        }
        return queries;
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

    public static class BearerAuthorization extends Authorization {

        public BearerAuthorization() {
        }

        public BearerAuthorization(String token) {
            setPass(token);
            setMechanism(AuthManager.Mechanism.BASIC);
        }

        @Override
        public String toBasicHeader() {
            return "Bearer " + getPass();
        }
    }
}
