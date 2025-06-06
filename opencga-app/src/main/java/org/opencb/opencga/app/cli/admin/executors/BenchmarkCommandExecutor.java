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

package org.opencb.opencga.app.cli.admin.executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.app.cli.admin.options.BenchmarkCommandOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.storage.BenchmarkConfiguration;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.storage.benchmark.BenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.VariantBenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.generators.MultiQueryGenerator;
import org.opencb.opencga.storage.benchmark.variant.queries.RandomQueries;
import org.opencb.opencga.storage.benchmark.variant.queries.Score;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageManagerRestSampler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class BenchmarkCommandExecutor extends AdminCommandExecutor {

    private final BenchmarkCommandOptions commandOptions;


    public BenchmarkCommandExecutor(BenchmarkCommandOptions commandOptions) {
        super(commandOptions.getCommonOptions());
        this.commandOptions = commandOptions;
    }

    @Override
    public void execute() throws Exception {
        String subCommandString = commandOptions.getSubCommand();
        logger.debug("Executing catalog admin {} command line", subCommandString);
        switch (subCommandString) {
            case "variant":
                variant();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void variant() throws Exception {
        BenchmarkCommandOptions.VariantBenchmarkCommandOptions options = commandOptions.variantBenchmarkCommandOptions;

        Path jmeterHome = Paths.get(appHome, "misc", "benchmark", "jmeter");

        BenchmarkConfiguration benchmarkConf = storageConfiguration.getBenchmark();

        if (options.repetition != null) {
            benchmarkConf.setNumRepetitions(options.repetition);
        }
        if (options.concurrency != null) {
            benchmarkConf.setConcurrency(options.concurrency);
        }
        if (options.delay != null) {
            benchmarkConf.setDelay(options.delay);
        }

        URI uri = new URI(clientConfiguration.getCurrentHost().getUrl());
        uri = uri.resolve(uri.getPath() + VariantStorageManagerRestSampler.STORAGE_MANAGER_REST_PATH).normalize();
        benchmarkConf.setRest(uri);

        Path outdirPath = Paths.get(options.outdir == null ? "" : options.outdir,
                "benchmark_"
                        + TimeUtils.getTime()
                        + "_" + benchmarkConf.getConcurrency() + "x" + benchmarkConf.getNumRepetitions())
                .toAbsolutePath();

        benchmarkConf.setMode(options.executionMode.name());
        benchmarkConf.setConnectionType(options.connectionType.name());

        QueryOptions queryOptions = new QueryOptions();
        if (options.limit != null) {
            queryOptions.append(QueryOptions.LIMIT, options.limit);
        }
        queryOptions.append(QueryOptions.COUNT, options.count);

        HashMap<String, String> httpHeader = new HashMap<>();
        Map<String, String> baseQuery = new HashMap<>();
        baseQuery.putAll(options.baseQuery);
        if (StringUtils.isEmpty(token) || token.equals("NO_TOKEN")) {
            throw new IllegalArgumentException("Please login before running the benchmark command");
        }

        // Populate dataDir with project-specific data
        Path dataDirBase = Paths.get(appHome, "misc", "benchmark", "data", "hsapiens");
        Path dataDir = outdirPath.resolve("data");

        // Create the output directory if it does not exist
        if (!dataDir.toFile().exists()) {
            dataDir.toFile().mkdirs();
        }
        // Copy the data from the base data directory to the output data directory
        Files.list(dataDirBase).forEach(file -> {
            try {
                Files.copy(file, dataDir.resolve(file.getFileName()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        OpenCGAClient openCGAClient = new OpenCGAClient(
                new AuthenticationResponse(sessionManager.getSession().getToken(), sessionManager.getSession().getRefreshToken()),
                clientConfiguration);
        openCGAClient.setUserId(sessionManager.getSession().getUser());
        openCGAClient.setThrowExceptionOnError(true);
        String projectFqn;
        if (StringUtils.isNotEmpty(options.project)) {
            projectFqn = openCGAClient.getProjectClient().info(options.project,
                    new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.FQN.key()))
                    .firstResult().getFqn();
            benchmarkConf.setDatabaseName(projectFqn);
            baseQuery.put(VariantCatalogQueryUtils.PROJECT.key(), projectFqn);
        } else {
            projectFqn = null;
        }
        if (options.executionMode == BenchmarkRunner.ExecutionMode.RANDOM) {
            // Edit randomQueries.yml to include project specific data
            Path randomQueriesFile = dataDir.resolve(MultiQueryGenerator.RANDOM_QUERIES_FILE);
            if (Files.exists(randomQueriesFile)) {
                ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
                RandomQueries randomQueries = objectMapper.readValue(randomQueriesFile.toFile(), RandomQueries.class);


                randomQueries.setStudy(new LinkedList<>());
                randomQueries.setIncludeStudy(new LinkedList<>());
                randomQueries.setCohortStats(new LinkedList<>());
                if (projectFqn != null) {
                    sessionManager.getSession().getStudies().forEach(study -> {
                        if (study.startsWith(projectFqn + ":")) {
                            randomQueries.getStudy().add(study);
                            randomQueries.getIncludeStudy().add(study);
                            randomQueries.getCohortStats().add(new Score(study + ":" + StudyEntry.DEFAULT_COHORT, 0, 0.15));
                        }
                    });
                }

                randomQueries.setBaseQuery(baseQuery);

                objectMapper.writeValue(randomQueriesFile.toFile(), randomQueries);

            } else {
                logger.warn("Random queries file {} does not exist. Skipping modification.", randomQueriesFile);
            }
        }


        // Copy the query file to the data directory
        if (StringUtils.isNotEmpty(options.queryFile)) {
            Path queryFilePath = Paths.get(options.queryFile);
            Path targetQueryFilePath = dataDir.resolve(queryFilePath.getFileName());
            java.nio.file.Files.copy(queryFilePath, targetQueryFilePath);
            options.queryFile = targetQueryFilePath.toString();
        }

        VariantBenchmarkRunner variantBenchmarkRunner = new VariantBenchmarkRunner(storageConfiguration, jmeterHome, outdirPath);

        variantBenchmarkRunner.addThreadGroup(options.connectionType, options.executionMode, dataDir, baseQuery,
                options.queryFile, options.query, queryOptions, httpHeader, token);
        variantBenchmarkRunner.run();
    }


}
