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

package org.opencb.opencga.storage.app.cli.server.executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.BenchmarkConfiguration;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.server.options.BenchmarkCommandOptions;
import org.opencb.opencga.storage.benchmark.variant.VariantBenchmarkRunner;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageEngineRestSampler;
import org.opencb.opencga.storage.benchmark.variant.samplers.VariantStorageManagerRestSampler;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created on 07/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class BenchmarkCommandExecutor extends CommandExecutor {

    private final BenchmarkCommandOptions commandOptions;

    public BenchmarkCommandExecutor(BenchmarkCommandOptions commandOptions) {
        super(commandOptions.commonCommandOptions);
        this.commandOptions = commandOptions;
    }


    @Override
    public void execute() throws Exception {
        String subCommandString = getParsedSubCommand(commandOptions.jCommander);
        switch (subCommandString) {
            case "variant":
                variant();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void variant() throws IOException, URISyntaxException {
        BenchmarkCommandOptions.VariantBenchmarkCommandOptions options = commandOptions.variantBenchmarkCommandOptions;

        Path jmeterHome = Paths.get(appHome, "benchmark", "jmeter");
        Path dataDir = Paths.get(appHome, "benchmark", "data", "hsapiens");

        if (StringUtils.isNotEmpty(options.dbName)) {
            configuration.getBenchmark().setDatabaseName(options.dbName);
        }
        if (options.repetition != null) {
            configuration.getBenchmark().setNumRepetitions(options.repetition);
        }
        if (options.concurrency != null) {
            configuration.getBenchmark().setConcurrency(options.concurrency);
        }
        if (options.delay != null) {
            configuration.getBenchmark().setDelay(options.delay);
        }
        setHost(options);
        Path outdirPath = getBenchmarkPath(options, configuration.getBenchmark());

        configuration.getBenchmark().setMode(options.executionMode.name());
        configuration.getBenchmark().setConnectionType(options.connectionType.name());

        VariantBenchmarkRunner variantBenchmarkRunner = new VariantBenchmarkRunner(configuration, jmeterHome, outdirPath);
        QueryOptions queryOptions = new QueryOptions();
        if (options.limit != null) {
            queryOptions.append(QueryOptions.LIMIT, options.limit);
        }
        queryOptions.append(QueryOptions.COUNT, options.count);
        variantBenchmarkRunner.addThreadGroup(options.connectionType, options.executionMode, dataDir, options.baseQuery, options.queryFile, options.query, queryOptions);
        variantBenchmarkRunner.run();
    }

    private Path getBenchmarkPath(BenchmarkCommandOptions.VariantBenchmarkCommandOptions options, BenchmarkConfiguration benchmark) {
        return Paths.get(options.outdir == null ? "" : options.outdir,
                "opencga_benchmark_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + "_" + benchmark.getConcurrency() + "x" + benchmark.getNumRepetitions() + "_" + options.executionMode).toAbsolutePath();
    }

    private void setHost(BenchmarkCommandOptions.VariantBenchmarkCommandOptions options) throws URISyntaxException {
        URIBuilder uriBuilder;
        String host = options.host;

        if (host == null) {
            uriBuilder = new URIBuilder(configuration.getBenchmark().getRest());
        } else {
            uriBuilder = new URIBuilder(host);
        }

        String storageRest = commandOptions.commonCommandOptions.params.get("storage.rest");

        if ("true".equalsIgnoreCase(storageRest)) {
            uriBuilder.setPath(uriBuilder.getPath().concat(VariantStorageEngineRestSampler.STORAGE_ENGINE_REST_PATH));
        } else {
            uriBuilder.setPath(uriBuilder.getPath().concat(VariantStorageManagerRestSampler.STORAGE_MANAGER_REST_PATH));
        }

        URI uri = new URI(uriBuilder.toString()).normalize();
        logger.info("Setting REST URI to {}", uri);
        configuration.getBenchmark().setRest(uri);
    }

}
