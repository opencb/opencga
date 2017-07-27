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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.app.cli.CommandExecutor;
import org.opencb.opencga.storage.app.cli.server.options.BenchmarkCommandOptions;
import org.opencb.opencga.storage.benchmark.variant.VariantBenchmarkRunner;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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
            case "alignment":
                alignment();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void variant() throws IOException {
        BenchmarkCommandOptions.VariantBenchmarkCommandOptions options = commandOptions.variantBenchmarkCommandOptions;

        Path outdirPath = Paths.get(options.outdir == null ? "" : options.outdir).toAbsolutePath();
        Path jmeterHome = Paths.get(appHome, "benchmark", "jmeter");
        Path dataDir = Paths.get(appHome, "benchmark", "data", "hsapiens");

        if (StringUtils.isNotEmpty(options.commonOptions.storageEngine)) {
            configuration.getBenchmark().setStorageEngine(options.commonOptions.storageEngine);
        }
        if (StringUtils.isNotEmpty(options.dbName)) {
            configuration.getBenchmark().setDatabaseName(options.dbName);
        }
        if (options.repetition != null) {
            configuration.getBenchmark().setNumRepetitions(options.repetition);
        }
        if (options.concurrency != null) {
            configuration.getBenchmark().setConcurrency(options.concurrency);
        }

        VariantBenchmarkRunner variantBenchmarkRunner = new VariantBenchmarkRunner(configuration, jmeterHome, outdirPath);
//        variantBenchmarkRunner.addThreadGroup(options.connectionType, dataDir,
//                Arrays.asList(GeneQueryGenerator.class, RegionQueryGenerator.class));
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.append(QueryOptions.LIMIT, options.limit);
        queryOptions.append(QueryOptions.COUNT, options.count);
        variantBenchmarkRunner.addThreadGroup(options.connectionType, dataDir, options.query, queryOptions);
//        variantBenchmarkRunner.newThreadGroup(VariantBenchmarkRunner.ConnectionType.REST, dataDir,
//                Arrays.asList(RegionQueryGenerator.class));
//        variantBenchmarkRunner.newThreadGroup(VariantBenchmarkRunner.ConnectionType.REST, dataDir,
//                Arrays.asList(GeneQueryGenerator.class));
        variantBenchmarkRunner.run();
    }


    private void alignment() {
        throw new UnsupportedOperationException("Benchmark not supported yet!");
    }


}
