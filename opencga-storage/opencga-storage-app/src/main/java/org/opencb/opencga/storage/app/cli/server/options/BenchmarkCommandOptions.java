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

package org.opencb.opencga.storage.app.cli.server.options;

import com.beust.jcommander.*;
import org.opencb.opencga.storage.app.cli.GeneralCliOptions;
import org.opencb.opencga.storage.benchmark.BenchmarkRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 07/04/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Parameters(commandNames = {"benchmark"}, commandDescription = "Execute benchmark operations.")
public class BenchmarkCommandOptions {

    public final VariantBenchmarkCommandOptions variantBenchmarkCommandOptions;
    public final AlignmentBenchmarkCommandOptions alignmentBenchmarkCommandOptions;
    public JCommander jCommander;
    public GeneralCliOptions.CommonOptions commonCommandOptions;

    public BenchmarkCommandOptions(GeneralCliOptions.CommonOptions commonOptions, JCommander jCommander) {
        this.jCommander = jCommander;
        this.commonCommandOptions = commonOptions;

        variantBenchmarkCommandOptions = new VariantBenchmarkCommandOptions();
        alignmentBenchmarkCommandOptions = new AlignmentBenchmarkCommandOptions();
    }

    /**
     * Generic benchmark options
     */
    public class GenericBenchmarkOptions {

        @Parameter(names = {"--connector"}, description = "How to connect to the system: REST or DIRECT")
        public BenchmarkRunner.ConnectionType connectionType = BenchmarkRunner.ConnectionType.REST;

        @Parameter(names = {"-m", "--mode"}, description = "Type of queries to execute: FIXED, RANDOM")
        public BenchmarkRunner.ExecutionMode executionMode = BenchmarkRunner.ExecutionMode.FIXED;

        @Parameter(names = {"-r", "--num-repetition"}, description = "Number of repetition to execute.", arity = 1)
        public Integer repetition;

        @Parameter(names = {"-c", "--concurrency"}, description = "Number of concurrent threads.", arity = 1)
        public Integer concurrency;

        @Parameter(names = {"--host"}, description = "Remote host.", arity = 1)
        public String host;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name to load the data", required = false, arity = 1)
        public String dbName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", required = false, arity = 1)
        public int limit;

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", required = false, arity = 0)
        public boolean count;

        @Parameter(names = {"-f", "--file"}, description = "File path to load queries", required = false, arity = 1)
        public String queryFile;

        @Parameter(names = {"--delay"}, description = "Delay between each sampler thread.", required = false, arity = 1)
        public Integer delay;

    }

    /**
     * Variant command
     */
    @Parameters(commandNames = {"variant"}, commandDescription = "Benchmark read operations over variants")
    public class VariantBenchmarkCommandOptions extends GenericBenchmarkOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-q", "--query"}, description = "Query Ids to execute for FIXED mode (Default All) OR Query pattern to execute for Random mode e.g. gene,ct(30);region(3)", arity = 1)
        public String query;

        @DynamicParameter(names = {"-B", "--baseQuery"}, description = "Overwrite baseQuery options from file, comma separated, ie. -Blimit=1000", hidden = false)
        public Map<String, String> baseQuery = new HashMap<>();
    }

    /**
     * Alignment command
     */
    @Parameters(commandNames = {"alignment"}, commandDescription = "[PENDING]")
    public class AlignmentBenchmarkCommandOptions extends GenericBenchmarkOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;
    }

    public JCommander getJCommander() {
        return this.jCommander;
    }


}
