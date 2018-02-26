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

package org.opencb.opencga.storage.app.cli.client.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.storage.app.cli.GeneralCliOptions;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by imedina on 22/01/17.
 */
@Parameters(commandNames = {"alignment"}, commandDescription = "Alignment management.")
public class StorageAlignmentCommandOptions {

    public IndexAlignmentsCommandOptions indexAlignmentsCommandOptions;
    public QueryAlignmentsCommandOptions queryAlignmentsCommandOptions;
    public CoverageAlignmentsCommandOptions coverageAlignmentsCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonOptions commonCommandOptions;
    public GeneralCliOptions.IndexCommandOptions indexCommandOptions;
    public GeneralCliOptions.QueryCommandOptions queryCommandOptions;

    public StorageAlignmentCommandOptions(GeneralCliOptions.CommonOptions commonOptions, GeneralCliOptions.IndexCommandOptions indexCommandOptions,
                                          GeneralCliOptions.QueryCommandOptions queryCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonOptions;
        this.indexCommandOptions  = indexCommandOptions;
        this.queryCommandOptions = queryCommandOptions;
        this.jCommander = jCommander;

        this.indexAlignmentsCommandOptions = new IndexAlignmentsCommandOptions();
        this.queryAlignmentsCommandOptions = new QueryAlignmentsCommandOptions();
        this.coverageAlignmentsCommandOptions = new CoverageAlignmentsCommandOptions();
    }


    @Parameters(commandNames = {"index"}, commandDescription = "Create the BAI file")
    public class IndexAlignmentsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public GeneralCliOptions.IndexCommandOptions commonIndexOptions = indexCommandOptions;

        @Parameter(names = {"--transform"}, description = "If present it only runs the transform stage, no load is executed")
        public boolean transform = false;

        @Parameter(names = {"--load"}, description = "If present only the load stage is executed, transformation is skipped")
        public boolean load = false;

        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        public String fileId;

        @Parameter(names = "--calculate-coverage", description = "Calculate coverage while indexing")
        public boolean calculateCoverage = true;

        @Parameter(names = "--mean-coverage", description = "Specify the chunk sizes to calculate average coverage. Only works if flag " +
                "\"--calculate-coverage\" is also given. Please specify chunksizes as CSV: --mean-coverage 200,400", required = false)
        public List<String> meanCoverage;
    }


    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed alignments")
    public class QueryAlignmentsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"--mode"}, description = "Whether to use 'local' or 'gRPC' server mode", arity = 1)
        public String mode = "local";

        @Parameter(names = {"--server-url"}, description = "REST or gRPC server host and port", arity = 1)
        public String host = "localhost:9091";

        @Parameter(names = {"--file"}, description = "Path to the BAM or CRAM file", required = true, arity = 1)
        public String filePath;

        @Parameter(names = {"-o", "--output"}, description = "Output file. [STDOUT]", arity = 1)
        public String output;

        @Parameter(names = {"--of", "--output-format"}, description = "Output file. [STDOUT]", arity = 1)
        public String outputFormat = "GA4GH";

        @Parameter(names = {"-r", "--region"}, required = true, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000")
        public String region;

        @Parameter(names = {"-g", "--gene"}, description = "CSV list of genes")
        public String gene;

        @Parameter(names = {"--min-mapq"}, description = "Window size")
        public Integer minMapq;

        @Parameter(names = {"--max-nm"}, description = "Window size")
        public Integer maxNm;

        @Parameter(names = {"--max-nh"}, description = "Window size")
        public Integer maxNH;

        @Parameter(names = {"--properly-paired"}, description = "Window size")
        public boolean properlyPaired;

        @Parameter(names = {"--max-insert-size"}, description = "Window size")
        public Integer maxInsertSize;

        @Parameter(names = {"--skip-unmapped"}, description = "Window size")
        public boolean skipUnmapped;

        @Parameter(names = {"--skip-duplicated"}, description = "Window size")
        public boolean skipDuplicated;

        @Parameter(names = {"--contained"}, description = "Window size")
        public boolean contained;

        @Parameter(names = {"--md-field"}, description = "Window size")
        public boolean mdField;

        @Parameter(names = {"--bin-qualities"}, description = "Window size")
        public boolean binQualities;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements", arity = 1)
        public int limit;

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", arity = 0)
        public boolean count;

//        @Parameter(names = {"-S", "--stats-filter"}, description = " [CSV]", required = false)
//        public List<String> stats = new LinkedList<>();

    }

    @Parameters(commandNames = {"coverage"}, commandDescription = "Return the coverage from the BigWig file if exists")
    public class CoverageAlignmentsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;


        @Parameter(names = {"--file"}, description = "BAM/CRAM file path", required = true, arity = 1)
        public String file;

        @Parameter(names = {"-r", "--region"}, required = true, description = "Comma-separated list of regions 'chr:start-end'")
        public String region;

        @Parameter(names = {"-w", "--window-size"}, description = "Window size")
        public int windowSize = 1;

    }
}
