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


    @Parameters(commandNames = {"index-alignments"}, commandDescription = "Index alignment file")
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

        @ParametersDelegate
        public GeneralCliOptions.QueryCommandOptions commonQueryOptions = queryCommandOptions;

        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000",
                required = false)
        public String region;

        @Parameter(names = {"--region-file"}, description = "GFF File with regions", required = false)
        public String regionFile;

        @Parameter(names = {"-g", "--gene"}, description = "CSV list of genes", required = false)
        public String gene;

        @Parameter(names = {"-a", "--alias"}, description = "File unique ID.", required = false, arity = 1)
        public String fileId;

        @Parameter(names = {"--file-path"}, description = "", required = false, arity = 1)
        public String filePath;

        @Parameter(names = {"--include-coverage"}, description = " [CSV]", required = false)
        public boolean coverage = false;

        @Parameter(names = {"-H", "--histogram"}, description = " ", required = false, arity = 1)
        public boolean histogram = false;

        @Parameter(names = {"--view-as-pairs"}, description = " ", required = false)
        public boolean asPairs;

        @Parameter(names = {"--process-differences"}, description = " ", required = false)
        public boolean processDifferences;

        @Parameter(names = {"-S", "--stats-filter"}, description = " [CSV]", required = false)
        public List<String> stats = new LinkedList<>();

    }

    @Parameters(commandNames = {"coverage"}, commandDescription = "Calculate and query coverage of indexed alignments")
    public class CoverageAlignmentsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-i", "--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000")
        public String region;

        @Parameter(names = {"-w", "--window-size"}, description = "Window size f the coverage", arity = 1)
        public int windowSize = 50;

        @Parameter(names = {"--create"}, description = "Creates the BigWig coverage file, even if it already exists")
        public boolean create;

    }
}
