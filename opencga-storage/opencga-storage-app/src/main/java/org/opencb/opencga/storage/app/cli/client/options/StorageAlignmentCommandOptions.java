/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.opencga.storage.app.cli.OptionsParser;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by imedina on 22/01/17.
 */
public class StorageAlignmentCommandOptions {

    public IndexAlignmentsCommandOptions indexAlignmentsCommandOptions;
    public QueryAlignmentsCommandOptions queryAlignmentsCommandOptions;

    public JCommander jCommander;
    public OptionsParser.CommonOptions commonCommandOptions;
    public OptionsParser.IndexCommandOptions indexCommandOptions;
    public OptionsParser.QueryCommandOptions queryCommandOptions;

    public StorageAlignmentCommandOptions(OptionsParser.CommonOptions commonOptions, OptionsParser.IndexCommandOptions indexCommandOptions,
                                          OptionsParser.QueryCommandOptions queryCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonOptions;
        this.indexCommandOptions  = indexCommandOptions;
        this.queryCommandOptions = queryCommandOptions;
        this.jCommander = jCommander;

        this.indexAlignmentsCommandOptions = new IndexAlignmentsCommandOptions();
        this.queryAlignmentsCommandOptions = new QueryAlignmentsCommandOptions();
    }


    @Parameters(commandNames = {"index-alignments"}, commandDescription = "Index alignment file")
    public class IndexAlignmentsCommandOptions {

        @ParametersDelegate
        public OptionsParser.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public OptionsParser.IndexCommandOptions commonIndexOptions = indexCommandOptions;


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
        public OptionsParser.CommonOptions commonOptions = commonCommandOptions;

        @ParametersDelegate
        public OptionsParser.QueryCommandOptions commonQueryOptions = queryCommandOptions;


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
}
