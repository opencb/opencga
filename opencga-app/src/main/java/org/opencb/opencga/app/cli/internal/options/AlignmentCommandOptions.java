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

package org.opencb.opencga.app.cli.internal.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;

/**
 * Created by imedina on 21/11/16.
 */
@Parameters(commandNames = {"alignment"}, commandDescription = "Implement several tools for the genomic alignment analysis")
public class AlignmentCommandOptions {

    public IndexAlignmentCommandOptions indexAlignmentCommandOptions;
    public QueryAlignmentCommandOptions queryAlignmentCommandOptions;
    public StatsAlignmentCommandOptions statsAlignmentCommandOptions;
    public CoverageAlignmentCommandOptions coverageAlignmentCommandOptions;

    public GeneralCliOptions.CommonCommandOptions analysisCommonOptions;
    public JCommander jCommander;

    public AlignmentCommandOptions(GeneralCliOptions.CommonCommandOptions analysisCommonCommandOptions, JCommander jCommander) {
        this.analysisCommonOptions = analysisCommonCommandOptions;
        this.jCommander = jCommander;

        this.indexAlignmentCommandOptions = new IndexAlignmentCommandOptions();
        this.queryAlignmentCommandOptions = new QueryAlignmentCommandOptions();
        this.statsAlignmentCommandOptions = new StatsAlignmentCommandOptions();
        this.coverageAlignmentCommandOptions = new CoverageAlignmentCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index alignment file")
    public class IndexAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;


        @Parameter(names = {"-i", "--file"}, description = "Unique ID for the file", required = true, arity = 1)
        public String fileId;

        @Parameter(names = "--skip-coverage", description = "Skip calculating the coverage after creating the .bai file")
        public boolean skipCoverage = false;

        @Parameter(names = "--skip-stats", description = "Skip calculating the bam stats after creating the .bai file")
        public boolean skipStats = false;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1)
        public String outdirId;
    }

    @Parameters(commandNames = {"query"}, commandDescription = "Search over indexed alignments")
    public class QueryAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--rpc"}, description = "RPC method used: {auto, GRPC, REST}. When auto, it will first try with GRPC and if "
                + "that does not work, it will try with REST", required = false, arity = 1)
        public String rpc;

        @Parameter(names = {"--file"}, description = "Id of the alignment file in catalog", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"--min-mapq"}, description = "Minimum mapping quality", arity = 1)
        public int minMappingQuality;

        @Parameter(names = {"--contained"}, description = "Set flag to select just the alignments completely contained within the "
                + "boundaries of the region", arity = 0)
        public boolean contained;

        @Parameter(names = {"--md-field"}, description = "Force SAM MD optional field to be set with the alignments", arity = 0)
        public boolean mdField;

        @Parameter(names = {"--bin-qualities"}, description = "Compress the nucleotide qualities by using 8 quality levels "
                + "(there will be loss of information)", arity = 0)
        public boolean binQualities;

        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000")
        public String region;

        @Parameter(names = {"--skip"}, description = "Skip some number of elements.", required = false, arity = 1)
        public int skip;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", required = false, arity = 1)
        public int limit;

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", required = false, arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"stats"}, commandDescription = "Obtain the global stats of an alignment")
    public class StatsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;


        @Parameter(names = {"--file"}, description = "Id of the alignment file in catalog", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"--min-mapq"}, description = "Minimum mapping quality", arity = 1)
        public Integer minMappingQuality;

        @Parameter(names = {"--contained"}, description = "Set flag to select just the alignments completely contained within the "
                + "boundaries of the region", arity = 0)
        public boolean contained;

        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000")
        public String region;
    }

    @Parameters(commandNames = {"coverage"}, commandDescription = "Obtain the coverage of an alignment")
    public class CoverageAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;


        @Parameter(names = {"--file"}, description = "Id of the alignment file in catalog", required = true, arity = 1)
        public String fileId;

        @Parameter(names = {"--min-mapq"}, description = "Minimum mapping quality", arity = 1)
        public Integer minMappingQuality;

        @Parameter(names = {"--contained"}, description = "Set flag to select just the alignments completely contained within the "
                + "boundaries of the region", arity = 0)
        public boolean contained;
        @Parameter(names = {"-r", "--region"}, description = "CSV list of regions: {chr}[:{start}-{end}]. example: 2,3:1000000-2000000")
        public String region;

    }
}
