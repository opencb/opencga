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
import org.opencb.opencga.analysis.wrappers.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.SamtoolsWrapperAnalysis;
import org.opencb.opencga.app.cli.GeneralCliOptions;

import static org.opencb.opencga.core.api.ParamConstants.*;

/**
 * Created by imedina on 21/11/16.
 */
@Parameters(commandNames = {"alignment"}, commandDescription = "Implement several tools for the genomic alignment analysis")
public class AlignmentCommandOptions {

    public IndexAlignmentCommandOptions indexAlignmentCommandOptions;
    public QueryAlignmentCommandOptions queryAlignmentCommandOptions;
    public StatsAlignmentCommandOptions statsAlignmentCommandOptions;
    public StatsInfoAlignmentCommandOptions statsInfoAlignmentCommandOptions;
    public StatsQueryAlignmentCommandOptions statsQueryAlignmentCommandOptions;
    public CoverageAlignmentCommandOptions coverageAlignmentCommandOptions;
    public CoverageQueryAlignmentCommandOptions coverageQueryAlignmentCommandOptions;
    public CoverageRatioAlignmentCommandOptions coverageRatioAlignmentCommandOptions;

    // Wrappers
    public BwaCommandOptions bwaCommandOptions;
    public SamtoolsCommandOptions samtoolsCommandOptions;
    public DeeptoolsCommandOptions deeptoolsCommandOptions;

    public GeneralCliOptions.CommonCommandOptions analysisCommonOptions;
    public JCommander jCommander;

    public AlignmentCommandOptions(GeneralCliOptions.CommonCommandOptions analysisCommonCommandOptions, JCommander jCommander) {
        this.analysisCommonOptions = analysisCommonCommandOptions;
        this.jCommander = jCommander;

        this.indexAlignmentCommandOptions = new IndexAlignmentCommandOptions();
        this.queryAlignmentCommandOptions = new QueryAlignmentCommandOptions();
        this.statsAlignmentCommandOptions = new StatsAlignmentCommandOptions();
        this.statsInfoAlignmentCommandOptions = new StatsInfoAlignmentCommandOptions();
        this.statsQueryAlignmentCommandOptions = new StatsQueryAlignmentCommandOptions();
        this.coverageAlignmentCommandOptions = new CoverageAlignmentCommandOptions();
        this.coverageQueryAlignmentCommandOptions = new CoverageQueryAlignmentCommandOptions();
        this.coverageRatioAlignmentCommandOptions = new CoverageRatioAlignmentCommandOptions();

        this.bwaCommandOptions = new BwaCommandOptions();
        this.samtoolsCommandOptions = new SamtoolsCommandOptions();
        this.deeptoolsCommandOptions = new DeeptoolsCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = ALIGNMENT_INDEX_DESCRIPTION)
    public class IndexAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"query"}, commandDescription = ALIGNMENT_QUERY_DESCRIPTION)
    public class QueryAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--rpc"}, description = RPC_METHOD_DESCRIPTION, arity = 1)
        public String rpc;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-r", "--region"}, description = REGION_DESCRIPTION)
        public String region;

        @Parameter(names = {"-g", "--gene"}, description = GENE_DESCRIPTION)
        public String gene;

        @Parameter(names = {"--coding-offset"}, description = CODING_OFFSET_DESCRIPTION)
        public int codingOffset = Integer.parseInt(CODING_OFFSET_DEFAULT);

        @Parameter(names = {"--only-exons"}, description = ONLY_EXONS_DESCRIPTION)
        public boolean onlyExons;

        @Parameter(names = {"--min-mapq"}, description = MINIMUM_MAPPING_QUALITY_DESCRIPTION, arity = 1)
        public int minMappingQuality;

        @Parameter(names = {"--max-num-mismatches"}, description = MAXIMUM_NUMBER_MISMATCHES_DESCRIPTION, arity = 1)
        public int maxNumMismatches;

        @Parameter(names = {"--max-num-hits"}, description = MAXIMUM_NUMBER_HITS_DESCRIPTION, arity = 1)
        public int maxNumHits;

        @Parameter(names = {"--properly-paired"}, description = PROPERLY_PAIRED_DESCRIPTION, arity = 0)
        public boolean properlyPaired;

        @Parameter(names = {"--max-insert-size"}, description = MAXIMUM_INSERT_SIZE_DESCRIPTION, arity = 1)
        public int maxInsertSize;

        @Parameter(names = {"--skip-unmapped"}, description = SKIP_UNMAPPED_DESCRIPTION, arity = 0)
        public boolean skipUnmapped;

        @Parameter(names = {"--skip-duplicated"}, description = SKIP_DUPLICATED_DESCRIPTION, arity = 0)
        public boolean skipDuplicated;

        @Parameter(names = {"--region-contained"}, description = REGION_CONTAINED_DESCRIPTION, arity = 0)
        public boolean contained;

        @Parameter(names = {"--force-md-field"}, description = FORCE_MD_FIELD_DESCRIPTION, arity = 0)
        public boolean forceMDField;

        @Parameter(names = {"--bin-qualities"}, description = BIN_QUALITIES_DESCRIPTION, arity = 0)
        public boolean binQualities;

        @Parameter(names = {"--split-results"}, description = SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION)
        public boolean splitResults;

        @Parameter(names = {"--skip"}, description = SKIP_DESCRIPTION, required = false, arity = 1)
        public int skip;

        @Parameter(names = {"--limit"}, description = LIMIT_DESCRIPTION, required = false, arity = 1)
        public int limit;

        @Parameter(names = {"--count"}, description = COUNT_DESCRIPTION, required = false, arity = 0)
        public boolean count;
    }

    @Parameters(commandNames = {"stats-run"}, commandDescription = ALIGNMENT_STATS_DESCRIPTION)
    public class StatsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"stats-info"}, commandDescription = ALIGNMENT_STATS_INFO_DESCRIPTION)
    public class StatsInfoAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"stats-query"}, commandDescription = ALIGNMENT_STATS_QUERY_DESCRIPTION)
    public class StatsQueryAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--raw-total-sequences"}, description = RAW_TOTAL_SEQUENCES_DESCRIPTION)
        public String rawTotalSequences;

        @Parameter(names = {"--filtered-sequences"}, description = FILTERED_SEQUENCES_DESCRIPTION)
        public String filteredSequences;

        @Parameter(names = {"--reads-mapped"}, description = READS_MAPPED_DESCRIPTION)
        public String readsMapped;

        @Parameter(names = {"--reads-mapped-and-paired"}, description = READS_MAPPED_AND_PAIRED_DESCRIPTION)
        public String readsMappedAndPaired;

        @Parameter(names = {"--reads-unmapped"}, description = READS_UNMAPPED_DESCRIPTION)
        public String readsUnmapped;

        @Parameter(names = {"--reads-properly-paired"}, description = READS_PROPERLY_PAIRED_DESCRIPTION)
        public String readsProperlyPaired;

        @Parameter(names = {"--reads-paired"}, description = READS_PAIRED_DESCRIPTION)
        public String readsPaired;

        @Parameter(names = {"--reads-duplicated"}, description = READS_DUPLICATED_DESCRIPTION)
        public String readsDuplicated;

        @Parameter(names = {"--reads-mq0"}, description = READS_MQ0_DESCRIPTION)
        public String readsMQ0;

        @Parameter(names = {"--reads-qc-failed"}, description = READS_QC_FAILED_DESCRIPTION)
        public String readsQCFailed;

        @Parameter(names = {"--non-primary-alignments"}, description = NON_PRIMARY_ALIGNMENTS_DESCRIPTION)
        public String nonPrimaryAlignments;

        @Parameter(names = {"--mismatches"}, description = MISMATCHES_DESCRIPTION)
        public String mismatches;

        @Parameter(names = {"--error-rate"}, description = ERROR_RATE_DESCRIPTION)
        public String errorRate;

        @Parameter(names = {"--average-length"}, description = AVERAGE_LENGTH_DESCRIPTION)
        public String averageLength;

        @Parameter(names = {"--average-first-fragment-length"}, description = AVERAGE_FIRST_FRAGMENT_LENGTH_DESCRIPTION)
        public String averageFirstFragmentLength;

        @Parameter(names = {"--average-last-fragment-length"}, description = AVERAGE_LAST_FRAGMENT_LENGTH_DESCRIPTION)
        public String averageLastFragmentLength;

        @Parameter(names = {"--average-quality"}, description = AVERAGE_QUALITY_DESCRIPTION)
        public String averageQuality;

        @Parameter(names = {"--insert-size-average"}, description = INSERT_SIZE_AVERAGE_DESCRIPTION)
        public String insertSizeAverage;

        @Parameter(names = {"--insert-size-standard-devitation"}, description = INSERT_SIZE_STANDARD_DEVIATION_DESCRIPTION)
        public String insertSizeStandardDeviation;

        @Parameter(names = {"--pairs-with-other-orientation"}, description = PAIRS_WITH_OTHER_ORIENTATION_DESCRIPTION)
        public String pairsWithOtherOrientation;

        @Parameter(names = {"--pairs-on-different-chromosomes"}, description = PAIRS_ON_DIFFERENT_CHROMOSOMES_DESCRIPTION)
        public String pairsOnDifferentChromosomes;

        @Parameter(names = {"--percentage-of-properly-paired-reads"}, description = PERCENTAGE_OF_PROPERLY_PAIRED_READS_DESCRIPTION)
        public String percentageOfProperlyPairedReads;
    }

    @Parameters(commandNames = {"coverage-run"}, commandDescription = ALIGNMENT_COVERAGE_DESCRIPTION)
    public class CoverageAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"--window-size"}, description = COVERAGE_WINDOW_SIZE_DESCRIPTION, arity = 1)
        public int windowSize = 1;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"coverage-query"}, commandDescription = ALIGNMENT_COVERAGE_QUERY_DESCRIPTION)
    public class CoverageQueryAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-r", "--region"}, description = REGION_DESCRIPTION)
        public String region;

        @Parameter(names = {"-g", "--gene"}, description = GENE_DESCRIPTION)
        public String gene;

        @Parameter(names = {"--coding-offset"}, description = CODING_OFFSET_DESCRIPTION)
        public int codingOffset = Integer.parseInt(CODING_OFFSET_DEFAULT);

        @Parameter(names = {"--only-exons"}, description = ONLY_EXONS_DESCRIPTION)
        public boolean onlyExons;

        @Parameter(names = {"--coverage-range"}, description = COVERAGE_RANGE_DESCRIPTION, arity = 1)
        public String range;

        @Parameter(names = {"--window-size"}, description = COVERAGE_WINDOW_SIZE_DESCRIPTION, arity = 1)
        public int windowSize = 1;

        @Parameter(names = {"--split-results"}, description = SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION)
        public boolean splitResults;
    }

    @Parameters(commandNames = {"coverage-ratio"}, commandDescription = ALIGNMENT_COVERAGE_RATIO_DESCRIPTION)
    public class CoverageRatioAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"--file1"}, description = FILE_ID_1_DESCRIPTION, required = true, arity = 1)
        public String file1;

        @Parameter(names = {"--file2"}, description = FILE_ID_2_DESCRIPTION, required = true, arity = 1)
        public String file2;

        @Parameter(names = {"--skip-log2"}, description = SKIP_LOG2_DESCRIPTION)
        public boolean skipLog2;

        @Parameter(names = {"-r", "--region"}, description = REGION_DESCRIPTION)
        public String region;

        @Parameter(names = {"-g", "--gene"}, description = GENE_DESCRIPTION)
        public String gene;

        @Parameter(names = {"--gene-offset"}, description = CODING_OFFSET_DESCRIPTION)
        public int codingOffset = Integer.parseInt(CODING_OFFSET_DEFAULT);

        @Parameter(names = {"--only-exons"}, description = ONLY_EXONS_DESCRIPTION)
        public boolean onlyExons;

        @Parameter(names = {"--window-size"}, description = COVERAGE_WINDOW_SIZE_DESCRIPTION, arity = 1)
        public int windowSize = 1;

        @Parameter(names = {"--split-results"}, description = SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION)
        public boolean splitResults;
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    // BWA

    @Parameters(commandNames = BwaWrapperAnalysis.ID, commandDescription = BwaWrapperAnalysis.DESCRIPTION)
    public class BwaCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study.", arity = 1)
        public String study;

        @Parameter(names = {"--command"}, description = "BWA comamnd. Valid values: index, mem.")
        public String command;

        @Parameter(names = {"--fasta-file"}, description = "Fasta file.")
        public String fastaFile;

        @Parameter(names = {"--index-base-file"}, description = "Index base file.")
        public String indexBaseFile;

        @Parameter(names = {"--fastq1-file"}, description = "FastQ #1 file.")
        public String fastq1File;

        @Parameter(names = {"--fastq2-file"}, description = "FastQ #2 file.")
        public String fastq2File;

        @Parameter(names = {"--sam-file"}, description = "SAM file.")
        public String samFile;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    // Samtools

    @Parameters(commandNames = SamtoolsWrapperAnalysis.ID, commandDescription = SamtoolsWrapperAnalysis.DESCRIPTION)
    public class SamtoolsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"-s", "--study"}, description = STUDY_DESCRIPTION, arity = 1)
        public String study;

        @Parameter(names = {"--command"}, description = SAMTOOLS_COMMAND_DESCRIPTION)
        public String command;

        @Parameter(names = {"--input-file"}, description = INPUT_FILE_DESCRIPTION)
        public String inputFile;

        @Parameter(names = {"--output-filename"}, description = OUTPUT_FILENAME_DESCRIPTION)
        public String outputFilename;

        @Parameter(names = {"--reference-file"}, description = REFERENCE_FILE_DESCRIPTION)
        public String referenceFile;

        @Parameter(names = {"--read-group-file"}, description = READ_GROUP_FILE_DESCRIPTION)
        public String readGroupFile;

        @Parameter(names = {"--bed-file"}, description = BED_FILE_DESCRIPTION)
        public String bedFile;

        @Parameter(names = {"--ref-seq-file"}, description = REF_SEQ_FILE_DESCRIPTION)
        public String refSeqFile;

        @Parameter(names = {"--reference-names-file"}, description = REFERENCE_NAMES_DESCRIPTION)
        public String referenceNamesFile;

        @Parameter(names = {"--target-region-file"}, description = TARGET_REGION_DESCRIPTION)
        public String targetRegionFile;

        @Parameter(names = {"--reads-not-selected-filename"}, description = READS_NOT_SELECTED_FILENAME_DESCRIPTION)
        public String readsNotSelectedFilename;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    // Deeptools

    @Parameters(commandNames = DeeptoolsWrapperAnalysis.ID, commandDescription = DeeptoolsWrapperAnalysis.DESCRIPTION)
    public class DeeptoolsCommandOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study.", arity = 1)
        public String study;

        @Parameter(names = {"--command"}, description = "Deeptools command. Valid values: bamCoverage.")
        public String executable;

        @Parameter(names = {"--bam-file"}, description = "BAM file.")
        public String bamFile;

        @Parameter(names = {"--coverage-file"}, description = "Coverage file.")
        public String coverageFile;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }
}
