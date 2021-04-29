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

import com.beust.jcommander.*;
import org.opencb.opencga.analysis.wrappers.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.picard.PicardWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.internal.InternalCliOptionsParser;

import java.util.HashMap;
import java.util.Map;

import static org.opencb.opencga.core.api.ParamConstants.*;

/**
 * Created by imedina on 21/11/16.
 */
@Parameters(commandNames = {"alignment"}, commandDescription = "Implement several tools for the genomic alignment analysis")
public class AlignmentCommandOptions {

    public IndexAlignmentCommandOptions indexAlignmentCommandOptions;
    public QueryAlignmentCommandOptions queryAlignmentCommandOptions;
    public QcAlignmentCommandOptions qcAlignmentCommandOptions;
    public GeneCoverageStatsAlignmentCommandOptions geneCoverageStatsAlignmentCommandOptions;
//    public StatsAlignmentCommandOptions statsAlignmentCommandOptions;
//    public FlagStatsAlignmentCommandOptions flagStatsAlignmentCommandOptions;
//    public FastQcMetricsAlignmentCommandOptions fastQcMetricsAlignmentCommandOptions;
//    public HsMetricsAlignmentCommandOptions hsMetricsAlignmentCommandOptions;
    public CoverageAlignmentCommandOptions coverageAlignmentCommandOptions;
    public CoverageQueryAlignmentCommandOptions coverageQueryAlignmentCommandOptions;
    public CoverageRatioAlignmentCommandOptions coverageRatioAlignmentCommandOptions;
    public CoverageStatsAlignmentCommandOptions coverageStatsAlignmentCommandOptions;

    // Wrappers
    public BwaCommandOptions bwaCommandOptions;
    public SamtoolsCommandOptions samtoolsCommandOptions;
    public DeeptoolsCommandOptions deeptoolsCommandOptions;
    public FastqcCommandOptions fastqcCommandOptions;
    public PicardCommandOptions picardCommandOptions;

    public GeneralCliOptions.CommonCommandOptions analysisCommonOptions;
    public final GeneralCliOptions.JobOptions commonJobOptions;
    public final InternalCliOptionsParser.JobOptions internalJobOptions;
    private final Object commonJobOptionsObject;
    private final Object internalJobOptionsObject;
    public JCommander jCommander;

    public AlignmentCommandOptions(GeneralCliOptions.CommonCommandOptions analysisCommonCommandOptions, JCommander jCommander,
                                   boolean withFullJobParams) {
        this.analysisCommonOptions = analysisCommonCommandOptions;
        this.commonJobOptions = new GeneralCliOptions.JobOptions();
        this.internalJobOptions = new InternalCliOptionsParser.JobOptions();
        this.commonJobOptionsObject = withFullJobParams ? commonJobOptions : new Object();
        this.internalJobOptionsObject = withFullJobParams ? new Object() : internalJobOptions;
        this.jCommander = jCommander;

        this.indexAlignmentCommandOptions = new IndexAlignmentCommandOptions();
        this.queryAlignmentCommandOptions = new QueryAlignmentCommandOptions();
        this.qcAlignmentCommandOptions = new QcAlignmentCommandOptions();
        this.geneCoverageStatsAlignmentCommandOptions = new GeneCoverageStatsAlignmentCommandOptions();
//        this.statsAlignmentCommandOptions = new StatsAlignmentCommandOptions();
//        this.flagStatsAlignmentCommandOptions = new FlagStatsAlignmentCommandOptions();
//        this.fastQcMetricsAlignmentCommandOptions = new FastQcMetricsAlignmentCommandOptions();
//        this.hsMetricsAlignmentCommandOptions = new HsMetricsAlignmentCommandOptions();
        this.coverageAlignmentCommandOptions = new CoverageAlignmentCommandOptions();
        this.coverageQueryAlignmentCommandOptions = new CoverageQueryAlignmentCommandOptions();
        this.coverageRatioAlignmentCommandOptions = new CoverageRatioAlignmentCommandOptions();
        this.coverageStatsAlignmentCommandOptions = new CoverageStatsAlignmentCommandOptions();

        this.bwaCommandOptions = new BwaCommandOptions();
        this.samtoolsCommandOptions = new SamtoolsCommandOptions();
        this.deeptoolsCommandOptions = new DeeptoolsCommandOptions();
        this.fastqcCommandOptions = new FastqcCommandOptions();
        this.picardCommandOptions = new PicardCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = ALIGNMENT_INDEX_DESCRIPTION)
    public class IndexAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"--overwrite"}, description = "Overwrite index file", arity = 0)
        public boolean overwrite = false;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"query"}, commandDescription = ALIGNMENT_QUERY_DESCRIPTION)
    public class QueryAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--rpc"}, description = RPC_METHOD_DESCRIPTION, arity = 1)
        public String rpc;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-r", "--region"}, description = REGION_DESCRIPTION)
        public String region;

        @Parameter(names = {"-g", "--gene"}, description = GENE_DESCRIPTION)
        public String gene;

        @Parameter(names = {"--offset"}, description = OFFSET_DESCRIPTION)
        public int offset = Integer.parseInt(OFFSET_DEFAULT);

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

    @Parameters(commandNames = {"qc-run"}, commandDescription = ALIGNMENT_QC_DESCRIPTION)
    public class QcAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--bam-file"}, description = FILE_ID_DESCRIPTION + " for the BAM file", required = true, arity = 1)
        public String bamFile;

        @Parameter(names = {"--bed-file"}, description = FILE_ID_DESCRIPTION + " for the BED file, mandatory to compute hybrid-selection (HS) metrics", required = false, arity = 1)
        public String bedFile;

        @Parameter(names = {"--dict-file"}, description = FILE_ID_DESCRIPTION + " for the dictionary file, mandatory to compute hybrid-selection (Hs) metrics", required = false, arity = 1)
        public String dictFile;

        @Parameter(names = {"--run-samtools-stats"}, description = ALIGNMENT_QC_SAMTOOLS_STATS_DESCRIPTION, required = false, arity = 0)
        public boolean runSamtoolsStats;

        @Parameter(names = {"--run-samtools-flagstats"}, description = ALIGNMENT_QC_SAMTOOLS_FLAG_STATS_DESCRIPTION, required = false, arity = 0)
        public boolean runSamtoolsFlagStats;

        @Parameter(names = {"--run-fastqc"}, description = ALIGNMENT_QC_FASTQC_DESCRIPTION, required = false, arity = 0)
        public boolean runFastQC;

        @Parameter(names = {"--run-hs-metrics"}, description = ALIGNMENT_QC_HS_METRICS_DESCRIPTION, required = false, arity = 0)
        public boolean runHsMetrics;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"stats-run"}, commandDescription = ALIGNMENT_STATS_DESCRIPTION)
    public class StatsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"flagstats-run"}, commandDescription = ALIGNMENT_FLAG_STATS_DESCRIPTION)
    public class FlagStatsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"fastqcmetrics-run"}, commandDescription = ALIGNMENT_FASTQC_METRICS_DESCRIPTION)
    public class FastQcMetricsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"hsmetrics-run"}, commandDescription = ALIGNMENT_HS_METRICS_DESCRIPTION)
    public class HsMetricsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--bam-file"}, description = FILE_ID_DESCRIPTION + " (BAM file)", required = true, arity = 1)
        public String bamFile;

        @Parameter(names = {"--bed-file"}, description = FILE_ID_DESCRIPTION + " (BED file with the interest regions)", required = true, arity = 1)
        public String bedFile;

        @Parameter(names = {"--dict-file"}, description = FILE_ID_DESCRIPTION + " (dictionary file)", required = true, arity = 1)
        public String dictFile;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"gene-coverage-stats-run"}, commandDescription = ALIGNMENT_GENE_COVERAGE_STATS_DESCRIPTION)
    public class GeneCoverageStatsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--bam-file"}, description = FILE_ID_DESCRIPTION + " (BAM file)", required = true, arity = 1)
        public String bamFile;

        @Parameter(names = {"--genes"}, description = GENE_DESCRIPTION, required = true, arity = 1)
        public String genes;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"coverage-run"}, commandDescription = ALIGNMENT_COVERAGE_DESCRIPTION)
    public class CoverageAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"--window-size"}, description = COVERAGE_WINDOW_SIZE_DESCRIPTION, arity = 1)
        public int windowSize = 1;

        @Parameter(names = {"--overwrite"}, description = "Overwrite coverage file", arity = 0)
        public boolean overwrite = false;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    @Parameters(commandNames = {"coverage-query"}, commandDescription = ALIGNMENT_COVERAGE_QUERY_DESCRIPTION)
    public class CoverageQueryAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-r", "--region"}, description = REGION_DESCRIPTION)
        public String region;

        @Parameter(names = {"-g", "--gene"}, description = GENE_DESCRIPTION)
        public String gene;

        @Parameter(names = {"--offset"}, description = OFFSET_DESCRIPTION)
        public int offset = Integer.parseInt(OFFSET_DEFAULT);

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

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

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

        @Parameter(names = {"--offset"}, description = OFFSET_DESCRIPTION)
        public int offset = Integer.parseInt(OFFSET_DEFAULT);

        @Parameter(names = {"--only-exons"}, description = ONLY_EXONS_DESCRIPTION)
        public boolean onlyExons;

        @Parameter(names = {"--window-size"}, description = COVERAGE_WINDOW_SIZE_DESCRIPTION, arity = 1)
        public int windowSize = 1;

        @Parameter(names = {"--split-results"}, description = SPLIT_RESULTS_INTO_REGIONS_DESCRIPTION)
        public boolean splitResults;
    }

    @Parameters(commandNames = {"coverage-stats"}, commandDescription = ALIGNMENT_COVERAGE_STATS_DESCRIPTION)
    public class CoverageStatsAlignmentCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"--file"}, description = FILE_ID_DESCRIPTION, required = true, arity = 1)
        public String file;

        @Parameter(names = {"-g", "--gene"}, description = GENE_DESCRIPTION, required = true)
        public String gene;


        @Parameter(names = {"--coverage-range"}, description = COVERAGE_RANGE_DESCRIPTION, arity = 1)
        public String range;

        @Parameter(names = {"--" + LOW_COVERAGE_REGION_THRESHOLD_PARAM}, description = LOW_COVERAGE_REGION_THRESHOLD_DESCRIPTION, arity = 1)
        public int threshold = Integer.parseInt(LOW_COVERAGE_REGION_THRESHOLD_DEFAULT);
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    // BWA

    @Parameters(commandNames = BwaWrapperAnalysis.ID, commandDescription = BwaWrapperAnalysis.DESCRIPTION)
    public class BwaCommandOptions {
        public static final String BWA_RUN_COMMAND = BwaWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

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

        @Parameter(names = {"--sam-filename"}, description = "SAM file name.")
        public String samFilename;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    // Samtools

    @Parameters(commandNames = SamtoolsWrapperAnalysis.ID, commandDescription = SamtoolsWrapperAnalysis.DESCRIPTION)
    public class SamtoolsCommandOptions {
        public static final String SAMTOOLS_RUN_COMMAND = SamtoolsWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-s", "--study"}, description = STUDY_DESCRIPTION, arity = 1)
        public String study;

        @Parameter(names = {"--command"}, description = SAMTOOLS_COMMAND_DESCRIPTION)
        public String command;

        @Parameter(names = {"--input-file"}, description = INPUT_FILE_DESCRIPTION)
        public String inputFile;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;

        @DynamicParameter(names = {"--samtools-params"}, description = "Samtools parameters e.g.:. --samtools-params F=0x800 --samtools-params remove-dups=true")
        public Map<String, String> samtoolsParams = new HashMap<>();
    }

    // Deeptools

    @Parameters(commandNames = DeeptoolsWrapperAnalysis.ID, commandDescription = DeeptoolsWrapperAnalysis.DESCRIPTION)
    public class DeeptoolsCommandOptions {
        public static final String DEEPTOOLS_RUN_COMMAND = DeeptoolsWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-s", "--study"}, description = "Study [[user@]project:]study.", arity = 1)
        public String study;

        @Parameter(names = {"--command"}, description = "Deeptools command. Valid values: bamCoverage.")
        public String executable;

        @Parameter(names = {"--bam-file"}, description = "BAM file.")
        public String bamFile;

        @DynamicParameter(names = {"--deeptools-params"}, description = "Deeptools parameters e.g.:. --deeptools-params bs=1 --deeptools-params of=bigwig")
        public Map<String, String> deeptoolsParams = new HashMap<>();

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.")
        public String outdir;
    }

    // FastQC

    @Parameters(commandNames = FastqcWrapperAnalysis.ID, commandDescription = FastqcWrapperAnalysis.DESCRIPTION)
    public class FastqcCommandOptions {
        public static final String FASTQC_RUN_COMMAND = FastqcWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-s", "--study"}, description = STUDY_DESCRIPTION, arity = 1)
        public String study;

        @Parameter(names = {"--input-file"}, description = FILE_ID_DESCRIPTION + " (FastQ, SAM or BAM file)", required = true)
        public String inputFile;

        @Parameter(names = {"--contaminants-file"}, description = FILE_ID_DESCRIPTION + " (file which contains the list of" +
                "  contaminants to screen overrepresented sequences against)")
        public String contaminantsFile;

        @Parameter(names = {"--adapters-file"}, description = FILE_ID_DESCRIPTION + " (adapter sequences file)")
        public String adaptersFile;

        @Parameter(names = {"--limits-file"}, description = FILE_ID_DESCRIPTION + " (file which contains a set of criteria"
                + " which will be used to determine the warn/error limits for the various modules)")
        public String limitsFile;

        @DynamicParameter(names = {"--fastqc-params"}, description = "FastQc parameters e.g.:. --fastqc-params kmers=10")
        public Map<String, String> fastqcParams = new HashMap<>();

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;
    }

    // Picard

    @Parameters(commandNames = PicardWrapperAnalysis.ID, commandDescription = PicardWrapperAnalysis.DESCRIPTION)
    public class PicardCommandOptions {
        public static final String PICARD_RUN_COMMAND = PicardWrapperAnalysis.ID + "-run";

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = analysisCommonOptions;

        @ParametersDelegate
        public Object jobOptions = commonJobOptionsObject;

        @ParametersDelegate
        public Object internalJobOptions = internalJobOptionsObject;

        @Parameter(names = {"-s", "--study"}, description = STUDY_DESCRIPTION, arity = 1)
        public String study;

        @Parameter(names = {"--command"}, description = PICARD_COMMAND_DESCRIPTION, required = true)
        public String command;

        @Parameter(names = {"--bam-file"}, description = FILE_ID_DESCRIPTION + " (BAM file)")
        public String bamFile;

        @Parameter(names = {"--bed-file"}, description = FILE_ID_DESCRIPTION + " (BEB file)")
        public String bedFile;

        @Parameter(names = {"--bait-intervals-file"}, description = FILE_ID_DESCRIPTION + " (BAIT file for intervals)")
        public String baitIntervalsFile;

        @Parameter(names = {"--target-intervals-file"}, description = FILE_ID_DESCRIPTION + " (BAIT file for target intervals)")
        public String targetIntervalsFile;

        @Parameter(names = {"--dict-file"}, description = FILE_ID_DESCRIPTION + " (dictionary file)")
        public String dictFile;

        @Parameter(names = {"--ref-seq-file"}, description = FILE_ID_DESCRIPTION + " (reference sequence file)")
        public String refSeqFile;

        @Parameter(names = {"--out-filename"}, description = "Output filename")
        public String outFilename;

        @Parameter(names = {"-o", "--outdir"}, description = OUTPUT_DIRECTORY_DESCRIPTION)
        public String outdir;

        @DynamicParameter(names = {"--pircard-params"}, description = "Picard parameters.")
        public Map<String, String> picardParams = new HashMap<>();
    }
}
