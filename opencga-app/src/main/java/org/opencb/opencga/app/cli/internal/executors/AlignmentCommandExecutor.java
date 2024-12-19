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

package org.opencb.opencga.app.cli.internal.executors;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.analysis.alignment.AlignmentCoverageAnalysis;
import org.opencb.opencga.analysis.alignment.AlignmentIndexOperation;
import org.opencb.opencga.analysis.alignment.qc.AlignmentGeneCoverageStatsAnalysis;
import org.opencb.opencga.analysis.alignment.qc.AlignmentQcAnalysis;
import org.opencb.opencga.analysis.wrappers.bwa.BwaWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.deeptools.DeeptoolsWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.picard.PicardWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.alignment.*;

import java.nio.file.Paths;
import java.util.Arrays;

import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.BwaCommandOptions.BWA_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.DeeptoolsCommandOptions.DEEPTOOLS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.FastqcCommandOptions.FASTQC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.PicardCommandOptions.PICARD_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.AlignmentCommandOptions.SamtoolsCommandOptions.SAMTOOLS_RUN_COMMAND;

/**
 * Created on 09/05/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class AlignmentCommandExecutor extends InternalCommandExecutor {

    private AlignmentCommandOptions alignmentCommandOptions;
    private String jobId;
    private boolean dryRun;
//    private AlignmentStorageEngine alignmentStorageManager;

    public AlignmentCommandExecutor(AlignmentCommandOptions options) {
        super(options.analysisCommonOptions);
        alignmentCommandOptions = options;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing alignment command line");

        String subCommandString = getParsedSubCommand(alignmentCommandOptions.jCommander);
        configure();

        jobId = alignmentCommandOptions.internalJobOptions.jobId;
        dryRun = alignmentCommandOptions.internalJobOptions.dryRun;

        switch (subCommandString) {
            case "index-run":
                indexRun();
                break;
            case "qc-run":
                qcRun();
                break;
            case "gene-coveratge-stats-run":
                geneCoverageStatsRun();
                break;
            case "coverage-index-run":
                coverageRun();
                break;
            case "delete":
                delete();
                break;
            case BWA_RUN_COMMAND:
                bwa();
                break;
            case SAMTOOLS_RUN_COMMAND:
                samtools();
                break;
            case DEEPTOOLS_RUN_COMMAND:
                deeptools();
                break;
            case FASTQC_RUN_COMMAND:
                fastqc();
                break;
            case PICARD_RUN_COMMAND:
                picard();
                break;
            default:
                logger.error("Subcommand not valid");
                break;

        }
    }

    private void indexRun() throws Exception {
        AlignmentCommandOptions.IndexAlignmentCommandOptions cliOptions = alignmentCommandOptions.indexAlignmentCommandOptions;

        ObjectMap params = new AlignmentIndexParams(
                cliOptions.fileId,
                cliOptions.overwrite
        ).toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(AlignmentIndexOperation.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    private void qcRun() throws ToolException {
        AlignmentCommandOptions.QcAlignmentCommandOptions cliOptions = alignmentCommandOptions.qcAlignmentCommandOptions;

        ObjectMap params = new AlignmentQcParams(
                cliOptions.bamFile,
                cliOptions.skip,
                cliOptions.overwrite,
                cliOptions.outdir
        ).toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(AlignmentQcAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    private void geneCoverageStatsRun() throws ToolException {
        AlignmentCommandOptions.GeneCoverageStatsAlignmentCommandOptions cliOptions = alignmentCommandOptions
                .geneCoverageStatsAlignmentCommandOptions;

        ObjectMap params = new AlignmentGeneCoverageStatsParams(
                cliOptions.bamFile,
                Arrays.asList(cliOptions.genes.split(",")),
                cliOptions.outdir
        ).toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(AlignmentGeneCoverageStatsAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    private void coverageRun() throws ToolException {
        AlignmentCommandOptions.CoverageAlignmentCommandOptions cliOptions = alignmentCommandOptions.coverageAlignmentCommandOptions;

        ObjectMap params = new CoverageIndexParams(
                cliOptions.bamFileId,
                cliOptions.windowSize,
                cliOptions.overwrite
        ).toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(AlignmentCoverageAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    // BWA

    private void bwa() throws Exception {
        AlignmentCommandOptions.BwaCommandOptions cliOptions = alignmentCommandOptions.bwaCommandOptions;

        ObjectMap params = new BwaWrapperParams(
                cliOptions.command,
                cliOptions.fastaFile,
                cliOptions.fastq1File,
                cliOptions.fastq2File,
                cliOptions.outdir,
                cliOptions.bwaParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(BwaWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    // Samtools

    private void samtools() throws Exception {
        AlignmentCommandOptions.SamtoolsCommandOptions cliOptions = alignmentCommandOptions.samtoolsCommandOptions;

        ObjectMap params = new SamtoolsWrapperParams(
                cliOptions.command,
                cliOptions.inputFile,
                cliOptions.outdir,
                cliOptions.samtoolsParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(SamtoolsWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    // Deeptools

    private void deeptools() throws Exception {
        AlignmentCommandOptions.DeeptoolsCommandOptions cliOptions = alignmentCommandOptions.deeptoolsCommandOptions;

        ObjectMap params = new DeeptoolsWrapperParams(
                cliOptions.command,
                cliOptions.outdir,
                cliOptions.deeptoolsParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(DeeptoolsWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    // FastQC

    private void fastqc() throws Exception {
        AlignmentCommandOptions.FastqcCommandOptions cliOptions = alignmentCommandOptions.fastqcCommandOptions;

        ObjectMap params = new FastqcWrapperParams(
                cliOptions.inputFile,
                cliOptions.outdir,
                cliOptions.fastqcParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(FastqcWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }

    // Picard

    private void picard() throws Exception {
        AlignmentCommandOptions.PicardCommandOptions cliOptions = alignmentCommandOptions.picardCommandOptions;

        ObjectMap params = new PicardWrapperParams(
                cliOptions.command,
                cliOptions.outdir,
                cliOptions.picardParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(PicardWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, dryRun, token);
    }
}
