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
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
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

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
//            case "stats-run":
//                statsRun();
//                break;
//            case "flagstats-run":
//                flagStatsRun();
//                break;
//            case "fastqcmetrics-run":
//                fastQcMetricsRun();
//                break;
//            case "hsmetrics-run":
//                hsMetricsRun();
//                break;
            case "coverage-run":
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

        AlignmentStorageManager alignmentManager = new AlignmentStorageManager(catalogManager, storageEngineFactory, alignmentCommandOptions.internalJobOptions.jobId);

        alignmentManager.index(cliOptions.study, cliOptions.file, cliOptions.overwrite, cliOptions.outdir, cliOptions.commonOptions.token);
    }


//    private void query() throws InterruptedException, CatalogException, IOException {
//        ObjectMap objectMap = new ObjectMap();
//        objectMap.putIfNotNull("sid", alignmentCommandOptions.queryAlignmentCommandOptions.commonOptions.token);
//        objectMap.putIfNotNull("study", alignmentCommandOptions.queryAlignmentCommandOptions.study);
//        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.REGION.key(), alignmentCommandOptions.queryAlignmentCommandOptions.region);
//        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.MIN_MAPQ.key(),
//                alignmentCommandOptions.queryAlignmentCommandOptions.minMappingQuality);
//        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.CONTAINED.key(),
//                alignmentCommandOptions.queryAlignmentCommandOptions.contained);
//        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.MD_FIELD.key(),
//                alignmentCommandOptions.queryAlignmentCommandOptions.mdField);
//        objectMap.putIfNotNull(AlignmentDBAdaptor.QueryParams.BIN_QUALITIES.key(),
//                alignmentCommandOptions.queryAlignmentCommandOptions.binQualities);
//        objectMap.putIfNotNull(QueryOptions.LIMIT, alignmentCommandOptions.queryAlignmentCommandOptions.limit);
//        objectMap.putIfNotNull(QueryOptions.SKIP, alignmentCommandOptions.queryAlignmentCommandOptions.skip);
//        objectMap.putIfNotNull(QueryOptions.COUNT, alignmentCommandOptions.queryAlignmentCommandOptions.count);
//
//        OpenCGAClient openCGAClient = new OpenCGAClient(clientConfiguration);
//        RestResponse<ReadAlignment> alignments = openCGAClient.getAlignmentClient()
//                .query(alignmentCommandOptions.queryAlignmentCommandOptions.fileId, objectMap);
//
//        for (ReadAlignment readAlignment : alignments.allResults()) {
//            System.out.println(readAlignment);
//        }
//    }

    private void qcRun() throws ToolException {
        AlignmentCommandOptions.QcAlignmentCommandOptions cliOptions = alignmentCommandOptions.qcAlignmentCommandOptions;

        ObjectMap params = new AlignmentQcParams(
                cliOptions.bamFile,
                cliOptions.bedFile,
                cliOptions.dictFile,
                cliOptions.skip,
                cliOptions.overwrite,
                cliOptions.outdir
        ).toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(AlignmentQcAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
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

        toolRunner.execute(AlignmentGeneCoverageStatsAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

//    private void statsRun() throws ToolException {
//        AlignmentCommandOptions.StatsAlignmentCommandOptions cliOptions = alignmentCommandOptions.statsAlignmentCommandOptions;
//
//        ObjectMap params = new AlignmentStatsParams(
//                cliOptions.file,
//                cliOptions.outdir
//        ).toObjectMap(cliOptions.commonOptions.params)
//                .append(ParamConstants.STUDY_PARAM, cliOptions.study);
//
//        toolRunner.execute(AlignmentStatsAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
//    }
//
//    private void flagStatsRun() throws ToolException {
//        AlignmentCommandOptions.FlagStatsAlignmentCommandOptions cliOptions = alignmentCommandOptions.flagStatsAlignmentCommandOptions;
//
//        ObjectMap params = new AlignmentFlagStatsParams(
//                cliOptions.file,
//                cliOptions.outdir
//        ).toObjectMap(cliOptions.commonOptions.params)
//                .append(ParamConstants.STUDY_PARAM, cliOptions.study);
//
//        toolRunner.execute(AlignmentFlagStatsAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
//    }
//
//    private void fastQcMetricsRun() throws ToolException {
//        AlignmentCommandOptions.FastQcMetricsAlignmentCommandOptions cliOptions = alignmentCommandOptions
//                .fastQcMetricsAlignmentCommandOptions;
//
//        ObjectMap params = new AlignmentFastQcMetricsParams(
//                cliOptions.file,
//                cliOptions.outdir
//        ).toObjectMap(cliOptions.commonOptions.params)
//                .append(ParamConstants.STUDY_PARAM, cliOptions.study);
//
//        toolRunner.execute(AlignmentFastQcMetricsAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
//    }
//
//    private void hsMetricsRun() throws ToolException {
//        AlignmentCommandOptions.HsMetricsAlignmentCommandOptions cliOptions = alignmentCommandOptions.hsMetricsAlignmentCommandOptions;
//
//        ObjectMap params = new AlignmentHsMetricsParams(
//                cliOptions.bamFile,
//                cliOptions.bedFile,
//                cliOptions.dictFile,
//                cliOptions.outdir
//        ).toObjectMap(cliOptions.commonOptions.params)
//                .append(ParamConstants.STUDY_PARAM, cliOptions.study);
//
//        toolRunner.execute(AlignmentHsMetricsAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
//    }

    private void coverageRun() throws ToolException {
        AlignmentCommandOptions.CoverageAlignmentCommandOptions cliOptions = alignmentCommandOptions.coverageAlignmentCommandOptions;

        Map<String, String> deeptoolsParams = new HashMap<>();
        deeptoolsParams.put("b", String.valueOf(cliOptions.file));
        deeptoolsParams.put("o", new File(cliOptions.file).getName() + ".bw");
        deeptoolsParams.put("binSize", String.valueOf(cliOptions.windowSize));

        ObjectMap params = new DeeptoolsWrapperParams(
                "bamCoverage",
                cliOptions.outdir,
                deeptoolsParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(DeeptoolsWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
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

        toolRunner.execute(BwaWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
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

        toolRunner.execute(SamtoolsWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    // Deeptools

    private void deeptools() throws Exception {
        AlignmentCommandOptions.DeeptoolsCommandOptions cliOptions = alignmentCommandOptions.deeptoolsCommandOptions;

        ObjectMap params = new DeeptoolsWrapperParams(
                cliOptions.command,
                cliOptions.outdir,
                cliOptions.deeptoolsParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(DeeptoolsWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    // FastQC

    private void fastqc() throws Exception {
        AlignmentCommandOptions.FastqcCommandOptions cliOptions = alignmentCommandOptions.fastqcCommandOptions;

        ObjectMap params = new FastqcWrapperParams(
                cliOptions.inputFile,
                cliOptions.outdir,
                cliOptions.fastqcParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(FastqcWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    // Picard

    private void picard() throws Exception {
        AlignmentCommandOptions.PicardCommandOptions cliOptions = alignmentCommandOptions.picardCommandOptions;

        ObjectMap params = new PicardWrapperParams(
                cliOptions.command,
                cliOptions.outdir,
                cliOptions.picardParams)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(PicardWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    //-------------------------------------------------------------------------
    // M I S C E L A N E O U S     M E T H O D S
    //-------------------------------------------------------------------------

    private void addParam(Map<String, String> map, String key, Object value) {
        if (value == null) {
            return;
        }

        if (value instanceof String) {
            if (!((String) value).isEmpty()) {
                map.put(key, (String) value);
            }
        } else if (value instanceof Integer) {
            map.put(key, Integer.toString((int) value));
        } else if (value instanceof Boolean) {
            map.put(key, Boolean.toString((boolean) value));
        } else {
            throw new UnsupportedOperationException();
        }
    }

}
