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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.family.qc.FamilyQcAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcAnalysis;
import org.opencb.opencga.analysis.sample.qc.SampleQcAnalysis;
import org.opencb.opencga.analysis.variant.VariantExportTool;
import org.opencb.opencga.analysis.variant.genomePlot.GenomePlotAnalysis;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.inferredSex.InferredSexAnalysis;
import org.opencb.opencga.analysis.variant.julie.JulieTool;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.mendelianError.MendelianErrorAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.operations.*;
import org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis;
import org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis;
import org.opencb.opencga.analysis.variant.samples.SampleVariantFilterAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.analysis.wrappers.exomiser.ExomiserWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.gatk.GatkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.plink.PlinkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.rvtests.RvtestsWrapperAnalysis;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.exceptions.AnalysisExecutionException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ExomiserWrapperParams;
import org.opencb.opencga.core.models.common.mixins.GenericRecordAvroJsonMixin;
import org.opencb.opencga.core.models.operations.variant.*;
import org.opencb.opencga.core.models.variant.*;
import org.opencb.opencga.core.tools.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.ExomiserAnalysisCommandOptions.EXOMISER_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyQcCommandOptions.FAMILY_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GatkCommandOptions.GATK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GenomePlotCommandOptions.GENOME_PLOT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.IndividualQcCommandOptions.INDIVIDUAL_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.InferredSexCommandOptions.INFERRED_SEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.KnockoutCommandOptions.KNOCKOUT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MendelianErrorCommandOptions.MENDELIAN_ERROR_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MutationalSignatureCommandOptions.MUTATIONAL_SIGNATURE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RelatednessCommandOptions.RELATEDNESS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTESTS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleQcCommandOptions.SAMPLE_QC_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantAnnotateCommandOptions.ANNOTATION_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantExportCommandOptions.EXPORT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantExportStatsCommandOptions.STATS_EXPORT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantIndexCommandOptions.INDEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSamplesFilterCommandOptions.SAMPLE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantScoreDeleteCommandOptions.SCORE_DELETE_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexCommandOptions.SECONDARY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantSecondaryIndexDeleteCommandOptions.SECONDARY_INDEX_DELETE_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantStatsCommandOptions.STATS_RUN_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateCommandOptions.AGGREGATE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.AggregateFamilyCommandOptions.AGGREGATE_FAMILY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantDeleteCommandOptions.VARIANT_DELETE_COMMAND;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantInternalCommandExecutor extends InternalCommandExecutor {

    //    private AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions;
    private VariantCommandOptions variantCommandOptions;
    private String jobId;

    public VariantInternalCommandExecutor(VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonCommandOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

//        String subCommandString = variantCommandOptions.getParsedSubCommand();
        String subCommandString = getParsedSubCommand(variantCommandOptions.jCommander);
        configure();

        jobId = variantCommandOptions.internalJobOptions.jobId;

        switch (subCommandString) {
            case VARIANT_DELETE_COMMAND:
                fileDelete();
                break;
            case EXPORT_RUN_COMMAND:
                export();
                break;
            case "query":
                query();
                break;
            case STATS_EXPORT_RUN_COMMAND:
                exportFrequencies();
                break;
            case "import":
                importData();
                break;
            case INDEX_RUN_COMMAND:
                index();
                break;
            case SECONDARY_INDEX_COMMAND:
                secondaryIndex();
                break;
            case SECONDARY_INDEX_DELETE_COMMAND:
                secondaryIndexRemove();
                break;
            case STATS_RUN_COMMAND:
                statsRun();
                break;
            case SCORE_INDEX_COMMAND:
                scoreLoad();
                break;
            case SCORE_DELETE_COMMAND:
                scoreRemove();
                break;
            case SAMPLE_INDEX_COMMAND:
                sampleIndex();
                break;
            case FAMILY_INDEX_COMMAND:
                familyIndex();
                break;
            case ANNOTATION_INDEX_COMMAND:
                annotate();
                break;
            case ANNOTATION_SAVE_COMMAND:
                annotationSave();
                break;
            case ANNOTATION_DELETE_COMMAND:
                annotationDelete();
                break;
            case ANNOTATION_QUERY_COMMAND:
                annotationQuery();
                break;
            case ANNOTATION_METADATA_COMMAND:
                annotationMetadata();
                break;
            case AGGREGATE_FAMILY_COMMAND:
                aggregateFamily();
                break;
            case AGGREGATE_COMMAND:
                aggregate();
                break;
            case SAMPLE_RUN_COMMAND:
                sampleRun();
                break;
            case GWAS_RUN_COMMAND:
                gwas();
                break;
            case KNOCKOUT_RUN_COMMAND:
                knockout();
                break;
            case VariantCommandOptions.SampleEligibilityCommandOptions.SAMPLE_ELIGIBILITY_RUN_COMMAND:
                sampleEligibility();
                break;
            case MUTATIONAL_SIGNATURE_RUN_COMMAND:
                mutationalSignature();
                break;
            case GENOME_PLOT_RUN_COMMAND:
                genomePlot();
                break;
            case MENDELIAN_ERROR_RUN_COMMAND:
                mendelianError();
                break;
            case INFERRED_SEX_RUN_COMMAND:
                inferredSex();
                break;
            case RELATEDNESS_RUN_COMMAND:
                relatedness();
                break;
            case FAMILY_QC_RUN_COMMAND:
                familyQc();
                break;
            case INDIVIDUAL_QC_RUN_COMMAND:
                individualQc();
                break;
            case SAMPLE_QC_RUN_COMMAND:
                sampleQc();
                break;
            case PLINK_RUN_COMMAND:
                plink();
                break;
            case RVTESTS_RUN_COMMAND:
                rvtests();
                break;
            case GATK_RUN_COMMAND:
                gatk();
                break;
            case SAMPLE_VARIANT_STATS_RUN_COMMAND:
                sampleStats();
                break;
            case COHORT_VARIANT_STATS_RUN_COMMAND:
                cohortStats();
                break;
            case VariantCommandOptions.JulieRunCommandOptions.JULIE_RUN_COMMAND:
                julie();
                break;
            case EXOMISER_RUN_COMMAND:
                exomiser();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void exportFrequencies() throws Exception {

        VariantCommandOptions.VariantExportStatsCommandOptions exportCliOptions = variantCommandOptions.exportVariantStatsCommandOptions;
//        AnalysisCliOptionsParser.ExportVariantStatsCommandOptions exportCliOptions = variantCommandOptions
//        .exportVariantStatsCommandOptions;
//        AnalysisCliOptionsParser.QueryVariantCommandOptions queryCliOptions = variantCommandOptions.queryVariantCommandOptions;

        VariantCommandOptions.VariantQueryCommandOptions queryCliOptions = variantCommandOptions.queryVariantCommandOptions;

        queryCliOptions.commonOptions.outputFormat = exportCliOptions.commonOptions.outputFormat.toLowerCase().replace("tsv", "stats");
        queryCliOptions.project = exportCliOptions.project;
        queryCliOptions.study = exportCliOptions.study;
        queryCliOptions.genericVariantQueryOptions.includeStudy = exportCliOptions.study;
        queryCliOptions.numericOptions.limit = exportCliOptions.numericOptions.limit;
//        queryCliOptions.sort = true;
        queryCliOptions.numericOptions.skip = exportCliOptions.numericOptions.skip;
        queryCliOptions.genericVariantQueryOptions.region = exportCliOptions.region;
        queryCliOptions.genericVariantQueryOptions.regionFile = exportCliOptions.regionFile;
        queryCliOptions.outdir = exportCliOptions.outdir;
        queryCliOptions.outputFileName = exportCliOptions.outputFileName;
        queryCliOptions.genericVariantQueryOptions.gene = exportCliOptions.gene;
        queryCliOptions.numericOptions.count = exportCliOptions.numericOptions.count;
        queryCliOptions.genericVariantQueryOptions.includeSample = VariantQueryUtils.NONE;
        queryCliOptions.dataModelOptions.include = String.join(",",
                VariantField.CHROMOSOME.fieldName(),
                VariantField.START.fieldName(),
                VariantField.ID.fieldName(),
                VariantField.REFERENCE.fieldName(),
                VariantField.ALTERNATE.fieldName(),
                VariantField.TYPE.fieldName(),
                VariantField.STUDIES_STUDY_ID.fieldName(),
                VariantField.STUDIES_STATS.fieldName(),
                VariantField.STUDIES_SECONDARY_ALTERNATES.fieldName()
        );

        query();
    }

    private void export() throws Exception {
        query(variantCommandOptions.exportVariantCommandOptions, variantCommandOptions.exportVariantCommandOptions.outdir);
    }

    private void query() throws Exception {
        query(variantCommandOptions.queryVariantCommandOptions, variantCommandOptions.queryVariantCommandOptions.outdir);
    }

    private void query(VariantCommandOptions.AbstractVariantQueryCommandOptions cliOptions, String outdir) throws Exception {
//        AnalysisCliOptionsParser.QueryVariantCommandOptions cliOptions = variantCommandOptions.queryVariantCommandOptions;
        if (cliOptions instanceof VariantCommandOptions.VariantQueryCommandOptions)
            if (((VariantCommandOptions.VariantQueryCommandOptions) cliOptions).compress) {
                if (!cliOptions.commonOptions.outputFormat.toLowerCase().endsWith(".gz")) {
                    cliOptions.commonOptions.outputFormat += ".GZ";
                }
            }

        Map<Long, String> studyIds = getStudyIds(token);
        Query query = VariantQueryCommandUtils.parseQuery(cliOptions, studyIds.values());
        QueryOptions queryOptions = VariantQueryCommandUtils.parseQueryOptions(cliOptions);
        queryOptions.put("summary", cliOptions.genericVariantQueryOptions.summary);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        if (cliOptions.numericOptions.count) {
            DataResult<Long> result = variantManager.count(query, token);
            System.out.println("Num. results\t" + result.getResults().get(0));
        } else {
            queryOptions.putIfNotEmpty("annotations", cliOptions.genericVariantQueryOptions.annotations);

            ObjectMap params = new VariantExportParams(
                    query, outdir,
                    cliOptions.outputFileName,
                    cliOptions.commonOptions.outputFormat,
                    cliOptions.variantsFile)
                    .toObjectMap(queryOptions);
            toolRunner.execute(VariantExportTool.class, params, Paths.get(outdir), jobId, token);
        }
    }

    private void importData() throws URISyntaxException, ToolException, CatalogException, StorageEngineException {
        VariantCommandOptions.VariantImportCommandOptions importVariantOptions = variantCommandOptions.importVariantCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.importData(UriUtils.createUri(importVariantOptions.input), importVariantOptions.study, token);
    }

    private void fileDelete() throws ToolException {
        VariantCommandOptions.VariantDeleteCommandOptions cliOptions = variantCommandOptions.variantDeleteCommandOptions;

        VariantFileDeleteParams params = new VariantFileDeleteParams(cliOptions.genericVariantDeleteOptions.file,
                cliOptions.genericVariantDeleteOptions.resume);
        toolRunner.execute(VariantFileDeleteOperationTool.class,
                params.toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir), jobId, token);
    }

    private void index() throws ToolException {
        VariantCommandOptions.VariantIndexCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        ObjectMap params = new VariantIndexParams(
                cliOptions.fileId,
                cliOptions.genericVariantIndexOptions.resume,
                cliOptions.outdir,
                cliOptions.genericVariantIndexOptions.transform,
                cliOptions.genericVariantIndexOptions.gvcf,
                cliOptions.genericVariantIndexOptions.normalizationSkip,
                cliOptions.genericVariantIndexOptions.referenceGenome,
                cliOptions.genericVariantIndexOptions.failOnMalformedLines,
                cliOptions.genericVariantIndexOptions.family,
                cliOptions.genericVariantIndexOptions.somatic,
                cliOptions.genericVariantIndexOptions.load,
                cliOptions.genericVariantIndexOptions.loadSplitData,
                cliOptions.genericVariantIndexOptions.loadMultiFileData,
                cliOptions.genericVariantIndexOptions.loadSampleIndex,
                cliOptions.genericVariantIndexOptions.loadArchive,
                cliOptions.genericVariantIndexOptions.loadHomRef,
                cliOptions.genericVariantIndexOptions.postLoadCheck,
                cliOptions.genericVariantIndexOptions.includeGenotype,
                cliOptions.genericVariantIndexOptions.includeSampleData,
                cliOptions.genericVariantIndexOptions.merge,
                cliOptions.genericVariantIndexOptions.deduplicationPolicy,
                cliOptions.genericVariantIndexOptions.calculateStats,
                cliOptions.genericVariantIndexOptions.aggregated,
                cliOptions.genericVariantIndexOptions.aggregationMappingFile,
                cliOptions.genericVariantIndexOptions.annotate,
                cliOptions.genericVariantIndexOptions.annotator,
                cliOptions.genericVariantIndexOptions.overwriteAnnotations,
                cliOptions.genericVariantIndexOptions.indexSearch,
                cliOptions.skipIndexedFiles)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study)
                .append(VariantStorageOptions.STDIN.key(), cliOptions.stdin)
                .append(VariantStorageOptions.STDOUT.key(), cliOptions.stdout);

        toolRunner.execute(VariantIndexOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void secondaryIndex() throws ToolException {
        VariantCommandOptions.VariantSecondaryIndexCommandOptions cliOptions = variantCommandOptions.variantSecondaryIndexCommandOptions;

        ObjectMap params = new VariantSecondaryIndexParams(
                cliOptions.region,
                cliOptions.sample,
                cliOptions.overwrite)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

        if (CollectionUtils.isEmpty(cliOptions.sample)) {
            toolRunner.execute(VariantSecondaryIndexOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
        } else {
            toolRunner.execute(VariantSecondaryIndexSamplesOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
        }
    }

    private void secondaryIndexRemove() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException, VariantSearchException {
        VariantCommandOptions.VariantSecondaryIndexDeleteCommandOptions cliOptions =
                variantCommandOptions.variantSecondaryIndexDeleteCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.removeSearchIndexSamples(cliOptions.study, Arrays.asList(cliOptions.sample.split(",")), params, token);
    }

    private void statsRun() throws ToolException {
        VariantCommandOptions.VariantStatsCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        ObjectMap params = new VariantStatsAnalysisParams(
                cliOptions.cohort,
                cliOptions.samples,
                cliOptions.region,
                cliOptions.gene,
                cliOptions.outdir,
                cliOptions.fileName,
                cliOptions.aggregated,
                cliOptions.aggregationMappingFile)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantStatsAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

//    private void statsIndex() throws ToolException {
//        VariantCommandOptions.VariantStatsCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;
//
//        ObjectMap params = new VariantStatsIndexParams(
//                cliOptions.cohort,
//                cliOptions.overwriteStats,
//                cliOptions.resume,
//                cliOptions.aggregated,
//                cliOptions.aggregationMappingFile)
//                .toObjectMap(cliOptions.commonOptions.params)
//                .append(ParamConstants.STUDY_PARAM, cliOptions.study);
//
//        toolRunner.execute(VariantIndexOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
//    }

    private void scoreLoad() throws ToolException {
        VariantCommandOptions.VariantScoreIndexCommandOptions cliOptions = variantCommandOptions.variantScoreIndexCommandOptions;

        ObjectMap params = new VariantScoreIndexParams(
                cliOptions.scoreName,
                cliOptions.cohort1,
                cliOptions.cohort2,
                cliOptions.input,
                cliOptions.columns,
                cliOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantScoreIndexOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void scoreRemove() throws ToolException {
        VariantCommandOptions.VariantScoreDeleteCommandOptions cliOptions = variantCommandOptions.variantScoreDeleteCommandOptions;

        ObjectMap params = new VariantScoreDeleteParams(
                cliOptions.scoreName,
                cliOptions.force,
                cliOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantScoreDeleteOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void sampleIndex()
            throws ToolException {
        VariantCommandOptions.SampleIndexCommandOptions cliOptions = variantCommandOptions.sampleIndexCommandOptions;

        VariantSampleIndexParams params = new VariantSampleIndexParams(
                cliOptions.sample,
                cliOptions.buildIndex,
                cliOptions.annotate,
                cliOptions.familyIndex,
                cliOptions.overwrite
        );

        toolRunner.execute(VariantSampleIndexOperationTool.class,
                params.toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir),
                jobId, token);
    }

    private void familyIndex()
            throws ToolException {
        VariantCommandOptions.FamilyIndexCommandOptions cliOptions = variantCommandOptions.familyIndexCommandOptions;

        VariantFamilyIndexParams params = new VariantFamilyIndexParams(
                cliOptions.family,
                cliOptions.overwrite,
                cliOptions.update,
                cliOptions.skipIncompleteFamilies);

        toolRunner.execute(VariantFamilyIndexOperationTool.class,
                params.toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir),
                jobId, token);
    }

    private void annotate() throws ToolException {
        VariantCommandOptions.VariantAnnotateCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;

        VariantAnnotationIndexParams params = new VariantAnnotationIndexParams(
                cliOptions.outdir,
                cliOptions.genericVariantAnnotateOptions.outputFileName,
                cliOptions.genericVariantAnnotateOptions.annotator == null
                        ? null
                        : cliOptions.genericVariantAnnotateOptions.annotator.toString(),
                cliOptions.genericVariantAnnotateOptions.overwriteAnnotations,
                cliOptions.genericVariantAnnotateOptions.region,
                cliOptions.genericVariantAnnotateOptions.create,
                cliOptions.genericVariantAnnotateOptions.load,
                cliOptions.genericVariantAnnotateOptions.customName,
                YesNoAuto.parse(cliOptions.genericVariantAnnotateOptions.sampleIndexAnnotation)
        );

        toolRunner.execute(VariantAnnotationIndexOperationTool.class,
                params.toObjectMap(cliOptions.commonOptions.params)
                        .append(ParamConstants.PROJECT_PARAM, cliOptions.project)
                        .append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir),
                jobId, token);
    }

    private void annotationSave() throws ToolException {
        VariantCommandOptions.AnnotationSaveCommandOptions cliOptions = variantCommandOptions.annotationSaveSnapshotCommandOptions;

        ObjectMap params = new VariantAnnotationSaveParams(cliOptions.annotationId)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

        toolRunner.execute(VariantAnnotationSaveOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void annotationDelete() throws ToolException {
        VariantCommandOptions.AnnotationDeleteCommandOptions cliOptions = variantCommandOptions.annotationDeleteCommandOptions;

        ObjectMap params = new VariantAnnotationDeleteParams(cliOptions.annotationId)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

        toolRunner.execute(VariantAnnotationDeleteOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void annotationQuery() throws CatalogException, IOException, StorageEngineException {
        VariantCommandOptions.AnnotationQueryCommandOptions cliOptions = variantCommandOptions.annotationQueryCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.put(QueryOptions.LIMIT, cliOptions.limit);
        options.put(QueryOptions.SKIP, cliOptions.skip);
        options.put(QueryOptions.INCLUDE, cliOptions.dataModelOptions.include);
        options.put(QueryOptions.EXCLUDE, cliOptions.dataModelOptions.exclude);
        options.putAll(cliOptions.commonOptions.params);

        Query query = new Query();
        query.put(VariantCatalogQueryUtils.PROJECT.key(), cliOptions.project);
        query.put(VariantQueryParam.REGION.key(), cliOptions.region);
        query.put(VariantQueryParam.ID.key(), cliOptions.id);

        DataResult<VariantAnnotation> queryResult = variantManager.getAnnotation(cliOptions.annotationId, query, options, token);

        // WRITE
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        ObjectWriter writer = objectMapper.writer();
//        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        SequenceWriter sequenceWriter = writer.writeValues(System.out);
        for (VariantAnnotation annotation : queryResult.getResults()) {
            sequenceWriter.write(annotation);
            sequenceWriter.flush();
//            writer.writeValue(System.out, annotation);
            System.out.println();
        }
    }

    private void annotationMetadata() throws CatalogException, IOException, StorageEngineException {
        VariantCommandOptions.AnnotationMetadataCommandOptions cliOptions = variantCommandOptions.annotationMetadataCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        DataResult<ProjectMetadata.VariantAnnotationMetadata> result =
                variantManager.getAnnotationMetadata(cliOptions.annotationId, cliOptions.project, token);

        // WRITE
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        ObjectWriter writer = objectMapper.writer();
//        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        SequenceWriter sequenceWriter = writer.writeValues(System.out);
        for (ProjectMetadata.VariantAnnotationMetadata metadata : result.getResults()) {
            sequenceWriter.write(metadata);
            sequenceWriter.flush();
//            writer.writeValue(System.out, annotation);
            System.out.println();
        }
    }

    private void aggregateFamily() throws ToolException {
        VariantCommandOptions.AggregateFamilyCommandOptions cliOptions = variantCommandOptions.fillGapsVariantCommandOptions;

        ObjectMap params = new VariantAggregateFamilyParams(
                cliOptions.genericAggregateFamilyOptions.samples,
                cliOptions.genericAggregateFamilyOptions.gapsGenotype,
                cliOptions.genericAggregateFamilyOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantAggregateFamilyOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void aggregate() throws ToolException {
        VariantCommandOptions.AggregateCommandOptions cliOptions = variantCommandOptions.aggregateCommandOptions;

        ObjectMap params = new VariantAggregateParams(
                cliOptions.aggregateCommandOptions.overwrite,
                cliOptions.aggregateCommandOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantAggregateOperationTool.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void sampleRun() throws Exception {
        VariantCommandOptions.VariantSamplesFilterCommandOptions cliOptions = variantCommandOptions.samplesFilterCommandOptions;
        cliOptions.toolParams.toObjectMap(cliOptions.commonOptions.params);

        toolRunner.execute(SampleVariantFilterAnalysis.class,
                cliOptions.toolParams.toObjectMap(cliOptions.commonOptions.params)
                        .append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir), jobId, token);
    }

    private void gwas() throws Exception {
        VariantCommandOptions.GwasCommandOptions cliOptions = variantCommandOptions.gwasCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        Query caseCohortSamplesQuery = null;
        if (StringUtils.isNotEmpty(cliOptions.caseCohortSamplesAnnotation)) {
            caseCohortSamplesQuery = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY.key(), cliOptions.study)
                    .append(SampleDBAdaptor.QueryParams.ANNOTATION.key(), cliOptions.caseCohortSamplesAnnotation);
        }
        Query controlCohortSamplesQuery = null;
        if (StringUtils.isNotEmpty(cliOptions.controlCohortSamplesAnnotation)) {
            controlCohortSamplesQuery = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY.key(), cliOptions.study)
                    .append(SampleDBAdaptor.QueryParams.ANNOTATION.key(), cliOptions.controlCohortSamplesAnnotation);
        }
        GwasAnalysis gwasAnalysis = new GwasAnalysis();
        gwasAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        gwasAnalysis.setStudy(cliOptions.study)
                .setPhenotype(cliOptions.phenotype)
                .setIndex(cliOptions.index)
                .setIndexScoreId(cliOptions.indexScoreId)
                .setFisherMode(cliOptions.fisherMode)
                .setGwasMethod(cliOptions.method)
                .setControlCohort(cliOptions.controlCohort)
                .setCaseCohort(cliOptions.caseCohort)
                .setCaseCohortSamplesQuery(caseCohortSamplesQuery)
                .setControlCohortSamplesQuery(controlCohortSamplesQuery)
                .setCaseCohortSamples(cliOptions.caseCohortSamples)
                .setControlCohortSamples(cliOptions.controlCohortSamples)
                .start();
    }

    private void knockout() throws Exception {
        VariantCommandOptions.KnockoutCommandOptions cliOptions = variantCommandOptions.knockoutCommandOptions;

        ObjectMap params = new KnockoutAnalysisParams(
                cliOptions.sample,
                cliOptions.gene,
                cliOptions.panel,
                cliOptions.biotype,
                cliOptions.consequenceType,
                cliOptions.filter,
                cliOptions.qual,
                cliOptions.skipGenesFile,
                cliOptions.outdir,
                cliOptions.index
        )
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(KnockoutAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void sampleEligibility() throws Exception {
        VariantCommandOptions.SampleEligibilityCommandOptions cliOptions = variantCommandOptions.sampleEligibilityCommandOptions;

        ObjectMap params = new SampleEligibilityAnalysisParams(
                cliOptions.query,
                cliOptions.index,
                cliOptions.cohortId)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(SampleEligibilityAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void sampleStats() throws Exception {
        VariantCommandOptions.SampleVariantStatsCommandOptions cliOptions = variantCommandOptions.sampleVariantStatsCommandOptions;
        ObjectMap params = new ObjectMap(ParamConstants.STUDY_PARAM, cliOptions.study);
        params.putAll(cliOptions.commonOptions.params);

        // Build variant query from cli options
        Query variantQuery = new Query();
        variantQuery.putAll(cliOptions.variantQuery);

        SampleVariantStatsAnalysisParams toolParams = new SampleVariantStatsAnalysisParams(
                cliOptions.sample,
                cliOptions.individual,
                cliOptions.outdir, cliOptions.index, cliOptions.indexOverwrite, cliOptions.indexId, cliOptions.indexDescription,
                cliOptions.batchSize,
                variantQuery
        );
        toolRunner.execute(SampleVariantStatsAnalysis.class, toolParams, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void cohortStats() throws Exception {
        VariantCommandOptions.CohortVariantStatsCommandOptions cliOptions = variantCommandOptions.cohortVariantStatsCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        Query query = null;
        if (StringUtils.isNotEmpty(cliOptions.samplesAnnotation)) {
            query = new Query();
            query.append(SampleDBAdaptor.QueryParams.STUDY.key(), cliOptions.study);
            query.append(SampleDBAdaptor.QueryParams.ANNOTATION.key(), cliOptions.samplesAnnotation);
        }

        List<String> sampleNames = cliOptions.samples;

        CohortVariantStatsAnalysis cohortVariantStatsAnalysis = new CohortVariantStatsAnalysis();
        cohortVariantStatsAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        cohortVariantStatsAnalysis.setStudy(cliOptions.study)
                .setCohortName(cliOptions.cohort)
                .setIndex(cliOptions.index)
                .setSampleAnnotation(cliOptions.samplesAnnotation)
                .setSampleNames(sampleNames)
                .start();
    }

    private void julie() throws Exception {
        VariantCommandOptions.JulieRunCommandOptions cliOptions = variantCommandOptions.julieRunCommandOptions;
        ObjectMap params = new ObjectMap();
        params.put(ParamConstants.PROJECT_PARAM, cliOptions.project);
        params.putAll(cliOptions.commonOptions.params);

        JulieParams toolParams = new JulieParams(StringUtils.isEmpty(cliOptions.cohort)
                ? Collections.emptyList()
                : Arrays.asList(cliOptions.cohort.split(",")), cliOptions.region, cliOptions.overwrite);

        Path outdir = Paths.get(cliOptions.outdir);

        toolRunner.execute(JulieTool.class, toolParams, params, outdir, jobId, token);
    }

    private void mutationalSignature() throws Exception {
        VariantCommandOptions.MutationalSignatureCommandOptions cliOptions = variantCommandOptions.mutationalSignatureCommandOptions;

        // Check signature release
        checkSignatureRelease(cliOptions.release);

        ObjectMap params = new MutationalSignatureAnalysisParams(
                cliOptions.sample,
                cliOptions.id,
                cliOptions.description,
                new ObjectMap(cliOptions.query),
                cliOptions.release,
                cliOptions.fitting,
                cliOptions.outdir)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(MutationalSignatureAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void genomePlot() throws Exception {
        VariantCommandOptions.GenomePlotInternalCommandOptions cliOptions = variantCommandOptions.genomePlotInternalCommandOptions;

        ObjectMap params = new GenomePlotAnalysisParams(
                cliOptions.sample,
                cliOptions.id,
                cliOptions.description,
                cliOptions.configFile,
                cliOptions.outdir)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(GenomePlotAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void mendelianError() throws Exception {
        VariantCommandOptions.MendelianErrorCommandOptions cliOptions = variantCommandOptions.mendelianErrorCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        MendelianErrorAnalysis mendelianErrorAnalysis = new MendelianErrorAnalysis();
        mendelianErrorAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        mendelianErrorAnalysis.setStudy(cliOptions.study)
                .setFamilyId(cliOptions.family)
                .setIndividualId(cliOptions.individual)
                .setSampleId(cliOptions.sample)
                .start();
    }

    private void inferredSex() throws Exception {
        VariantCommandOptions.InferredSexCommandOptions cliOptions = variantCommandOptions.inferredSexCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        InferredSexAnalysis inferredSexAnalysis = new InferredSexAnalysis();
        inferredSexAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        inferredSexAnalysis.setStudyId(cliOptions.study)
                .setIndividualId(cliOptions.individual)
                .start();
    }

    private void relatedness() throws Exception {
        VariantCommandOptions.RelatednessCommandOptions cliOptions = variantCommandOptions.relatednessCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        RelatednessAnalysis relatednessAnalysis = new RelatednessAnalysis();
        relatednessAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        relatednessAnalysis.setStudyId(cliOptions.study)
                .setIndividualIds(cliOptions.individuals)
                .setSampleIds(cliOptions.samples)
                .setMinorAlleleFreq(cliOptions.minorAlleleFreq)
                .setMethod(cliOptions.method)
                .start();
    }

    private void familyQc() throws Exception {
        VariantCommandOptions.FamilyQcCommandOptions cliOptions = variantCommandOptions.familyQcCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        FamilyQcAnalysis familyQcAnalysis = new FamilyQcAnalysis();
        familyQcAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        familyQcAnalysis.setStudyId(cliOptions.study)
                .setFamilyId(cliOptions.family)
                .setRelatednessMethod(cliOptions.relatednessMethod)
                .setRelatednessMaf(cliOptions.relatednessMaf)
                .start();
    }

    private void individualQc() throws Exception {
        VariantCommandOptions.IndividualQcCommandOptions cliOptions = variantCommandOptions.individualQcCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        IndividualQcAnalysis individualQcAnalysis = new IndividualQcAnalysis();
        individualQcAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        individualQcAnalysis.setStudyId(cliOptions.study)
                .setIndividualId(cliOptions.individual)
                .setSampleId(cliOptions.sample)
                .setInferredSexMethod(cliOptions.inferredSexMethod)
                .start();
    }

    private void sampleQc() throws Exception {
        VariantCommandOptions.SampleQcCommandOptions cliOptions = variantCommandOptions.sampleQcCommandOptions;

        // Check signature release
        checkSignatureRelease(cliOptions.signatureRelease);

        // Build variant query from cli options
        AnnotationVariantQueryParams variantStatsQuery = ToolParams.fromParams(AnnotationVariantQueryParams.class,
                cliOptions.variantStatsQuery);

        ObjectMap params = new SampleQcAnalysisParams(
                cliOptions.sample,
                cliOptions.variantStatsId,
                cliOptions.variantStatsDecription,
                variantStatsQuery,
                cliOptions.signatureId,
                cliOptions.signatureDescription,
                new ObjectMap(cliOptions.signatureQuery),
                cliOptions.signatureRelease,
                cliOptions.genomePlotId,
                cliOptions.genomePlotDescr,
                cliOptions.genomePlotConfigFile,
                cliOptions.outdir)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(SampleQcAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    // Wrappers

    private void plink() throws Exception {
        VariantCommandOptions.PlinkCommandOptions cliOptions = variantCommandOptions.plinkCommandOptions;

        ObjectMap params = new PlinkWrapperParams(
                cliOptions.outdir,
                cliOptions.plinkParams)
                .toObjectMap(cliOptions.basicOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(PlinkWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void rvtests() throws Exception {
        VariantCommandOptions.RvtestsCommandOptions cliOptions = variantCommandOptions.rvtestsCommandOptions;

        ObjectMap params = new RvtestsWrapperParams(
                cliOptions.command,
                cliOptions.outdir,
                cliOptions.rvtestsParams)
                .toObjectMap(cliOptions.basicOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(RvtestsWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void gatk() throws Exception {
        VariantCommandOptions.GatkCommandOptions cliOptions = variantCommandOptions.gatkCommandOptions;

        ObjectMap params = new GatkWrapperParams(
                cliOptions.command,
                cliOptions.outdir,
                cliOptions.gatkParams)
                .toObjectMap(cliOptions.basicOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(GatkWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void exomiser() throws Exception {
        VariantCommandOptions.ExomiserAnalysisCommandOptions cliOptions = variantCommandOptions.exomiserAnalysisCommandOptions;

        ObjectMap params = new ExomiserWrapperParams(
                cliOptions.sample,
                cliOptions.outdir)
                .toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(ExomiserWrapperAnalysis.class, params, Paths.get(cliOptions.outdir), jobId, token);
    }

    private void checkSignatureRelease(String release) throws ClientException {
        switch (release) {
            case "2":
            case "3":
            case "3.1":
            case "3.2":
                break;
            default:
                throw new ClientException("Invalid value " + release + " for the mutational signature release. "
                        + "Valid values are: 2, 3, 3.1 and 3.2");
        }
    }

}
