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


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.sample.qc.SampleQcAnalysis;
import org.opencb.opencga.analysis.tools.ToolRunner;
import org.opencb.opencga.analysis.variant.VariantExportTool;
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
import org.opencb.opencga.analysis.wrappers.GatkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.RvtestsWrapperAnalysis;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exceptions.AnalysisExecutionException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.operations.variant.*;
import org.opencb.opencga.core.models.variant.*;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GatkCommandOptions.GATK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.InferredSexCommandOptions.INFERRED_SEX_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.KnockoutCommandOptions.KNOCKOUT_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MendelianErrorCommandOptions.MENDELIAN_ERROR_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.MutationalSignatureCommandOptions.MUTATIONAL_SIGNATURE_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RelatednessCommandOptions.RELATEDNESS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTEST_RUN_COMMAND;
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
    private ToolRunner toolRunner;

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

        token = getSessionId(variantCommandOptions.commonCommandOptions);
        toolRunner = new ToolRunner(appHome, catalogManager, storageEngineFactory, variantCommandOptions.internalJobOptions.jobId);

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
                stats();
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
            case MENDELIAN_ERROR_RUN_COMMAND:
                mendelianError();
                break;
            case INFERRED_SEX_RUN_COMMAND:
                inferredSex();
                break;
            case RELATEDNESS_RUN_COMMAND:
                relatedness();
                break;
            case SAMPLE_QC_RUN_COMMAND:
                sampleQc();
                break;
            case PLINK_RUN_COMMAND:
                plink();
                break;
            case RVTEST_RUN_COMMAND:
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
            default:
                logger.error("Subcommand not valid");
                break;
        }
    }

    private void exportFrequencies() throws Exception {

        VariantCommandOptions.VariantExportStatsCommandOptions exportCliOptions = variantCommandOptions.exportVariantStatsCommandOptions;
//        AnalysisCliOptionsParser.ExportVariantStatsCommandOptions exportCliOptions = variantCommandOptions.exportVariantStatsCommandOptions;
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
        if (cliOptions.compress) {
            if (!cliOptions.commonOptions.outputFormat.toLowerCase().endsWith(".gz")) {
                cliOptions.commonOptions.outputFormat += ".GZ";
            }
        }

        Map<Long, String> studyIds = getStudyIds(token);
        Query query = VariantQueryCommandUtils.parseQuery(cliOptions, studyIds.values(), clientConfiguration);
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
                    cliOptions.compress,
                    cliOptions.variantsFile)
                    .toObjectMap(queryOptions);
            toolRunner.execute(VariantExportTool.class, params, Paths.get(outdir), token);
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
                Paths.get(cliOptions.outdir), token);
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
                cliOptions.genericVariantIndexOptions.family,
                cliOptions.genericVariantIndexOptions.somatic,
                cliOptions.genericVariantIndexOptions.load,
                cliOptions.genericVariantIndexOptions.loadSplitData,
                cliOptions.genericVariantIndexOptions.loadMultiFileData,
                cliOptions.genericVariantIndexOptions.loadSampleIndex,
                cliOptions.genericVariantIndexOptions.loadArchive,
                cliOptions.genericVariantIndexOptions.loadHomRef,
                cliOptions.genericVariantIndexOptions.postLoadCheck,
                cliOptions.genericVariantIndexOptions.excludeGenotype,
                cliOptions.genericVariantIndexOptions.includeSampleData,
                cliOptions.genericVariantIndexOptions.merge,
                cliOptions.genericVariantIndexOptions.deduplicationPolicy,
                cliOptions.genericVariantIndexOptions.calculateStats,
                cliOptions.genericVariantIndexOptions.aggregated,
                cliOptions.genericVariantIndexOptions.aggregationMappingFile,
                cliOptions.genericVariantIndexOptions.annotate,
                cliOptions.genericVariantIndexOptions.annotator,
                cliOptions.genericVariantIndexOptions.overwriteAnnotations, cliOptions.genericVariantIndexOptions.indexSearch)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study)
                .append(VariantStorageOptions.STDIN.key(), cliOptions.stdin)
                .append(VariantStorageOptions.STDOUT.key(), cliOptions.stdout);

        toolRunner.execute(VariantIndexOperationTool.class, params, Paths.get(cliOptions.outdir), token);
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
            toolRunner.execute(VariantSecondaryIndexOperationTool.class, params, Paths.get(cliOptions.outdir), token);
        } else {
            toolRunner.execute(VariantSecondaryIndexSamplesOperationTool.class, params, Paths.get(cliOptions.outdir), token);
        }
    }

    private void secondaryIndexRemove() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException, VariantSearchException {
        VariantCommandOptions.VariantSecondaryIndexDeleteCommandOptions cliOptions = variantCommandOptions.variantSecondaryIndexDeleteCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.removeSearchIndexSamples(cliOptions.study, Arrays.asList(cliOptions.sample.split(",")), params, token);
    }

    private void stats() throws ToolException {
        VariantCommandOptions.VariantStatsCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        ObjectMap params = new VariantStatsAnalysisParams(
                cliOptions.cohort,
                cliOptions.samples,
                cliOptions.index,
                cliOptions.outdir,
                cliOptions.genericVariantStatsOptions.fileName,
                cliOptions.genericVariantStatsOptions.region,
                cliOptions.genericVariantStatsOptions.gene,
                cliOptions.genericVariantStatsOptions.overwriteStats,
                cliOptions.genericVariantStatsOptions.updateStats,
                cliOptions.genericVariantStatsOptions.resume,
                cliOptions.genericVariantStatsOptions.aggregated,
                cliOptions.genericVariantStatsOptions.aggregationMappingFile)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantStatsAnalysis.class, params, Paths.get(cliOptions.outdir), token);
    }

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


        toolRunner.execute(VariantScoreIndexOperationTool.class, params, Paths.get(cliOptions.outdir), token);
    }

    private void scoreRemove() throws ToolException {
        VariantCommandOptions.VariantScoreDeleteCommandOptions cliOptions = variantCommandOptions.variantScoreDeleteCommandOptions;

        ObjectMap params = new VariantScoreDeleteParams(
                cliOptions.scoreName,
                cliOptions.force,
                cliOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantScoreDeleteOperationTool.class, params, Paths.get(cliOptions.outdir), token);
    }

    private void sampleIndex()
            throws ToolException {
        VariantCommandOptions.SampleIndexCommandOptions cliOptions = variantCommandOptions.sampleIndexCommandOptions;

        VariantSampleIndexParams params = new VariantSampleIndexParams(
                cliOptions.sample,
                cliOptions.buildIndex,
                cliOptions.annotate
        );

        toolRunner.execute(VariantSampleIndexOperationTool.class,
                params.toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir),
                token);
    }

    private void familyIndex()
            throws ToolException {
        VariantCommandOptions.FamilyIndexCommandOptions cliOptions = variantCommandOptions.familyIndexCommandOptions;

        VariantFamilyIndexParams params = new VariantFamilyIndexParams(
                cliOptions.family,
                cliOptions.overwrite,
                cliOptions.skipIncompleteFamilies);

        toolRunner.execute(VariantFamilyIndexOperationTool.class,
                params.toObjectMap(cliOptions.commonOptions.params).append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir),
                token);
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
                cliOptions.genericVariantAnnotateOptions.customName
        );

        toolRunner.execute(VariantAnnotationIndexOperationTool.class,
                params.toObjectMap(cliOptions.commonOptions.params)
                        .append(ParamConstants.PROJECT_PARAM, cliOptions.project)
                        .append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir),
                token);
    }

    private void annotationSave() throws ToolException {
        VariantCommandOptions.AnnotationSaveCommandOptions cliOptions = variantCommandOptions.annotationSaveSnapshotCommandOptions;

        ObjectMap params = new VariantAnnotationSaveParams(cliOptions.annotationId)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

        toolRunner.execute(VariantAnnotationSaveOperationTool.class, params, Paths.get(cliOptions.outdir), token);
    }

    private void annotationDelete() throws ToolException {
        VariantCommandOptions.AnnotationDeleteCommandOptions cliOptions = variantCommandOptions.annotationDeleteCommandOptions;

        ObjectMap params = new VariantAnnotationDeleteParams(cliOptions.annotationId)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.PROJECT_PARAM, cliOptions.project);

        toolRunner.execute(VariantAnnotationDeleteOperationTool.class, params, Paths.get(cliOptions.outdir), token);
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
                cliOptions.genericAggregateFamilyOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantAggregateFamilyOperationTool.class, params, Paths.get(cliOptions.outdir), token);
    }

    private void aggregate() throws ToolException {
        VariantCommandOptions.AggregateCommandOptions cliOptions = variantCommandOptions.aggregateCommandOptions;

        ObjectMap params = new VariantAggregateParams(
                cliOptions.aggregateCommandOptions.overwrite,
                cliOptions.aggregateCommandOptions.resume)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(VariantAggregateOperationTool.class, params, Paths.get(cliOptions.outdir), token);
    }

    private void sampleRun() throws Exception {
        VariantCommandOptions.VariantSamplesFilterCommandOptions cliOptions = variantCommandOptions.samplesFilterCommandOptions;
        cliOptions.toolParams.toObjectMap(cliOptions.commonOptions.params);

        toolRunner.execute(SampleVariantFilterAnalysis.class,
                cliOptions.toolParams.toObjectMap(cliOptions.commonOptions.params)
                        .append(ParamConstants.STUDY_PARAM, cliOptions.study),
                Paths.get(cliOptions.outdir), token);
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
                cliOptions.outdir)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(KnockoutAnalysis.class, params, Paths.get(cliOptions.outdir), token);
    }

    private void sampleEligibility() throws Exception {
        VariantCommandOptions.SampleEligibilityCommandOptions cliOptions = variantCommandOptions.sampleEligibilityCommandOptions;

        ObjectMap params = new SampleEligibilityAnalysisParams(
                cliOptions.query,
                cliOptions.index,
                cliOptions.cohortId)
                .toObjectMap(cliOptions.commonOptions.params)
                .append(ParamConstants.STUDY_PARAM, cliOptions.study);

        toolRunner.execute(SampleEligibilityAnalysis.class, params, Paths.get(cliOptions.outdir), token);
    }

    private void sampleStats() throws Exception {
        VariantCommandOptions.SampleVariantStatsCommandOptions cliOptions = variantCommandOptions.sampleVariantStatsCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        Query query = null;
        if (StringUtils.isNotEmpty(cliOptions.samplesAnnotation)) {
            query = new Query();
            query.append(SampleDBAdaptor.QueryParams.STUDY.key(), cliOptions.study);
            query.append(SampleDBAdaptor.QueryParams.ANNOTATION.key(), cliOptions.samplesAnnotation);
        }

        SampleVariantStatsAnalysis sampleVariantStatsAnalysis = new SampleVariantStatsAnalysis();
        sampleVariantStatsAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        sampleVariantStatsAnalysis.setStudy(cliOptions.study)
                .setIndexResults(cliOptions.index)
                .setFamily(cliOptions.family)
                .setSamplesQuery(query)
                .setSampleNames(cliOptions.sample)
                .start();
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
                .setSamplesQuery(query)
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

        toolRunner.execute(JulieTool.class, toolParams, params, outdir, token);
    }

    private void mutationalSignature() throws Exception {
        VariantCommandOptions.MutationalSignatureCommandOptions cliOptions = variantCommandOptions.mutationalSignatureCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        MutationalSignatureAnalysis mutationalSignatureAnalysis = new MutationalSignatureAnalysis();
        mutationalSignatureAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        mutationalSignatureAnalysis.setStudy(cliOptions.study)
                .setSampleName(cliOptions.sample)
                .start();
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

    private void sampleQc() throws Exception {
        VariantCommandOptions.SampleQcCommandOptions cliOptions = variantCommandOptions.sampleQcCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String,String>>() {};
        Map<String, String> variantStatsQuery = new HashMap<>();
        if (StringUtils.isNotEmpty(cliOptions.variantStatsQuery)) {
            try {
                variantStatsQuery = JacksonUtils.getDefaultObjectMapper().readValue(cliOptions.variantStatsQuery, typeRef);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        Map<String, String> signatureQuery = new HashMap<>();
        if (StringUtils.isNotEmpty(cliOptions.signatureQuery)) {
            try {
                signatureQuery = JacksonUtils.getDefaultObjectMapper().readValue(cliOptions.signatureQuery, typeRef);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        List<String> genesForCoverageStats = new ArrayList<>();
        if (StringUtils.isNotEmpty(cliOptions.genesForCoverageStats)) {
            genesForCoverageStats = Arrays.asList(cliOptions.genesForCoverageStats.split(","));
        }

        SampleQcAnalysis sampleQcAnalysis = new SampleQcAnalysis();
        sampleQcAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);
        sampleQcAnalysis.setStudyId(cliOptions.study)
                .setSampleId(cliOptions.sample)
                .setFastaFile(cliOptions.fastaFile)
                .setBaitFile(cliOptions.baitFile)
                .setTargetFile(cliOptions.targetFile)
                .setVariantStatsId(cliOptions.variantStatsId)
                .setVariantStatsDecription(cliOptions.variantStatsDecription)
                .setVariantStatsQuery(variantStatsQuery)
                .setVariantStatsJobId(cliOptions.variantStatsJobId)
                .setSignatureId(cliOptions.signatureId)
                .setSignatureQuery(signatureQuery)
                .setVariantStatsJobId(cliOptions.signatureJobId)
                .setGenesForCoverageStats(genesForCoverageStats)
                .start();
    }

    // Wrappers

    private void plink() throws Exception {
        VariantCommandOptions.PlinkCommandOptions cliOptions = variantCommandOptions.plinkCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.basicOptions.params);

        if (StringUtils.isNotEmpty(cliOptions.tpedFile)) {
            params.put(PlinkWrapperAnalysis.TPED_FILE_PARAM, cliOptions.tpedFile);
        }
        if (StringUtils.isNotEmpty(cliOptions.tfamFile)) {
            params.put(PlinkWrapperAnalysis.TFAM_FILE_PARAM, cliOptions.tfamFile);
        }
        if (StringUtils.isNotEmpty(cliOptions.covarFile)) {
            params.put(PlinkWrapperAnalysis.COVAR_FILE_PARAM, cliOptions.covarFile);
        }

        PlinkWrapperAnalysis plink = new PlinkWrapperAnalysis();
        plink.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);

        plink.start();
    }

    private void rvtests() throws Exception {
        VariantCommandOptions.RvtestsCommandOptions cliOptions = variantCommandOptions.rvtestsCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.basicOptions.params);

        params.put(RvtestsWrapperAnalysis.EXECUTABLE_PARAM, cliOptions.executable);
        if (StringUtils.isNotEmpty(cliOptions.vcfFile)) {
            params.put(RvtestsWrapperAnalysis.VCF_FILE_PARAM, cliOptions.vcfFile);
        }
        if (StringUtils.isNotEmpty(cliOptions.phenoFile)) {
            params.put(RvtestsWrapperAnalysis.PHENOTYPE_FILE_PARAM, cliOptions.phenoFile);
        }
        if (StringUtils.isNotEmpty(cliOptions.pedigreeFile)) {
            params.put(RvtestsWrapperAnalysis.PEDIGREE_FILE_PARAM, cliOptions.pedigreeFile);
        }
        if (StringUtils.isNotEmpty(cliOptions.kinshipFile)) {
            params.put(RvtestsWrapperAnalysis.KINSHIP_FILE_PARAM, cliOptions.kinshipFile);
        }
        if (StringUtils.isNotEmpty(cliOptions.covarFile)) {
            params.put(RvtestsWrapperAnalysis.COVAR_FILE_PARAM, cliOptions.covarFile);
        }

        RvtestsWrapperAnalysis rvtests = new RvtestsWrapperAnalysis();
        rvtests.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, token);

        rvtests.start();
    }

    private void gatk() throws Exception {
        VariantCommandOptions.GatkCommandOptions cliOptions = variantCommandOptions.gatkCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.basicOptions.params);

        GatkWrapperAnalysis gatk = new GatkWrapperAnalysis();
        gatk.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir),
                variantCommandOptions.internalJobOptions.jobId, cliOptions.basicOptions.token);

        gatk.setStudy(cliOptions.study);
        gatk.setCommand(cliOptions.command);
        gatk.setFastaFile(cliOptions.fastaFile);
        gatk.setBamFile(cliOptions.bamFile);
        gatk.setVcfFilename(cliOptions.vcfFilename);

        gatk.start();
    }
}
