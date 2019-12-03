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
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.old.AnalysisExecutionException;
import org.opencb.opencga.analysis.old.execution.plugins.PluginExecutor;
import org.opencb.opencga.analysis.old.execution.plugins.hist.VariantHistogramAnalysis;
import org.opencb.opencga.analysis.old.execution.plugins.ibs.IbsAnalysis;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.RvtestsWrapperAnalysis;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.analysis.VariantSampleFilter;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.operations.VariantFileIndexerStorageOperation.LOAD;
import static org.opencb.opencga.analysis.variant.operations.VariantFileIndexerStorageOperation.TRANSFORM;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.GwasCommandOptions.GWAS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.PlinkCommandOptions.PLINK_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.RvtestsCommandOptions.RVTEST_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_RUN_COMMAND;
import static org.opencb.opencga.app.cli.internal.options.VariantCommandOptions.VariantAnnotateCommandOptions.ANNOTATION_INDEX_COMMAND;
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
public class VariantCommandExecutor extends InternalCommandExecutor {

    //    private AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions;
    private VariantCommandOptions variantCommandOptions;

    public VariantCommandExecutor(VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonCommandOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

//        String subCommandString = variantCommandOptions.getParsedSubCommand();
        String subCommandString = getParsedSubCommand(variantCommandOptions.jCommander);
        configure();

        sessionId = getSessionId(variantCommandOptions.commonCommandOptions);

        switch (subCommandString) {
            case "ibs":
                ibs();
                break;
            case VARIANT_DELETE_COMMAND:
                remove();
                break;
            case "export-run":
                export();
                break;
            case "query":
                query();
                break;
            case "stats-export-run":
                exportFrequencies();
                break;
            case "import":
                importData();
                break;
            case "index":
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
                fillGaps();
                break;
            case AGGREGATE_COMMAND:
                fillMissing();
                break;
            case "samples":
                samples();
                break;
            case "histogram":
                histogram();
                break;
            case GWAS_RUN_COMMAND:
                gwas();
                break;
            case PLINK_RUN_COMMAND:
                plink();
                break;
            case RVTEST_RUN_COMMAND:
                rvtests();
                break;
            case SAMPLE_VARIANT_STATS_RUN_COMMAND:
                sampleStats();
                break;
            case COHORT_VARIANT_STATS_RUN_COMMAND:
                cohortStats();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void ibs() throws CatalogException, AnalysisExecutionException {
        VariantCommandOptions.VariantIbsCommandOptions cliOptions = variantCommandOptions.ibsVariantCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(IbsAnalysis.SAMPLES, cliOptions.samples);
        params.putIfNotEmpty(IbsAnalysis.OUTDIR, cliOptions.outdir);

        String userId1 = catalogManager.getUserManager().getUserId(sessionId);
        new PluginExecutor(catalogManager, sessionId).execute(IbsAnalysis.class, "default", catalogManager.getStudyManager().resolveId
                (cliOptions.study, userId1).getUid(), params);
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

        Map<Long, String> studyIds = getStudyIds(sessionId);
        Query query = VariantQueryCommandUtils.parseQuery(cliOptions, studyIds.values(), clientConfiguration);
        QueryOptions queryOptions = VariantQueryCommandUtils.parseQueryOptions(cliOptions);
        queryOptions.put("summary", cliOptions.genericVariantQueryOptions.summary);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        if (cliOptions.numericOptions.count) {
            DataResult<Long> result = variantManager.count(query, sessionId);
            System.out.println("Num. results\t" + result.getResults().get(0));
        } else if (StringUtils.isNotEmpty(cliOptions.genericVariantQueryOptions.groupBy)) {
            ObjectMapper objectMapper = new ObjectMapper();
            DataResult groupBy = variantManager.groupBy(cliOptions.genericVariantQueryOptions.groupBy, query, queryOptions, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
        } else if (StringUtils.isNotEmpty(cliOptions.genericVariantQueryOptions.rank)) {
            ObjectMapper objectMapper = new ObjectMapper();

            DataResult rank = variantManager.rank(query, cliOptions.genericVariantQueryOptions.rank, 10, true, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rank));
        } else {
            if (cliOptions.genericVariantQueryOptions.annotations != null) {
                queryOptions.add("annotations", cliOptions.genericVariantQueryOptions.annotations);
            }
            VariantWriterFactory.VariantOutputFormat outputFormat = VariantWriterFactory
                    .toOutputFormat(cliOptions.commonOptions.outputFormat, cliOptions.outputFileName);
            String outputFile = outdir + "/" + (cliOptions.outputFileName == null ? "" : cliOptions.outputFileName);
            variantManager.exportData(outputFile, outputFormat, cliOptions.variantsFile, query, queryOptions, sessionId);
        }
    }

    private void importData() throws URISyntaxException, AnalysisException {
        VariantCommandOptions.VariantImportCommandOptions importVariantOptions = variantCommandOptions.importVariantCommandOptions;


        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.importData(UriUtils.createUri(importVariantOptions.input), importVariantOptions.study, sessionId);

    }

    private void remove() throws AnalysisException {
        VariantCommandOptions.VariantDeleteCommandOptions cliOptions = variantCommandOptions.variantDeleteCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageOptions.RESUME.key(), cliOptions.genericVariantDeleteOptions.resume);
        options.putAll(cliOptions.commonOptions.params);
        Path outdir = Paths.get(cliOptions.outdir);
        if (cliOptions.genericVariantDeleteOptions.files.size() == 1 && cliOptions.genericVariantDeleteOptions.files.get(0).equalsIgnoreCase(VariantQueryUtils.ALL)) {
            variantManager.removeStudy(cliOptions.study, outdir, options, sessionId);
        } else {
            variantManager.removeFile(cliOptions.study, cliOptions.genericVariantDeleteOptions.files, outdir, options, sessionId);
        }
    }

    private void index() throws AnalysisException {
        VariantCommandOptions.VariantIndexCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LOAD, cliOptions.genericVariantIndexOptions.load);
        queryOptions.put(TRANSFORM, cliOptions.genericVariantIndexOptions.transform);
        queryOptions.put(VariantStorageOptions.STDIN.key(), cliOptions.stdin);
        queryOptions.put(VariantStorageOptions.STDOUT.key(), cliOptions.stdout);
        queryOptions.put(VariantStorageOptions.MERGE_MODE.key(), cliOptions.genericVariantIndexOptions.merge);

        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), cliOptions.genericVariantIndexOptions.calculateStats);
        queryOptions.put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), cliOptions.genericVariantIndexOptions.includeExtraFields);
        queryOptions.put(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), cliOptions.genericVariantIndexOptions.excludeGenotype);
        queryOptions.put(VariantStorageOptions.STATS_AGGREGATION.key(), cliOptions.genericVariantIndexOptions.aggregated);
        queryOptions.put(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), cliOptions.genericVariantIndexOptions.aggregationMappingFile);
        queryOptions.put(VariantStorageOptions.GVCF.key(), cliOptions.genericVariantIndexOptions.gvcf);

        queryOptions.putIfNotNull(VariantFileIndexerStorageOperation.TRANSFORMED_FILES, cliOptions.transformedPaths);

        queryOptions.put(VariantStorageOptions.ANNOTATE.key(), cliOptions.genericVariantIndexOptions.annotate);
        if (cliOptions.genericVariantIndexOptions.annotator != null) {
            queryOptions.put(VariantStorageOptions.ANNOTATOR.key(),
                    cliOptions.genericVariantIndexOptions.annotator);
        }
        queryOptions.put(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), cliOptions.genericVariantIndexOptions.overwriteAnnotations);
        queryOptions.put(VariantStorageOptions.RESUME.key(), cliOptions.genericVariantIndexOptions.resume);
        queryOptions.put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), cliOptions.genericVariantIndexOptions.loadSplitData);
        queryOptions.put(VariantStorageOptions.POST_LOAD_CHECK_SKIP.key(), cliOptions.genericVariantIndexOptions.skipPostLoadCheck);
        queryOptions.put(VariantStorageOptions.INDEX_SEARCH.key(), cliOptions.genericVariantIndexOptions.indexSearch);
        queryOptions.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.index(cliOptions.study, cliOptions.fileId, cliOptions.outdir, queryOptions, sessionId);
    }

    private void secondaryIndex() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException, VariantSearchException, AnalysisException {
        VariantCommandOptions.VariantSecondaryIndexCommandOptions cliOptions = variantCommandOptions.variantSecondaryIndexCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        if (StringUtils.isNotEmpty(cliOptions.sample)) {
            variantManager.searchIndexSamples(cliOptions.study, Arrays.asList(cliOptions.sample.split(",")), queryOptions, sessionId);
        } else {
            variantManager.searchIndex(cliOptions.project, cliOptions.region, cliOptions.overwrite, Paths.get(cliOptions.outdir),
                    new ObjectMap(cliOptions.commonOptions.params), sessionId);
        }
    }

    private void secondaryIndexRemove() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException, VariantSearchException {
        VariantCommandOptions.VariantSecondaryIndexDeleteCommandOptions cliOptions = variantCommandOptions.variantSecondaryIndexDeleteCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.removeSearchIndexSamples(cliOptions.study, Arrays.asList(cliOptions.sample.split(",")), queryOptions, sessionId);
    }

    private void stats() throws AnalysisException {
        VariantCommandOptions.VariantStatsCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions()
                .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, cliOptions.genericVariantStatsOptions.fileName)
//                .append(AnalysisFileIndexer.CREATE, cliOptions.create)
//                .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                .append(VariantStorageOptions.STATS_OVERWRITE.key(), cliOptions.genericVariantStatsOptions.overwriteStats)
                .append(VariantStorageOptions.STATS_UPDATE.key(), cliOptions.genericVariantStatsOptions.updateStats)
                .append(VariantStorageOptions.STATS_AGGREGATION.key(), cliOptions.genericVariantStatsOptions.aggregated)
                .append(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), cliOptions.genericVariantStatsOptions.aggregationMappingFile)
                .append(VariantStorageOptions.RESUME.key(), cliOptions.genericVariantStatsOptions.resume)
                .append(VariantQueryParam.REGION.key(), cliOptions.genericVariantStatsOptions.region)
                .append(VariantQueryParam.GENE.key(), cliOptions.genericVariantStatsOptions.gene);

        options.putAll(cliOptions.commonOptions.params);

        List<String> cohorts = cliOptions.cohort;
        List<String> samples = cliOptions.samples;
        variantManager.stats(cliOptions.study, cohorts, samples, cliOptions.outdir, cliOptions.index, options, sessionId);
    }

    private void scoreLoad() throws CatalogException, URISyntaxException, StorageEngineException {
        VariantCommandOptions.VariantScoreIndexCommandOptions cliOptions = variantCommandOptions.variantScoreIndexCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions()
                .append(VariantStorageOptions.RESUME.key(), cliOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        URI inputUri = UriUtils.createUri(cliOptions.input, true);

        VariantScoreFormatDescriptor descriptor = new VariantScoreFormatDescriptor();
        for (String column : cliOptions.columns.split(",")) {
            String[] split = column.split("=");
            if (split.length != 2) {
                throw new IllegalArgumentException("Malformed value '" + column + "'. Please, use COLUMN=INDEX");
            }
            int columnIdx = Integer.parseInt(split[1]);
            switch (split[0].toUpperCase()) {
                case "SCORE":
                    descriptor.setScoreColumnIdx(columnIdx);
                    break;
                case "PVALUE":
                    descriptor.setPvalueColumnIdx(columnIdx);
                    break;
                case "VAR":
                    descriptor.setVariantColumnIdx(columnIdx);
                    break;
                case "CHR":
                case "CHROM":
                    descriptor.setChrColumnIdx(columnIdx);
                    break;
                case "POS":
                    descriptor.setPosColumnIdx(columnIdx);
                    break;
                case "REF":
                    descriptor.setRefColumnIdx(columnIdx);
                    break;
                case "ALT":
                    descriptor.setAltColumnIdx(columnIdx);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown column " + split[0].toUpperCase() + ". "
                            + "Known columns are: ['SCORE','PVALUE','VAR','CHROM','POS','REF','ALT']");
            }
        }
        descriptor.checkValid();


        variantManager.loadVariantScore(cliOptions.study.study, inputUri, cliOptions.scoreName,
                cliOptions.cohort1, cliOptions.cohort2, descriptor, options, sessionId);
    }

    private void scoreRemove() throws CatalogException, StorageEngineException {
        VariantCommandOptions.VariantScoreDeleteCommandOptions cliOptions = variantCommandOptions.variantScoreDeleteCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions()
                .append(VariantStorageOptions.RESUME.key(), cliOptions.resume)
                .append(VariantStorageOptions.FORCE.key(), cliOptions.force);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.removeVariantScore(cliOptions.study.study, cliOptions.scoreName, options, sessionId);
    }

    private void sampleIndex()
            throws CatalogException, ClassNotFoundException, StorageEngineException, InstantiationException, IllegalAccessException {
        VariantCommandOptions.SampleIndexCommandOptions cliOptions = variantCommandOptions.sampleIndexCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);

        List<String> samples = Arrays.asList(cliOptions.sample.split(","));

        variantManager.sampleIndexAnnotate(cliOptions.study, samples, options, sessionId);
    }

    private void familyIndex()
            throws AnalysisException {
        VariantCommandOptions.FamilyIndexCommandOptions cliOptions = variantCommandOptions.familyIndexCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions()
                .append("overwrite", cliOptions.overwrite);
        options.putAll(cliOptions.commonOptions.params);

        List<String> families = Arrays.asList(cliOptions.family.split(","));

        variantManager.familyIndex(cliOptions.study, families, Paths.get(cliOptions.outdir), options, sessionId);
    }

    private void annotate() throws AnalysisException {

        VariantCommandOptions.VariantAnnotateCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), cliOptions.genericVariantAnnotateOptions.overwriteAnnotations);
        options.put(VariantAnnotationManager.CREATE, cliOptions.genericVariantAnnotateOptions.create);
        options.putIfNotEmpty(VariantAnnotationManager.LOAD_FILE, cliOptions.genericVariantAnnotateOptions.load);
        options.putIfNotEmpty(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, cliOptions.genericVariantAnnotateOptions.customAnnotationKey);
        options.putIfNotNull(VariantStorageOptions.ANNOTATOR.key(), cliOptions.genericVariantAnnotateOptions.annotator);
        options.putIfNotEmpty(DefaultVariantAnnotationManager.FILE_NAME, cliOptions.genericVariantAnnotateOptions.fileName);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.annotate(cliOptions.project, cliOptions.study, cliOptions.genericVariantAnnotateOptions.region, cliOptions.outdir, options, sessionId);
    }

    private void annotationSave() throws AnalysisException {
        VariantCommandOptions.AnnotationSaveCommandOptions cliOptions = variantCommandOptions.annotationSaveSnapshotCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);


        variantManager.saveAnnotation(cliOptions.project, cliOptions.annotationId, Paths.get(cliOptions.outdir), options, sessionId);
    }

    private void annotationDelete() throws AnalysisException {
        VariantCommandOptions.AnnotationDeleteCommandOptions cliOptions = variantCommandOptions.annotationDeleteCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);


        variantManager.deleteAnnotation(cliOptions.project, cliOptions.annotationId, Paths.get(cliOptions.outdir), options, sessionId);
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

        DataResult<VariantAnnotation> queryResult = variantManager.getAnnotation(cliOptions.annotationId, query, options, sessionId);

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
                variantManager.getAnnotationMetadata(cliOptions.annotationId, cliOptions.project, sessionId);

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

    private void fillGaps() throws StorageEngineException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        VariantCommandOptions.AggregateFamilyCommandOptions cliOptions = variantCommandOptions.fillGapsVariantCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        ObjectMap options = new ObjectMap();
//        options.put("skipReferenceVariants", cliOptions.genericAggregateFamilyOptions.excludeHomRef);
        options.put(VariantStorageOptions.RESUME.key(), cliOptions.genericAggregateFamilyOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.fillGaps(cliOptions.study, cliOptions.genericAggregateFamilyOptions.samples, options, sessionId);
    }

    private void fillMissing() throws StorageEngineException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        VariantCommandOptions.AggregateCommandOptions cliOptions = variantCommandOptions.aggregateCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        ObjectMap options = new ObjectMap();
        options.put(VariantStorageOptions.RESUME.key(), cliOptions.aggregateCommandOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.fillMissing(cliOptions.study, cliOptions.aggregateCommandOptions.overwrite, options, sessionId);
    }

    private void samples() throws Exception {

        VariantCommandOptions.VariantSamplesFilterCommandOptions cliOptions = variantCommandOptions.samplesFilterCommandOptions;

//        Map<Long, String> studyIds = getStudyIds(sessionId);
        Query query = VariantQueryCommandUtils.parseBasicVariantQuery(cliOptions.variantQueryOptions, new Query());

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        VariantSampleFilter variantSampleFilter = new VariantSampleFilter(variantManager.iterable(sessionId));

        if (StringUtils.isNotEmpty(cliOptions.samples)) {
            query.append(VariantQueryParam.INCLUDE_SAMPLE.key(), Arrays.asList(cliOptions.samples.split(",")));
        }
        if (StringUtils.isNotEmpty(cliOptions.study)) {
            query.append(VariantQueryParam.STUDY.key(), cliOptions.study);
        }

        List<String> genotypes = Arrays.asList(cliOptions.genotypes.split(","));
        if (cliOptions.all) {
            Collection<String> samplesInAllVariants = variantSampleFilter.getSamplesInAllVariants(query, genotypes);
            System.out.println("##Samples in ALL variants with genotypes " + genotypes);
            for (String sample : samplesInAllVariants) {
                System.out.println(sample);
            }
        } else {
            Map<String, Set<Variant>> samplesInAnyVariants = variantSampleFilter.getSamplesInAnyVariants(query, genotypes);
            System.out.println("##Samples in ANY variants with genotypes " + genotypes);
            Set<Variant> variants = new TreeSet<>((v1, o2) -> v1.getStart().compareTo(o2.getStart()));
            samplesInAnyVariants.forEach((sample, v) -> variants.addAll(v));

            System.out.print(StringUtils.rightPad("#SAMPLE", 10));
//            System.out.print("|");
            for (Variant variant : variants) {
                System.out.print(StringUtils.center(variant.toString(), 15));
//                System.out.print("|");
            }
            System.out.println();
            samplesInAnyVariants.forEach((sample, v) -> {
                System.out.print(StringUtils.rightPad(sample, 10));
//                System.out.print("|");
                for (Variant variant : variants) {
                    if (v.contains(variant)) {
                        System.out.print(StringUtils.center("X", 15));
                    } else {
                        System.out.print(StringUtils.center("-", 15));
                    }
//                    System.out.print("|");
                }
                System.out.println();
            });

        }
    }

    private void histogram() throws Exception {
        VariantCommandOptions.VariantHistogramCommandOptions cliOptions = variantCommandOptions.histogramCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);
        params.put(VariantHistogramAnalysis.INTERVAL, cliOptions.interval.toString());
        params.put(VariantHistogramAnalysis.OUTDIR, cliOptions.outdir);
        Query query = VariantQueryCommandUtils.parseBasicVariantQuery(cliOptions.variantQueryOptions, new Query());
        params.putAll(query);

        String userId1 = catalogManager.getUserManager().getUserId(sessionId);
        new PluginExecutor(catalogManager, sessionId)
                .execute(VariantHistogramAnalysis.class, "default", catalogManager.getStudyManager().resolveId(cliOptions.study, userId1).getUid(), params);
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
        gwasAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir), sessionId);
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
        sampleVariantStatsAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir), sessionId);
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
        cohortVariantStatsAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir), sessionId);
        cohortVariantStatsAnalysis.setStudy(cliOptions.study)
                .setCohortName(cliOptions.cohort)
                .setIndexResults(cliOptions.index)
                .setSamplesQuery(query)
                .setSampleNames(sampleNames)
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
        plink.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir), sessionId);

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
        rvtests.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir), sessionId);

        rvtests.start();
    }
}
