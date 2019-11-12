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

package org.opencb.opencga.app.cli.analysis.executors;


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
import org.opencb.opencga.analysis.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.File;
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
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.operations.VariantFileIndexerStorageOperation.LOAD;
import static org.opencb.opencga.analysis.variant.operations.VariantFileIndexerStorageOperation.TRANSFORM;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.CohortVariantStatsCommandOptions.COHORT_VARIANT_STATS_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.FamilyIndexCommandOptions.FAMILY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.SampleIndexCommandOptions.SAMPLE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.SampleVariantStatsCommandOptions.SAMPLE_VARIANT_STATS_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantScoreIndexCommandOptions.SCORE_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantScoreRemoveCommandOptions.SCORE_REMOVE_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantSecondaryIndexCommandOptions.SECONDARY_INDEX_COMMAND;
import static org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions.VariantSecondaryIndexRemoveCommandOptions.SECONDARY_INDEX_REMOVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillGapsCommandOptions.FILL_GAPS_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillMissingCommandOptions.FILL_MISSING_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationDeleteCommandOptions.ANNOTATION_DELETE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationMetadataCommandOptions.ANNOTATION_METADATA_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationQueryCommandOptions.ANNOTATION_QUERY_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.GenericAnnotationSaveCommandOptions.ANNOTATION_SAVE_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends AnalysisCommandExecutor {

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
            case VARIANT_REMOVE_COMMAND:
                remove();
                break;
            case "query":
                query();
                break;
            case "export-frequencies":
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
            case SECONDARY_INDEX_REMOVE_COMMAND:
                secondaryIndexRemove();
                break;
            case "stats":
                stats();
                break;
            case SCORE_INDEX_COMMAND:
                scoreLoad();
                break;
            case SCORE_REMOVE_COMMAND:
                scoreRemove();
                break;
            case SAMPLE_INDEX_COMMAND:
                sampleIndex();
                break;
            case FAMILY_INDEX_COMMAND:
                familyIndex();
                break;
            case "annotate":
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
            case FILL_GAPS_COMMAND:
                fillGaps();
                break;
            case FILL_MISSING_COMMAND:
                fillMissing();
                break;
            case "samples":
                samples();
                break;
            case "histogram":
                histogram();
                break;
            case GwasAnalysis.ID:
                gwas();
                break;
            case SAMPLE_VARIANT_STATS_COMMAND:
                sampleStats();
                break;
            case COHORT_VARIANT_STATS_COMMAND:
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
        queryCliOptions.output = exportCliOptions.output;
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

    private void query() throws Exception {
//        AnalysisCliOptionsParser.QueryVariantCommandOptions cliOptions = variantCommandOptions.queryVariantCommandOptions;
        VariantCommandOptions.VariantQueryCommandOptions cliOptions = variantCommandOptions.queryVariantCommandOptions;

        Map<Long, String> studyIds = getStudyIds(sessionId);
        Query query = VariantQueryCommandUtils.parseQuery(cliOptions, studyIds, clientConfiguration);
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
                    .toOutputFormat(cliOptions.commonOptions.outputFormat, cliOptions.output);
            variantManager.exportData(cliOptions.output, outputFormat, cliOptions.variantsFile, query, queryOptions, sessionId);
        }
    }

    private void importData() throws URISyntaxException, CatalogException, StorageEngineException, IOException {
        VariantCommandOptions.VariantImportCommandOptions importVariantOptions = variantCommandOptions.importVariantCommandOptions;


        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.importData(UriUtils.createUri(importVariantOptions.input), importVariantOptions.study, sessionId);

    }

    private void remove() throws CatalogException, StorageEngineException, IOException {
        VariantCommandOptions.VariantRemoveCommandOptions cliOptions = variantCommandOptions.variantRemoveCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageOptions.RESUME.key(), cliOptions.genericVariantRemoveOptions.resume);
        options.putAll(cliOptions.commonOptions.params);
        if (cliOptions.genericVariantRemoveOptions.files.size() == 1 && cliOptions.genericVariantRemoveOptions.files.get(0).equalsIgnoreCase(VariantQueryUtils.ALL)) {
            variantManager.removeStudy(cliOptions.study, sessionId, options);
        } else {
            List<File> removedFiles = variantManager.removeFile(cliOptions.genericVariantRemoveOptions.files, cliOptions.study, sessionId, options);
        }
    }

    private void index() throws AnalysisException {
        VariantCommandOptions.VariantIndexCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LOAD, cliOptions.genericVariantIndexOptions.load);
        queryOptions.put(TRANSFORM, cliOptions.genericVariantIndexOptions.transform);
        queryOptions.put(VariantStorageOptions.STDIN.key(), cliOptions.genericVariantIndexOptions.stdin);
        queryOptions.put(VariantStorageOptions.STDOUT.key(), cliOptions.genericVariantIndexOptions.stdout);
        queryOptions.put(VariantStorageOptions.MERGE_MODE.key(), cliOptions.genericVariantIndexOptions.merge);

        queryOptions.put(VariantStorageOptions.STATS_CALCULATE.key(), cliOptions.genericVariantIndexOptions.calculateStats);
        queryOptions.put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), cliOptions.genericVariantIndexOptions.extraFields);
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
            InstantiationException, IllegalAccessException, URISyntaxException, VariantSearchException {
        VariantCommandOptions.VariantSecondaryIndexCommandOptions cliOptions = variantCommandOptions.variantSecondaryIndexCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        if (StringUtils.isNotEmpty(cliOptions.sample)) {
            variantManager.searchIndexSamples(cliOptions.study, Arrays.asList(cliOptions.sample.split(",")), queryOptions, sessionId);
        } else {
            Query query = new Query();
            query.putIfNotEmpty(VariantCatalogQueryUtils.PROJECT.key(), cliOptions.project);
            query.putIfNotEmpty(VariantQueryParam.REGION.key(), cliOptions.region);
            variantManager.searchIndex(query, queryOptions, cliOptions.overwrite, sessionId);
        }
    }

    private void secondaryIndexRemove() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException, VariantSearchException {
        VariantCommandOptions.VariantSecondaryIndexRemoveCommandOptions cliOptions = variantCommandOptions.variantSecondaryIndexRemoveCommandOptions;

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
                .append(VariantQueryParam.REGION.key(), cliOptions.genericVariantStatsOptions.region);

        options.putAll(cliOptions.commonOptions.params);

        List<String> cohorts;
        if (StringUtils.isNotBlank(cliOptions.cohortIds)) {
            cohorts = Arrays.asList(cliOptions.cohortIds.split(","));
        } else {
            cohorts = Collections.emptyList();
        }
        List<String> samples;
        if (StringUtils.isNotBlank(cliOptions.samples)) {
            samples = Arrays.asList(cliOptions.samples.split(","));
        } else {
            samples = Collections.emptyList();
        }
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


        variantManager.loadVariantScore(cliOptions.study, inputUri, cliOptions.scoreName,
                cliOptions.cohort1, cliOptions.cohort2, descriptor, options, sessionId);
    }

    private void scoreRemove() throws CatalogException, StorageEngineException {
        VariantCommandOptions.VariantScoreRemoveCommandOptions cliOptions = variantCommandOptions.variantScoreRemoveCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions()
                .append(VariantStorageOptions.RESUME.key(), cliOptions.resume)
                .append(VariantStorageOptions.FORCE.key(), cliOptions.force);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.removeVariantScore(cliOptions.study, cliOptions.scoreName, options, sessionId);
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
            throws CatalogException, ClassNotFoundException, StorageEngineException, InstantiationException, IllegalAccessException {
        VariantCommandOptions.FamilyIndexCommandOptions cliOptions = variantCommandOptions.familyIndexCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions()
                .append("overwrite", cliOptions.overwrite);
        options.putAll(cliOptions.commonOptions.params);

        List<String> families = Arrays.asList(cliOptions.family.split(","));

        variantManager.familyIndex(cliOptions.study, families, options, sessionId);
    }

    private void annotate() throws StorageEngineException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        VariantCommandOptions.VariantAnnotateCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        Query query = new Query()
                .append(VariantQueryParam.REGION.key(), cliOptions.genericVariantAnnotateOptions.filterRegion)
                .append(VariantQueryParam.GENE.key(), cliOptions.genericVariantAnnotateOptions.filterGene)
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), cliOptions.genericVariantAnnotateOptions.filterAnnotConsequenceType);

        QueryOptions options = new QueryOptions();
        options.put(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), cliOptions.genericVariantAnnotateOptions.overwriteAnnotations);
        options.put(VariantAnnotationManager.CREATE, cliOptions.genericVariantAnnotateOptions.create);
        options.putIfNotEmpty(VariantAnnotationManager.LOAD_FILE, cliOptions.genericVariantAnnotateOptions.load);
        options.putIfNotEmpty(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, cliOptions.genericVariantAnnotateOptions.customAnnotationKey);
        options.putIfNotNull(VariantStorageOptions.ANNOTATOR.key(), cliOptions.genericVariantAnnotateOptions.annotator);
        options.putIfNotEmpty(DefaultVariantAnnotationManager.FILE_NAME, cliOptions.genericVariantAnnotateOptions.fileName);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.annotate(cliOptions.project, cliOptions.study, query, cliOptions.outdir, options, sessionId);
    }

    private void annotationSave() throws IllegalAccessException, StorageEngineException, InstantiationException, VariantAnnotatorException, CatalogException, ClassNotFoundException {
        VariantCommandOptions.AnnotationSaveCommandOptions cliOptions = variantCommandOptions.annotationSaveSnapshotCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);


        variantManager.saveAnnotation(cliOptions.project, cliOptions.annotationId, options, sessionId);
    }

    private void annotationDelete() throws IllegalAccessException, StorageEngineException, InstantiationException, VariantAnnotatorException, CatalogException, ClassNotFoundException {
        VariantCommandOptions.AnnotationSaveCommandOptions cliOptions = variantCommandOptions.annotationSaveSnapshotCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);


        variantManager.deleteAnnotation(cliOptions.project, cliOptions.annotationId, options, sessionId);
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

        VariantCommandOptions.FillGapsCommandOptions cliOptions = variantCommandOptions.fillGapsVariantCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        ObjectMap options = new ObjectMap();
//        options.put("skipReferenceVariants", cliOptions.genericFillGapsOptions.excludeHomRef);
        options.put(VariantStorageOptions.RESUME.key(), cliOptions.genericFillGapsOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.fillGaps(cliOptions.study, cliOptions.genericFillGapsOptions.samples, options, sessionId);
    }

    private void fillMissing() throws StorageEngineException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        VariantCommandOptions.FillMissingCommandOptions cliOptions = variantCommandOptions.fillMissingCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        ObjectMap options = new ObjectMap();
        options.put(VariantStorageOptions.RESUME.key(), cliOptions.fillMissingCommandOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.fillMissing(cliOptions.study, cliOptions.fillMissingCommandOptions.overwrite, options, sessionId);
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
        if (StringUtils.isNotEmpty(cliOptions.caseSamplesAnnotation)) {
            caseCohortSamplesQuery = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY.key(), cliOptions.study)
                    .append(SampleDBAdaptor.QueryParams.ANNOTATION.key(), cliOptions.caseSamplesAnnotation);
        }
        Query controlCohortSamplesQuery = null;
        if (StringUtils.isNotEmpty(cliOptions.controlSamplesAnnotation)) {
            controlCohortSamplesQuery = new Query()
                    .append(SampleDBAdaptor.QueryParams.STUDY.key(), cliOptions.study)
                    .append(SampleDBAdaptor.QueryParams.ANNOTATION.key(), cliOptions.controlSamplesAnnotation);
        }
        GwasAnalysis gwasAnalysis = new GwasAnalysis();
        gwasAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir), sessionId);
        gwasAnalysis.setStudy(cliOptions.study)
                .setPhenotype(cliOptions.phenotype)
                .setScoreName(cliOptions.scoreName)
                .setFisherMode(cliOptions.fisherMode)
                .setGwasMethod(cliOptions.method)
                .setControlCohort(cliOptions.controlCohort)
                .setCaseCohort(cliOptions.caseCohort)
                .setCaseCohortSamplesQuery(caseCohortSamplesQuery)
                .setControlCohortSamplesQuery(controlCohortSamplesQuery)
                .start();
    }

    private void sampleStats() throws Exception {
        VariantCommandOptions.SampleVariantStatsCommandOptions cliOptions = variantCommandOptions.sampleVariantStatsCommandOptions;
        ObjectMap params = new ObjectMap();
        params.putAll(cliOptions.commonOptions.params);

        List<String> sampleNames;
        if (StringUtils.isNotBlank(cliOptions.samples)) {
            sampleNames = Arrays.asList(cliOptions.samples.split(","));
        } else {
            sampleNames = null;
        }

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
                .setSampleNames(sampleNames)
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

        List<String> sampleNames;
        if (StringUtils.isNotBlank(cliOptions.samples)) {
            sampleNames = Arrays.asList(cliOptions.samples.split(","));
        } else {
            sampleNames = null;
        }

        CohortVariantStatsAnalysis cohortVariantStatsAnalysis = new CohortVariantStatsAnalysis();
        cohortVariantStatsAnalysis.setUp(appHome, catalogManager, storageEngineFactory, params, Paths.get(cliOptions.outdir), sessionId);
        cohortVariantStatsAnalysis.setStudy(cliOptions.study)
                .setCohortName(cliOptions.cohort)
                .setIndexResults(cliOptions.index)
                .setSamplesQuery(query)
                .setSampleNames(sampleNames)
                .start();
    }
}
