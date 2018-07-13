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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.old.AnalysisExecutionException;
import org.opencb.opencga.analysis.old.execution.plugins.PluginExecutor;
import org.opencb.opencga.analysis.old.execution.plugins.hist.VariantHistogramAnalysis;
import org.opencb.opencga.analysis.old.execution.plugins.ibs.IbsAnalysis;
import org.opencb.opencga.app.cli.analysis.options.VariantCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.analysis.VariantSampleFilter;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.json.mixin.GenericRecordAvroJsonMixin;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.CreateAnnotationSnapshotCommandOptions.COPY_ANNOTATION_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.DeleteAnnotationSnapshotCommandOptions.DELETE_ANNOTATION_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillGapsCommandOptions.FILL_GAPS_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.FillMissingCommandOptions.FILL_MISSING_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.QueryAnnotationCommandOptions.QUERY_ANNOTATION_COMMAND;
import static org.opencb.opencga.storage.app.cli.client.options.StorageVariantCommandOptions.VariantRemoveCommandOptions.VARIANT_REMOVE_COMMAND;
import static org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation.LOAD;
import static org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation.TRANSFORM;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends AnalysisCommandExecutor {

    //    private AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions;
    private VariantCommandOptions variantCommandOptions;
    private VariantStorageEngine variantStorageEngine;

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
            case "index-search":
                indexSearch();
                break;
            case "stats":
                stats();
                break;
            case "annotate":
                annotate();
                break;
            case COPY_ANNOTATION_COMMAND:
                copyAnnotation();
                break;
            case DELETE_ANNOTATION_COMMAND:
                deleteAnnotation();
                break;
            case QUERY_ANNOTATION_COMMAND:
                queryAnnotation();
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
        queryCliOptions.genericVariantQueryOptions.returnStudy = exportCliOptions.study;
        queryCliOptions.numericOptions.limit = exportCliOptions.numericOptions.limit;
//        queryCliOptions.sort = true;
        queryCliOptions.numericOptions.skip = exportCliOptions.numericOptions.skip;
        queryCliOptions.genericVariantQueryOptions.region = exportCliOptions.region;
        queryCliOptions.genericVariantQueryOptions.regionFile = exportCliOptions.regionFile;
        queryCliOptions.output = exportCliOptions.output;
        queryCliOptions.genericVariantQueryOptions.gene = exportCliOptions.gene;
        queryCliOptions.numericOptions.count = exportCliOptions.numericOptions.count;
        queryCliOptions.genericVariantQueryOptions.returnSample = VariantQueryUtils.NONE;
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
            QueryResult<Long> result = variantManager.count(query, sessionId);
            System.out.println("Num. results\t" + result.getResult().get(0));
        } else if (StringUtils.isNotEmpty(cliOptions.genericVariantQueryOptions.groupBy)) {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryResult groupBy = variantManager.groupBy(cliOptions.genericVariantQueryOptions.groupBy, query, queryOptions, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
        } else if (StringUtils.isNotEmpty(cliOptions.genericVariantQueryOptions.rank)) {
            ObjectMapper objectMapper = new ObjectMapper();

            QueryResult rank = variantManager.rank(query, cliOptions.genericVariantQueryOptions.rank, 10, true, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rank));
        } else {
            if (cliOptions.genericVariantQueryOptions.annotations != null) {
                queryOptions.add("annotations", cliOptions.genericVariantQueryOptions.annotations);
            }
            VariantWriterFactory.VariantOutputFormat outputFormat = VariantWriterFactory
                    .toOutputFormat(cliOptions.commonOptions.outputFormat, cliOptions.output);
            variantManager.exportData(cliOptions.output, outputFormat, query, queryOptions, sessionId);
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
        options.put(VariantStorageEngine.Options.RESUME.key(), cliOptions.genericVariantRemoveOptions.resume);
        options.putAll(cliOptions.commonOptions.params);
        if (cliOptions.genericVariantRemoveOptions.files.size() == 1 && cliOptions.genericVariantRemoveOptions.files.get(0).equalsIgnoreCase(VariantQueryUtils.ALL)) {
            variantManager.removeStudy(cliOptions.study, sessionId, options);
        } else {
            List<File> removedFiles = variantManager.removeFile(cliOptions.genericVariantRemoveOptions.files, cliOptions.study, sessionId, options);
        }
    }

    private void index() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException {
        VariantCommandOptions.VariantIndexCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LOAD, cliOptions.genericVariantIndexOptions.load);
        queryOptions.put(TRANSFORM, cliOptions.genericVariantIndexOptions.transform);
        queryOptions.put(VariantStorageEngine.Options.MERGE_MODE.key(), cliOptions.genericVariantIndexOptions.merge);

        queryOptions.put(VariantStorageEngine.Options.CALCULATE_STATS.key(), cliOptions.genericVariantIndexOptions.calculateStats);
        queryOptions.put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.genericVariantIndexOptions.extraFields);
        queryOptions.put(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), cliOptions.genericVariantIndexOptions.excludeGenotype);
        queryOptions.put(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), cliOptions.genericVariantIndexOptions.aggregated);
        queryOptions.put(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.genericVariantIndexOptions.aggregationMappingFile);
        queryOptions.put(VariantStorageEngine.Options.GVCF.key(), cliOptions.genericVariantIndexOptions.gvcf);

        queryOptions.putIfNotNull(StorageOperation.CATALOG_PATH, cliOptions.catalogPath);
        queryOptions.putIfNotNull(VariantFileIndexerStorageOperation.TRANSFORMED_FILES, cliOptions.transformedPaths);

        queryOptions.put(VariantStorageEngine.Options.ANNOTATE.key(), cliOptions.genericVariantIndexOptions.annotate);
        if (cliOptions.genericVariantIndexOptions.annotator != null) {
            queryOptions.put(VariantAnnotationManager.ANNOTATOR,
                    cliOptions.genericVariantIndexOptions.annotator);
        }
        queryOptions.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.genericVariantIndexOptions.overwriteAnnotations);
        queryOptions.put(VariantStorageEngine.Options.RESUME.key(), cliOptions.genericVariantIndexOptions.resume);
        queryOptions.put(VariantStorageEngine.Options.LOAD_SPLIT_DATA.key(), cliOptions.genericVariantIndexOptions.loadSplitData);
        queryOptions.put(VariantStorageEngine.Options.POST_LOAD_CHECK_SKIP.key(), cliOptions.genericVariantIndexOptions.skipPostLoadCheck);
        queryOptions.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        variantManager.index(cliOptions.study, cliOptions.fileId, cliOptions.outdir, queryOptions, sessionId);
    }

    private void indexSearch() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException, VariantSearchException {
        VariantCommandOptions.VariantIndexSearchCommandOptions cliOptions = variantCommandOptions.variantIndexSearchCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.putAll(cliOptions.commonOptions.params);

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        String project = StringUtils.isEmpty(cliOptions.project) ? cliOptions.projectId : cliOptions.project;

        Query query = new Query();
        query.putIfNotEmpty(VariantCatalogQueryUtils.PROJECT.key(), project);
        query.putIfNotEmpty(VariantQueryParam.STUDY.key(), cliOptions.study);
        query.putIfNotEmpty(VariantQueryParam.REGION.key(), cliOptions.region);
        query.putIfNotEmpty(VariantQueryParam.GENE.key(), cliOptions.gene);
        query.putIfNotEmpty(VariantQueryParam.SAMPLE.key(), cliOptions.sample);
        query.putIfNotEmpty(VariantQueryParam.FILE.key(), cliOptions.file);
        query.putIfNotEmpty(VariantQueryParam.COHORT.key(), cliOptions.cohort);
        variantManager.searchIndex(query, queryOptions, sessionId);
    }

    private void stats() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageEngineException, InstantiationException, IllegalAccessException, URISyntaxException {
        VariantCommandOptions.VariantStatsCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions()
                .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, cliOptions.genericVariantStatsOptions.fileName)
//                .append(AnalysisFileIndexer.CREATE, cliOptions.create)
//                .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                .append(VariantStorageEngine.Options.OVERWRITE_STATS.key(), cliOptions.genericVariantStatsOptions.overwriteStats)
                .append(VariantStorageEngine.Options.UPDATE_STATS.key(), cliOptions.genericVariantStatsOptions.updateStats)
                .append(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), cliOptions.genericVariantStatsOptions.aggregated)
                .append(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.genericVariantStatsOptions.aggregationMappingFile)
                .append(VariantStorageEngine.Options.RESUME.key(), cliOptions.genericVariantStatsOptions.resume)
                .append(VariantQueryParam.REGION.key(), cliOptions.genericVariantStatsOptions.region)
                .append(StorageOperation.CATALOG_PATH, cliOptions.catalogPath);

        options.putAll(cliOptions.commonOptions.params);

        List<String> cohorts;
        if (StringUtils.isNotBlank(cliOptions.cohortIds)) {
            cohorts = Arrays.asList(cliOptions.cohortIds.split(","));
        } else {
            cohorts = Collections.emptyList();
        }

        variantManager.stats(cliOptions.study, cohorts, cliOptions.outdir, options, sessionId);
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
        options.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.genericVariantAnnotateOptions.overwriteAnnotations);
        options.put(VariantAnnotationManager.CREATE, cliOptions.genericVariantAnnotateOptions.create);
        options.putIfNotEmpty(VariantAnnotationManager.LOAD_FILE, cliOptions.genericVariantAnnotateOptions.load);
        options.putIfNotEmpty(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, cliOptions.genericVariantAnnotateOptions.customAnnotationKey);
        options.putIfNotNull(VariantAnnotationManager.ANNOTATOR, cliOptions.genericVariantAnnotateOptions.annotator);
        options.putIfNotEmpty(DefaultVariantAnnotationManager.FILE_NAME, cliOptions.genericVariantAnnotateOptions.fileName);
        options.put(StorageOperation.CATALOG_PATH, cliOptions.catalogPath);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.annotate(cliOptions.project, cliOptions.study, query, cliOptions.outdir, options, sessionId);
    }

    private void copyAnnotation() throws IllegalAccessException, StorageEngineException, InstantiationException, VariantAnnotatorException, CatalogException, ClassNotFoundException {
        VariantCommandOptions.CreateAnnotationSnapshotCommandOptions cliOptions = variantCommandOptions.createAnnotationSnapshotCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);


        variantManager.createAnnotationSnapshot(cliOptions.project, cliOptions.name, options, sessionId);
    }

    private void deleteAnnotation() throws IllegalAccessException, StorageEngineException, InstantiationException, VariantAnnotatorException, CatalogException, ClassNotFoundException {
        VariantCommandOptions.CreateAnnotationSnapshotCommandOptions cliOptions = variantCommandOptions.createAnnotationSnapshotCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        QueryOptions options = new QueryOptions();
        options.putAll(cliOptions.commonOptions.params);


        variantManager.deleteAnnotationSnapshot(cliOptions.project, cliOptions.name, options, sessionId);
    }

    private void queryAnnotation() throws CatalogException, IOException, StorageEngineException {
        VariantCommandOptions.QueryAnnotationCommandOptions cliOptions = variantCommandOptions.queryAnnotationCommandOptions;
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

        QueryResult<VariantAnnotation> queryResult = variantManager.getAnnotation(cliOptions.name, query, options, sessionId);

        // WRITE
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.addMixIn(GenericRecord.class, GenericRecordAvroJsonMixin.class);
        objectMapper.configure(SerializationFeature.CLOSE_CLOSEABLE, false);
        ObjectWriter writer = objectMapper.writer();
//        ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
        SequenceWriter sequenceWriter = writer.writeValues(System.out);
        for (VariantAnnotation annotation : queryResult.getResult()) {
            sequenceWriter.write(annotation);
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
        options.put(VariantStorageEngine.Options.RESUME.key(), cliOptions.genericFillGapsOptions.resume);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.fillGaps(cliOptions.study, cliOptions.genericFillGapsOptions.samples, options, sessionId);
    }

    private void fillMissing() throws StorageEngineException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        VariantCommandOptions.FillMissingCommandOptions cliOptions = variantCommandOptions.fillMissingCommandOptions;
        VariantStorageManager variantManager = new VariantStorageManager(catalogManager, storageEngineFactory);

        ObjectMap options = new ObjectMap();
        options.put(VariantStorageEngine.Options.RESUME.key(), cliOptions.fillMissingCommandOptions.resume);
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
}
