/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.app.cli.analysis;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.AnalysisExecutionException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation.LOAD;
import static org.opencb.opencga.storage.core.manager.variant.operations.VariantFileIndexerStorageOperation.TRANSFORM;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends AnalysisStorageCommandExecutor {

    private AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions;
    private VariantStorageEngine variantStorageManager;

    public VariantCommandExecutor(AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions) {
        super(variantCommandOptions.commonOptions);
        this.variantCommandOptions = variantCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing variant command line");

        String subCommandString = variantCommandOptions.getParsedSubCommand();
        configure();

        sessionId = getSessionId(variantCommandOptions.commonOptions);

        switch (subCommandString) {
            case "ibs":
                ibs();
                break;
            case "delete":
                delete();
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
            case "stats":
                stats();
                break;
            case "annotate":
                annotate();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }


    private VariantStorageEngine initVariantStorageManager(DataStore dataStore)
            throws CatalogException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        String storageEngine = dataStore.getStorageEngine();
        if (isEmpty(storageEngine)) {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager();
        } else {
            this.variantStorageManager = storageManagerFactory.getVariantStorageManager(storageEngine);
        }
        return variantStorageManager;
    }


    private void ibs() {
        throw new UnsupportedOperationException();
    }


    private void exportFrequencies() throws Exception {

        AnalysisCliOptionsParser.ExportVariantStatsCommandOptions exportCliOptions = variantCommandOptions.exportVariantStatsCommandOptions;
        AnalysisCliOptionsParser.QueryVariantCommandOptions queryCliOptions = variantCommandOptions.queryVariantCommandOptions;

        queryCliOptions.commonOptions.outputFormat = exportCliOptions.commonOptions.outputFormat.toLowerCase().replace("tsv", "stats");
        queryCliOptions.study = exportCliOptions.studies;
        queryCliOptions.returnStudy = exportCliOptions.studies;
        queryCliOptions.limit = exportCliOptions.queryOptions.limit;
        queryCliOptions.sort = true;
        queryCliOptions.skip = exportCliOptions.queryOptions.skip;
        queryCliOptions.region = exportCliOptions.queryOptions.region;
        queryCliOptions.regionFile = exportCliOptions.queryOptions.regionFile;
        queryCliOptions.output = exportCliOptions.queryOptions.output;
        queryCliOptions.gene = exportCliOptions.queryOptions.gene;
        queryCliOptions.count = exportCliOptions.queryOptions.count;
        queryCliOptions.returnSample = "";

        query();
    }

    private void query() throws Exception {

        AnalysisCliOptionsParser.QueryVariantCommandOptions cliOptions = variantCommandOptions.queryVariantCommandOptions;

        Map<Long, String> studyIds = getStudyIds(sessionId);
        Query query = VariantQueryCommandUtils.parseQuery(cliOptions, studyIds);
        QueryOptions queryOptions = VariantQueryCommandUtils.parseQueryOptions(cliOptions);

        org.opencb.opencga.storage.core.manager.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.manager.variant.VariantStorageManager(catalogManager, storageManagerFactory);

        if (cliOptions.count) {
            QueryResult<Long> result = variantManager.count(query, sessionId);
            System.out.println("Num. results\t" + result.getResult().get(0));
        } else if (StringUtils.isNotEmpty(cliOptions.groupBy)) {
            ObjectMapper objectMapper = new ObjectMapper();
            QueryResult groupBy = variantManager.groupBy(cliOptions.groupBy, query, queryOptions, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(groupBy));
        } else if (StringUtils.isNotEmpty(cliOptions.rank)) {
            ObjectMapper objectMapper = new ObjectMapper();

            QueryResult rank = variantManager.rank(query, cliOptions.rank, 10, true, sessionId);
            System.out.println("rank = " + objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rank));
        } else {
            if (cliOptions.annotations != null) {
                queryOptions.add("annotations", cliOptions.annotations);
            }
            VariantWriterFactory.VariantOutputFormat outputFormat = VariantWriterFactory
                    .toOutputFormat(cliOptions.commonOptions.outputFormat, cliOptions.output);
            variantManager.exportData(cliOptions.output, outputFormat, query, queryOptions, sessionId);
        }
    }

    private void importData() throws URISyntaxException, CatalogException, StorageEngineException, IOException {
        AnalysisCliOptionsParser.ImportVariantCommandOptions importVariantOptions = variantCommandOptions.importVariantCommandOptions;


        org.opencb.opencga.storage.core.manager.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.manager.variant.VariantStorageManager(catalogManager, storageManagerFactory);

        variantManager.importData(UriUtils.createUri(importVariantOptions.input), importVariantOptions.study, sessionId);

    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

    private void index() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageEngineException,
            InstantiationException, IllegalAccessException, URISyntaxException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LOAD, variantCommandOptions.indexVariantCommandOptions.load);
        queryOptions.put(TRANSFORM, variantCommandOptions.indexVariantCommandOptions.transform);

        queryOptions.put(VariantStorageEngine.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
        queryOptions.put(VariantStorageEngine.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
        queryOptions.put(VariantStorageEngine.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype);
        queryOptions.put(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);
        queryOptions.put(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.aggregationMappingFile);
        queryOptions.put(VariantStorageEngine.Options.GVCF.key(), cliOptions.gvcf);

        queryOptions.putIfNotNull(StorageOperation.CATALOG_PATH, cliOptions.catalogPath);
        queryOptions.putIfNotNull(VariantFileIndexerStorageOperation.TRANSFORMED_FILES, cliOptions.transformedPaths);

        queryOptions.put(VariantStorageEngine.Options.ANNOTATE.key(), cliOptions.annotate);
        if (cliOptions.annotator != null) {
            queryOptions.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        }
        queryOptions.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);
        queryOptions.put(VariantStorageEngine.Options.RESUME.key(), cliOptions.resume);
        queryOptions.putAll(cliOptions.commonOptions.params);

        org.opencb.opencga.storage.core.manager.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.manager.variant.VariantStorageManager(catalogManager, storageManagerFactory);

        variantManager.index(cliOptions.study, cliOptions.fileId, cliOptions.outdir, queryOptions, sessionId);

    }

    private void stats() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageEngineException, InstantiationException, IllegalAccessException, URISyntaxException {
        AnalysisCliOptionsParser.StatsVariantCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        org.opencb.opencga.storage.core.manager.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.manager.variant.VariantStorageManager(catalogManager, storageManagerFactory);

        QueryOptions options = new QueryOptions()
                .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, cliOptions.fileName)
//                .append(AnalysisFileIndexer.CREATE, cliOptions.create)
//                .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                .append(VariantStorageEngine.Options.UPDATE_STATS.key(), cliOptions.updateStats)
                .append(VariantStorageEngine.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated)
                .append(VariantStorageEngine.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.aggregationMappingFile)
                .append(VariantStorageEngine.Options.RESUME.key(), cliOptions.resume)
                .append(StorageOperation.CATALOG_PATH, cliOptions.catalogPath);
        options.putIfNotEmpty(VariantStorageEngine.Options.FILE_ID.key(), cliOptions.fileId);

        options.putAll(cliOptions.commonOptions.params);

        List<String> cohorts;
        if (StringUtils.isNotBlank(cliOptions.cohortIds)) {
            cohorts = Arrays.asList(cliOptions.cohortIds.split(","));
        } else {
            cohorts = Collections.emptyList();
        }

        variantManager.stats(cliOptions.studyId, cohorts, cliOptions.outdir, options, sessionId);
    }

    private void annotate() throws StorageEngineException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        AnalysisCliOptionsParser.AnnotateVariantCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;
        org.opencb.opencga.storage.core.manager.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.manager.variant.VariantStorageManager(catalogManager, storageManagerFactory);

        Query query = new Query()
                .append(VariantDBAdaptor.VariantQueryParams.REGION.key(), cliOptions.filterRegion)
                .append(VariantDBAdaptor.VariantQueryParams.CHROMOSOME.key(), cliOptions.filterChromosome)
                .append(VariantDBAdaptor.VariantQueryParams.GENE.key(), cliOptions.filterGene)
                .append(VariantDBAdaptor.VariantQueryParams.ANNOT_CONSEQUENCE_TYPE.key(), cliOptions.filterAnnotConsequenceType);

        QueryOptions options = new QueryOptions();
        options.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);
        options.putIfNotEmpty(VariantAnnotationManager.SPECIES, cliOptions.species);
        options.putIfNotEmpty(VariantAnnotationManager.ASSEMBLY, cliOptions.assembly);
        options.put(VariantAnnotationManager.CREATE, cliOptions.create);
        options.putIfNotEmpty(VariantAnnotationManager.LOAD_FILE, cliOptions.load);
        options.putIfNotEmpty(VariantAnnotationManager.CUSTOM_ANNOTATION_KEY, cliOptions.customAnnotationKey);
        options.putIfNotNull(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        options.putIfNotEmpty(DefaultVariantAnnotationManager.FILE_NAME, cliOptions.fileName);
        options.put(StorageOperation.CATALOG_PATH, cliOptions.catalogPath);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.annotate(cliOptions.project, cliOptions.studyId, query, cliOptions.outdir, options, sessionId);

    }

}
