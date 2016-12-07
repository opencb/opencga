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
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DataStore;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.local.variant.operations.VariantFileIndexerStorageOperation;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.DefaultVariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.stats.DefaultVariantStatisticsManager;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.opencb.opencga.storage.core.local.variant.operations.VariantFileIndexerStorageOperation.LOAD;
import static org.opencb.opencga.storage.core.local.variant.operations.VariantFileIndexerStorageOperation.TRANSFORM;

/**
 * Created by imedina on 02/03/15.
 */
public class VariantCommandExecutor extends AnalysisStorageCommandExecutor {

    private AnalysisCliOptionsParser.VariantCommandOptions variantCommandOptions;
    private VariantStorageManager variantStorageManager;

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


    private VariantStorageManager initVariantStorageManager(DataStore dataStore)
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

        org.opencb.opencga.storage.core.local.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.local.variant.VariantStorageManager(catalogManager, storageConfiguration);

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
            variantManager.exportData(cliOptions.output, null, cliOptions.commonOptions.outputFormat, query, queryOptions, sessionId);
        }
    }

    private void delete() {
        throw new UnsupportedOperationException();
    }

    private void index() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException, StorageManagerException,
            InstantiationException, IllegalAccessException, URISyntaxException {
        AnalysisCliOptionsParser.IndexVariantCommandOptions cliOptions = variantCommandOptions.indexVariantCommandOptions;

        QueryOptions queryOptions = new QueryOptions();
        queryOptions.put(LOAD, variantCommandOptions.indexVariantCommandOptions.load);
        queryOptions.put(TRANSFORM, variantCommandOptions.indexVariantCommandOptions.transform);

        queryOptions.put(VariantStorageManager.Options.CALCULATE_STATS.key(), cliOptions.calculateStats);
        queryOptions.put(VariantStorageManager.Options.EXTRA_GENOTYPE_FIELDS.key(), cliOptions.extraFields);
        queryOptions.put(VariantStorageManager.Options.EXCLUDE_GENOTYPES.key(), cliOptions.excludeGenotype);
        queryOptions.put(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated);
        queryOptions.put(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.aggregationMappingFile);
        queryOptions.put(VariantStorageManager.Options.GVCF.key(), cliOptions.gvcf);

        queryOptions.putIfNotNull(VariantFileIndexerStorageOperation.CATALOG_PATH, cliOptions.catalogPath);
        queryOptions.putIfNotNull(VariantFileIndexerStorageOperation.TRANSFORMED_FILES, cliOptions.transformedPaths);

        queryOptions.put(VariantStorageManager.Options.ANNOTATE.key(), cliOptions.annotate);
        if (cliOptions.annotator != null) {
            queryOptions.put(VariantAnnotationManager.ANNOTATION_SOURCE, cliOptions.annotator);
        }
        queryOptions.put(VariantAnnotationManager.OVERWRITE_ANNOTATIONS, cliOptions.overwriteAnnotations);
        queryOptions.putAll(cliOptions.commonOptions.params);

        org.opencb.opencga.storage.core.local.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.local.variant.VariantStorageManager(catalogManager, storageConfiguration);
        variantManager.index(cliOptions.fileId, cliOptions.outdir, cliOptions.catalogPath, queryOptions, sessionId);

    }

    private void stats() throws CatalogException, AnalysisExecutionException, IOException, ClassNotFoundException,
            StorageManagerException, InstantiationException, IllegalAccessException, URISyntaxException {
        AnalysisCliOptionsParser.StatsVariantCommandOptions cliOptions = variantCommandOptions.statsVariantCommandOptions;

        org.opencb.opencga.storage.core.local.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.local.variant.VariantStorageManager(catalogManager, storageConfiguration);

        QueryOptions options = new QueryOptions()
                .append(DefaultVariantStatisticsManager.OUTPUT_FILE_NAME, cliOptions.fileName)
//                .append(AnalysisFileIndexer.CREATE, cliOptions.create)
//                .append(AnalysisFileIndexer.LOAD, cliOptions.load)
                .append(VariantStorageManager.Options.UPDATE_STATS.key(), cliOptions.updateStats)
                .append(VariantStorageManager.Options.AGGREGATED_TYPE.key(), cliOptions.aggregated)
                .append(VariantStorageManager.Options.AGGREGATION_MAPPING_PROPERTIES.key(), cliOptions.aggregationMappingFile);
        options.putIfNotEmpty(VariantStorageManager.Options.FILE_ID.key(), cliOptions.fileId);

        options.putAll(cliOptions.commonOptions.params);

        List<String> cohorts;
        if (StringUtils.isNotBlank(cliOptions.cohortIds)) {
            cohorts = Arrays.asList(cliOptions.cohortIds.split(","));
        } else {
            cohorts = Collections.emptyList();
        }

        variantManager.stats(cliOptions.studyId, cohorts, cliOptions.outdir, cliOptions.catalogPath, options, sessionId);
    }

    private void annotate() throws StorageManagerException, IOException, URISyntaxException, VariantAnnotatorException, CatalogException,
            AnalysisExecutionException, IllegalAccessException, InstantiationException, ClassNotFoundException {

        AnalysisCliOptionsParser.AnnotateVariantCommandOptions cliOptions = variantCommandOptions.annotateVariantCommandOptions;
        org.opencb.opencga.storage.core.local.variant.VariantStorageManager variantManager =
                new org.opencb.opencga.storage.core.local.variant.VariantStorageManager(catalogManager, storageConfiguration);


        long studyId = catalogManager.getStudyId(cliOptions.studyId, sessionId);
        String catalogOutDirId;
        if (isEmpty(cliOptions.catalogPath)) {
            catalogOutDirId = String.valueOf(catalogManager.getAllFiles(studyId, new Query(FileDBAdaptor.QueryParams.PATH.key(), ""), null, sessionId)
                    .first().getId());
        } else {
            catalogOutDirId = cliOptions.catalogPath;
        }

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
        options.putIfNotEmpty(DefaultVariantAnnotationManager.FILE_NAME, cliOptions.fileName);
        options.putAll(cliOptions.commonOptions.params);

        variantManager.annotate(cliOptions.studyId, query, cliOptions.outdir, catalogOutDirId, options, sessionId);

    }

}
