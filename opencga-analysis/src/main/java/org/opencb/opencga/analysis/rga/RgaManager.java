package org.opencb.opencga.analysis.rga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.analysis.rga.exceptions.RgaException;
import org.opencb.opencga.analysis.rga.iterators.RgaIterator;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.models.analysis.knockout.*;
import org.opencb.opencga.core.models.common.RgaIndex;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.api.ParamConstants.ACL_PARAM;

public class RgaManager implements AutoCloseable {

    private CatalogManager catalogManager;
    private StorageConfiguration storageConfiguration;
    private final RgaEngine rgaEngine;
    private final VariantStorageManager variantStorageManager;

    private IndividualRgaConverter individualRgaConverter;
    private GeneRgaConverter geneConverter;
    private VariantRgaConverter variantConverter;

    private final Logger logger;

    private static final int KNOCKOUT_INSERT_BATCH_SIZE = 25;
    private static ExecutorService executor;

    static {
        executor = Executors.newCachedThreadPool();
    }

    public RgaManager(CatalogManager catalogManager, VariantStorageManager variantStorageManager,
                      StorageEngineFactory storageEngineFactory) {
        this.catalogManager = catalogManager;
        this.storageConfiguration = storageEngineFactory.getStorageConfiguration();
        this.rgaEngine = new RgaEngine(storageConfiguration);
        this.variantStorageManager = variantStorageManager;

        this.individualRgaConverter = new IndividualRgaConverter();
        this.geneConverter = new GeneRgaConverter();
        this.variantConverter = new VariantRgaConverter();

        this.logger = LoggerFactory.getLogger(getClass());
    }

    public RgaManager(Configuration configuration, StorageConfiguration storageConfiguration) throws CatalogException {
        this.catalogManager = new CatalogManager(configuration);
        this.storageConfiguration = storageConfiguration;
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.get(storageConfiguration);
        this.variantStorageManager = new VariantStorageManager(catalogManager, storageEngineFactory);
        this.rgaEngine = new RgaEngine(storageConfiguration);

        this.individualRgaConverter = new IndividualRgaConverter();
        this.geneConverter = new GeneRgaConverter();
        this.variantConverter = new VariantRgaConverter();

        this.logger = LoggerFactory.getLogger(getClass());
    }

    public RgaManager(Configuration configuration, StorageConfiguration storageConfiguration, VariantStorageManager variantStorageManager,
                      RgaEngine rgaEngine) throws CatalogException {
        this.catalogManager = new CatalogManager(configuration);
        this.storageConfiguration = storageConfiguration;
        this.rgaEngine = rgaEngine;
        this.variantStorageManager = variantStorageManager;

        this.individualRgaConverter = new IndividualRgaConverter();
        this.geneConverter = new GeneRgaConverter();
        this.variantConverter = new VariantRgaConverter();

        this.logger = LoggerFactory.getLogger(getClass());
    }

    public OpenCGAResult<KnockoutByIndividual> individualQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String collection = getCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Preprocess preprocess;
        try {
            preprocess = individualQueryPreprocess(study, query, options, token);
        } catch (RgaException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getMessage())) {
                stopWatch.stop();
                return OpenCGAResult.empty(KnockoutByIndividual.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            throw e;
        } catch (CatalogException | IOException e) {
            throw e;
        }

        Future<VariantDBIterator> variantFuture = executor.submit(
                () -> variantStorageQuery(study.getFqn(), preprocess.getQuery().getAsStringList("sampleId"), preprocess.getQuery(),
                        options, token)
        );

        Future<RgaIterator> rgaIteratorFuture = executor.submit(() -> rgaEngine.individualQuery(collection, preprocess.getQuery(),
                QueryOptions.empty()));

        VariantDBIterator variantDBIterator;
        try {
            variantDBIterator = variantFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        RgaIterator rgaIterator;
        try {
            rgaIterator = rgaIteratorFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        List<KnockoutByIndividual> knockoutByIndividuals = individualRgaConverter.convertToDataModelType(rgaIterator, variantDBIterator);
        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        OpenCGAResult<KnockoutByIndividual> result = new OpenCGAResult<>(time, Collections.emptyList(), knockoutByIndividuals.size(),
                knockoutByIndividuals, -1);

        if (preprocess.getQueryOptions().getBoolean(QueryOptions.COUNT)) {
            result.setNumMatches(preprocess.getNumTotalResults());
        }
        if (preprocess.getEvent() != null) {
            result.setEvents(Collections.singletonList(preprocess.getEvent()));
        }

        return result;
    }

    public OpenCGAResult<RgaKnockoutByGene> geneQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());

        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        QueryOptions queryOptions = setDefaultLimit(options);
        List<String> includeIndividuals = queryOptions.getAsStringList(RgaQueryParams.INCLUDE_INDIVIDUAL);

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        Query auxQuery = query != null ? new Query(query) : new Query();

        // Get numTotalResults in a future
        Future<Integer> numTotalResults = null;
        if (queryOptions.getBoolean(QueryOptions.COUNT)) {
            numTotalResults = executor.submit(() -> {
                QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, "unique(" + RgaDataModel.GENE_ID + ")");
                try {
                    DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
                    return ((Number) result.first().getAggregationValues().get(0)).intValue();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return -1;
            });
        }

        // If the user is querying by gene id, we don't need to do a facet first
        if (!auxQuery.containsKey(RgaDataModel.GENE_ID)) {
            // 1st. we perform a facet to get the different gene ids matching the user query and using the skip and limit values
            QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, RgaDataModel.GENE_ID);
            facetOptions.putIfNotNull(QueryOptions.LIMIT, queryOptions.get(QueryOptions.LIMIT));
            facetOptions.putIfNotNull(QueryOptions.SKIP, queryOptions.get(QueryOptions.SKIP));

            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
            if (result.getNumResults() == 0) {
                stopWatch.stop();
                return OpenCGAResult.empty(RgaKnockoutByGene.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            List<String> geneIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
            auxQuery.put(RgaDataModel.GENE_ID, geneIds);
        }

        // Fetch all individuals matching the user query
        if (!auxQuery.containsKey("sampleId") && !auxQuery.containsKey("individualId") && includeIndividuals.isEmpty()) {
            // We perform a facet to get the different individual ids matching the user query
            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                    new QueryOptions(QueryOptions.FACET, RgaDataModel.INDIVIDUAL_ID).append(QueryOptions.LIMIT, -1));
            if (result.getNumResults() == 0) {
                stopWatch.stop();
                return OpenCGAResult.empty(RgaKnockoutByGene.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            includeIndividuals = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
        }

        // Get the set of sample ids the user will be able to see
        Set<String> includeSampleIds;
        if (!isOwnerOrAdmin) {
            if (!includeIndividuals.isEmpty()) {
                // 3. Get list of indexed sample ids for which the user has permissions from the list of includeIndividuals provided
                Query sampleQuery = new Query()
                        .append(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                                + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.INTERNAL_RGA_STATUS.key(), RgaIndex.Status.INDEXED)
                        .append(SampleDBAdaptor.QueryParams.ID.key(), includeIndividuals);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
                includeSampleIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            } else {
                // 2. Check permissions
                DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                        new QueryOptions(QueryOptions.FACET, RgaDataModel.SAMPLE_ID).append(QueryOptions.LIMIT, -1));
                if (result.getNumResults() == 0) {
                    stopWatch.stop();
                    return OpenCGAResult.empty(RgaKnockoutByGene.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
                }
                List<String> sampleIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());

                // 3. Get list of sample ids for which the user has permissions
                Query sampleQuery = new Query(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
                // TODO: The number of samples to include could be really high
                includeSampleIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            }
        } else {
            // TODO: Check if samples or individuals are provided
            // Obtain samples
            Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.INTERNAL_RGA_STATUS.key(), RgaIndex.Status.INDEXED);

            if (!includeIndividuals.isEmpty()) {
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), includeIndividuals);
            } else {
                // TODO: Include only the samples that will be necessary
                logger.warn("Include only the samples that are actually necessary");
            }

            OpenCGAResult<?> sampleResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                    SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
            includeSampleIds = new HashSet<>((List<String>) sampleResult.getResults());
        }

        Future<VariantDBIterator> variantFuture = executor.submit(
                () -> variantStorageQuery(study.getFqn(), new ArrayList<>(includeSampleIds), auxQuery, options, token)
        );

        Future<RgaIterator> tmpResultFuture = executor.submit(
                () -> rgaEngine.geneQuery(collection, auxQuery, queryOptions)
        );

        VariantDBIterator variantDBIterator;
        try {
            variantDBIterator = variantFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        RgaIterator rgaIterator;
        try {
            rgaIterator = tmpResultFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        int skipIndividuals = queryOptions.getInt(RgaQueryParams.SKIP_INDIVIDUAL);
        int limitIndividuals = queryOptions.getInt(RgaQueryParams.LIMIT_INDIVIDUAL, RgaQueryParams.DEFAULT_INDIVIDUAL_LIMIT);

        // 4. Solr gene query
        List<RgaKnockoutByGene> knockoutResultList = geneConverter.convertToDataModelType(rgaIterator, variantDBIterator,
                includeIndividuals, skipIndividuals, limitIndividuals);
        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        OpenCGAResult<RgaKnockoutByGene> knockoutResult = new OpenCGAResult<>(time, Collections.emptyList(), knockoutResultList.size(),
                knockoutResultList, -1);

        knockoutResult.setTime((int) stopWatch.getTime(TimeUnit.MILLISECONDS));
        try {
            knockoutResult.setNumMatches(numTotalResults != null ? numTotalResults.get() : -1);
        } catch (InterruptedException | ExecutionException e) {
            knockoutResult.setNumMatches(-1);
        }
        if (isOwnerOrAdmin && includeSampleIds.isEmpty()) {
            return knockoutResult;
        } else {
            // 5. Filter out individual or samples for which user does not have permissions
            for (RgaKnockoutByGene knockout : knockoutResult.getResults()) {
                List<RgaKnockoutByGene.KnockoutIndividual> individualList = new ArrayList<>(knockout.getIndividuals().size());
                for (RgaKnockoutByGene.KnockoutIndividual individual : knockout.getIndividuals()) {
                    if (includeSampleIds.contains(individual.getSampleId())) {
                        individualList.add(individual);
                    }
                }
                knockout.setIndividuals(individualList);
            }

            return knockoutResult;
        }
    }

    public OpenCGAResult<KnockoutByVariant> variantQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        QueryOptions queryOptions = setDefaultLimit(options);

        List<String> includeIndividuals = queryOptions.getAsStringList(RgaQueryParams.INCLUDE_INDIVIDUAL);

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        Query auxQuery = query != null ? new Query(query) : new Query();

        Future<Integer> numTotalResults = null;
        if (queryOptions.getBoolean(QueryOptions.COUNT)) {
            numTotalResults = executor.submit(() -> {
                QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, "unique(" + RgaDataModel.VARIANTS + ")");
                try {
                    DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
                    return ((Number) result.first().getAggregationValues().get(0)).intValue();
                } catch (Exception e) {
                    logger.error("Could not obtain the count: {}", e.getMessage(), e);
                }
                return -1;
            });
        }

        // If the user is querying by variant id, we don't need to do a facet first
        if (!auxQuery.containsKey(RgaDataModel.VARIANTS)) {
            // 1st. we perform a facet to get the different variant ids matching the user query and using the skip and limit values
            QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, RgaDataModel.VARIANTS);
            facetOptions.putIfNotNull(QueryOptions.LIMIT, queryOptions.get(QueryOptions.LIMIT));
            facetOptions.putIfNotNull(QueryOptions.SKIP, queryOptions.get(QueryOptions.SKIP));

            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
            if (result.getNumResults() == 0) {
                stopWatch.stop();
                return OpenCGAResult.empty(KnockoutByVariant.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            List<String> variantIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
            auxQuery.put(RgaDataModel.VARIANTS, variantIds);
        }

        Set<String> includeSampleIds;
        if (!isOwnerOrAdmin) {
            if (!includeIndividuals.isEmpty()) {
                // 3. Get list of sample ids for which the user has permissions from the list of includeIndividuals provided
                Query sampleQuery = new Query()
                        .append(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                                + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.INTERNAL_RGA_STATUS.key(), RgaIndex.Status.INDEXED)
                        .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), includeIndividuals);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
                includeSampleIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            } else {
                // 2. Check permissions
                DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                        new QueryOptions(QueryOptions.FACET, RgaDataModel.SAMPLE_ID).append(QueryOptions.LIMIT, -1));
                if (result.getNumResults() == 0) {
                    stopWatch.stop();
                    return OpenCGAResult.empty(KnockoutByVariant.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
                }
                List<String> sampleIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue)
                        .collect(Collectors.toList());

                // 3. Get list of individual ids for which the user has permissions
                Query sampleQuery = new Query()
                        .append(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                                + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
                includeSampleIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            }
        } else {
            // Obtain samples
            Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.INTERNAL_RGA_STATUS.key(), RgaIndex.Status.INDEXED);

            if (!includeIndividuals.isEmpty()) {
                query.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), includeIndividuals);
            } else {
                // TODO: Include only the samples that will be necessary
                logger.warn("Include only the samples that are actually necessary");
            }

            OpenCGAResult<?> sampleResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                    SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
            includeSampleIds = new HashSet<>((List<String>) sampleResult.getResults());
        }

        Future<VariantDBIterator> variantFuture = executor.submit(
                () -> variantStorageQuery(study.getFqn(), new ArrayList<>(includeSampleIds), auxQuery, options, token)
        );

        Future<RgaIterator> rgaIteratorFuture = executor.submit(() -> rgaEngine.variantQuery(collection, auxQuery, queryOptions));

        VariantDBIterator variantDBIterator;
        try {
            variantDBIterator = variantFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        RgaIterator rgaIterator;
        try {
            rgaIterator = rgaIteratorFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        int skipIndividuals = queryOptions.getInt(RgaQueryParams.SKIP_INDIVIDUAL);
        int limitIndividuals = queryOptions.getInt(RgaQueryParams.LIMIT_INDIVIDUAL, RgaQueryParams.DEFAULT_INDIVIDUAL_LIMIT);

        // 4. Solr gene query
        List<KnockoutByVariant> knockoutResultList = variantConverter.convertToDataModelType(rgaIterator, variantDBIterator,
                query.getAsStringList(RgaQueryParams.VARIANTS.key()), includeIndividuals, skipIndividuals, limitIndividuals);

        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        OpenCGAResult<KnockoutByVariant> knockoutResult = new OpenCGAResult<>(time, Collections.emptyList(), knockoutResultList.size(),
                knockoutResultList, -1);

        knockoutResult.setTime((int) stopWatch.getTime(TimeUnit.MILLISECONDS));
        try {
            knockoutResult.setNumMatches(numTotalResults != null ? numTotalResults.get() : -1);
        } catch (InterruptedException | ExecutionException e) {
            knockoutResult.setNumMatches(-1);
        }
        if (isOwnerOrAdmin && includeSampleIds.isEmpty()) {
            return knockoutResult;
        } else {
            // 5. Filter out individual or samples for which user does not have permissions
            for (KnockoutByVariant knockout : knockoutResult.getResults()) {
                List<KnockoutByIndividual> individualList = new ArrayList<>(knockout.getIndividuals().size());
                for (KnockoutByIndividual individual : knockout.getIndividuals()) {
                    if (includeSampleIds.contains(individual.getSampleId())) {
                        individualList.add(individual);
                    }
                }
                knockout.setIndividuals(individualList);
            }

            return knockoutResult;
        }
    }

    private VariantDBIterator variantStorageQuery(String study, List<String> sampleIds, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, StorageEngineException, RgaException {
        String collection = getCollectionName(study);

        DataResult<FacetField> result = rgaEngine.facetedQuery(collection, query,
                new QueryOptions(QueryOptions.FACET, RgaDataModel.VARIANTS).append(QueryOptions.LIMIT, -1));
        if (result.getNumResults() == 0) {
            return VariantDBIterator.EMPTY_ITERATOR;
        }
        List<String> variantIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());

        if (variantIds.size() > 500) {
            // TODO: Batches
            variantIds = variantIds.subList(0, 500);
        }

        Query variantQuery = new Query(VariantQueryParam.ID.key(), variantIds)
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds)
                .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT,DP");

        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.EXCLUDE, Arrays.asList(
//                        VariantField.ANNOTATION_POPULATION_FREQUENCIES,
                        VariantField.ANNOTATION_CYTOBAND,
                        VariantField.ANNOTATION_CONSERVATION,
                        VariantField.ANNOTATION_DRUGS,
                        VariantField.ANNOTATION_GENE_EXPRESSION
                ));

        return variantStorageManager.iterator(variantQuery, queryOptions, token);
    }

    public OpenCGAResult<Long> updateRgaInternalIndexStatus(String studyStr, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());

        catalogManager.getAuthorizationManager().checkIsOwnerOrAdmin(study.getUid(), userId);

        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        QueryOptions facetOptions = new QueryOptions()
                .append(QueryOptions.FACET, RgaDataModel.SAMPLE_ID)
                .append(QueryOptions.LIMIT, -1);
        DataResult<FacetField> result = rgaEngine.facetedQuery(collection, new Query(), facetOptions);

        int totalSamples = result.first().getBuckets().size();
        int updatedSamples = 0;

        // Before doing anything, we first reset all the sample rga indexes
        OpenCGAResult<Sample> resetResult = catalogManager.getSampleManager().resetRgaIndexes(studyStr, token);
        logger.debug("Resetting RGA indexes for " + resetResult.getNumMatches() + " samples took " + resetResult.getTime() + " ms.");

        // Update samples in batches of 100
        List<String> sampleIds = new ArrayList<>(100);

        RgaIndex rgaIndex = new RgaIndex(RgaIndex.Status.INDEXED, TimeUtils.getTime());
        for (FacetField.Bucket bucket : result.first().getBuckets()) {
            sampleIds.add(bucket.getValue());
            if (sampleIds.size() == 100) {
                OpenCGAResult<Sample> update = catalogManager.getSampleManager().updateRgaIndexes(study.getFqn(), sampleIds, rgaIndex,
                        token);
                updatedSamples += update.getNumUpdated();

                sampleIds = new ArrayList<>(100);
            }
        }

        if (!sampleIds.isEmpty()) {
            // Update last batch
            OpenCGAResult<Sample> update = catalogManager.getSampleManager().updateRgaIndexes(study.getFqn(), sampleIds, rgaIndex, token);
            updatedSamples += update.getNumUpdated();
        }

        stopWatch.stop();
        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), null, totalSamples, 0, updatedSamples, 0);
    }


    public OpenCGAResult<Long> updateRgaInternalIndexStatus(String studyStr, List<String> sampleIds, RgaIndex.Status status,
                                                            String token) throws CatalogException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());

        catalogManager.getAuthorizationManager().checkIsOwnerOrAdmin(study.getUid(), userId);

        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        int updatedSamples = 0;

        // Update samples in batches of 100
        List<String> tmpSampleIds = new ArrayList<>(100);

        RgaIndex rgaIndex = new RgaIndex(status, TimeUtils.getTime());
        for (String sampleId : sampleIds) {
            tmpSampleIds.add(sampleId);
            if (tmpSampleIds.size() == 100) {
                OpenCGAResult<Sample> update = catalogManager.getSampleManager().updateRgaIndexes(study.getFqn(), tmpSampleIds, rgaIndex,
                        token);
                updatedSamples += update.getNumUpdated();

                tmpSampleIds = new ArrayList<>(100);
            }
        }

        if (!tmpSampleIds.isEmpty()) {
            // Update last batch
            OpenCGAResult<Sample> update = catalogManager.getSampleManager().updateRgaIndexes(study.getFqn(), tmpSampleIds, rgaIndex,
                    token);
            updatedSamples += update.getNumUpdated();
        }

        stopWatch.stop();
        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), null, sampleIds.size(), 0, updatedSamples, 0);
    }

    public OpenCGAResult<KnockoutByIndividualSummary> individualSummary(String studyStr, Query query, QueryOptions options, String token)
            throws RgaException, CatalogException, IOException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String collection = getCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        Preprocess preprocess;
        try {
            preprocess = individualQueryPreprocess(study, query, options, token);
        } catch (RgaException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getMessage())) {
                stopWatch.stop();
                return OpenCGAResult.empty(KnockoutByIndividualSummary.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            throw e;
        } catch (CatalogException | IOException e) {
            throw e;
        }

        List<String> sampleIds = preprocess.getQuery().getAsStringList(RgaQueryParams.SAMPLE_ID.key());
        preprocess.getQuery().remove(RgaQueryParams.SAMPLE_ID.key());
        List<KnockoutByIndividualSummary> knockoutByIndividualSummaryList = new ArrayList<>(sampleIds.size());

        // Generate 4 batches to create 4 threads
        int batchSize = 4;
        List<List<String>> batchList = new ArrayList<>(batchSize);
        int size = (int) Math.ceil(sampleIds.size() / (double) batchSize);

        List<String> batch = null;
        for (int i = 0; i < sampleIds.size(); i++) {
            if (i % size == 0) {
                if (batch != null) {
                    batchList.add(batch);
                }
                batch = new ArrayList<>(size);
            }
            batch.add(sampleIds.get(i));
        }
        batchList.add(batch);

        List<Future<List<KnockoutByIndividualSummary>>> futureList = new ArrayList<>(batchSize);

        for (List<String> sampleIdList : batchList) {
            futureList.add(executor.submit(() -> calculateIndividualSummary(collection, preprocess.getQuery(), sampleIdList)));
        }

        try {
            for (Future<List<KnockoutByIndividualSummary>> summaryFuture : futureList) {
                knockoutByIndividualSummaryList.addAll(summaryFuture.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        OpenCGAResult<KnockoutByIndividualSummary> result = new OpenCGAResult<>(time, Collections.emptyList(),
                knockoutByIndividualSummaryList.size(), knockoutByIndividualSummaryList, -1);

        if (preprocess.getQueryOptions().getBoolean(QueryOptions.COUNT)) {
            result.setNumMatches(preprocess.getNumTotalResults());
        }
        if (preprocess.getEvent() != null) {
            result.setEvents(Collections.singletonList(preprocess.getEvent()));
        }

        return result;
    }

    public OpenCGAResult<KnockoutByGeneSummary> geneSummary(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());

        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        catalogManager.getAuthorizationManager().checkCanViewStudy(study.getUid(), userId);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        QueryOptions queryOptions = setDefaultLimit(options);

        Query auxQuery = query != null ? new Query(query) : new Query();

        // Get numTotalResults in a future
        Future<Integer> numTotalResults = null;
        if (queryOptions.getBoolean(QueryOptions.COUNT)) {
            numTotalResults = executor.submit(() -> {
                QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, "unique(" + RgaDataModel.GENE_ID + ")");
                try {
                    DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
                    return ((Number) result.first().getAggregationValues().get(0)).intValue();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return -1;
            });
        }

        List<String> geneIds = auxQuery.getAsStringList(RgaDataModel.GENE_ID);
        auxQuery.remove(RgaDataModel.GENE_ID);
        // If the user is querying by gene id, we don't need to do a facet first
        if (geneIds.isEmpty()) {
            // 1st. we perform a facet to get the different gene ids matching the user query and using the skip and limit values
            QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, RgaDataModel.GENE_ID);
            facetOptions.putIfNotNull(QueryOptions.LIMIT, queryOptions.get(QueryOptions.LIMIT));
            facetOptions.putIfNotNull(QueryOptions.SKIP, queryOptions.get(QueryOptions.SKIP));

            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
            if (result.getNumResults() == 0) {
                stopWatch.stop();
                return OpenCGAResult.empty(KnockoutByGeneSummary.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            geneIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
        }

        // Generate 4 batches to create 4 threads
        int batchSize = 4;
        List<List<String>> batchList = new ArrayList<>(batchSize);
        int size = (int) Math.ceil(geneIds.size() / (double) batchSize);

        List<String> batch = null;
        for (int i = 0; i < geneIds.size(); i++) {
            if (i % size == 0) {
                if (batch != null) {
                    batchList.add(batch);
                }
                batch = new ArrayList<>(size);
            }
            batch.add(geneIds.get(i));
        }
        batchList.add(batch);

        List<Future<List<KnockoutByGeneSummary>>> geneSummaryFutureList = new ArrayList<>(batchSize);

        for (List<String> geneIdList : batchList) {
            geneSummaryFutureList.add(executor.submit(() -> calculateGeneSummary(collection, auxQuery, geneIdList)));
        }

        List<KnockoutByGeneSummary> knockoutByGeneSummaryList = new ArrayList<>(geneIds.size());
        try {
            for (Future<List<KnockoutByGeneSummary>> summaryFuture : geneSummaryFutureList) {
                knockoutByGeneSummaryList.addAll(summaryFuture.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        OpenCGAResult<KnockoutByGeneSummary> result = new OpenCGAResult<>(time, Collections.emptyList(),
                knockoutByGeneSummaryList.size(), knockoutByGeneSummaryList, -1);

        if (queryOptions.getBoolean(QueryOptions.COUNT)) {
            try {
                assert numTotalResults != null;
                result.setNumMatches(numTotalResults.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RgaException(e.getMessage(), e);
            }
        }

        return result;
    }

    private List<KnockoutByGeneSummary> calculateGeneSummary(String collection, Query query, List<String> geneIdList)
            throws RgaException, IOException {
        List<KnockoutByGeneSummary> knockoutByGeneSummaryList = new ArrayList<>(geneIdList.size());
        for (String geneId : geneIdList) {
            Query auxQuery = new Query(query);
            auxQuery.put(RgaQueryParams.GENE_ID.key(), geneId);

            // 1. Get KnockoutByGene information
            Query individualQuery = new Query(RgaQueryParams.GENE_ID.key(), geneId);
            QueryOptions options = new QueryOptions()
                    .append(QueryOptions.LIMIT, 1)
                    .append(QueryOptions.EXCLUDE, "individuals");
            RgaIterator rgaIterator = rgaEngine.geneQuery(collection, individualQuery, options);

            if (!rgaIterator.hasNext()) {
                throw new RgaException("Unexpected error. Gene '" + geneId + "' not found");
            }
            RgaDataModel rgaDataModel = rgaIterator.next();
            KnockoutByGeneSummary geneSummary = new KnockoutByGeneSummary(rgaDataModel.getGeneId(), rgaDataModel.getGeneName(),
                    rgaDataModel.getChromosome(), rgaDataModel.getStart(), rgaDataModel.getEnd(), rgaDataModel.getStrand(),
                    rgaDataModel.getGeneBiotype(), null, null);

            // 2. Get KnockoutType counts
            QueryOptions knockoutTypeFacet = new QueryOptions()
                    .append(QueryOptions.LIMIT, -1)
                    .append(QueryOptions.FACET, RgaDataModel.FULL_VARIANT_INFO);
            DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, knockoutTypeFacet);
            KnockoutTypeCount knockoutTypeCount = new KnockoutTypeCount(auxQuery);
            for (FacetField.Bucket variantBucket : facetFieldDataResult.first().getBuckets()) {
                knockoutTypeCount.processVariant(variantBucket.getValue());
            }
            VariantStats variantStats = new VariantStats(knockoutTypeCount.getNumVariants(), knockoutTypeCount.getNumHomVariants(),
                    knockoutTypeCount.getNumCompHetVariants(), knockoutTypeCount.getNumHetVariants(),
                    knockoutTypeCount.getNumDelOverlapVariants());
            geneSummary.setVariantStats(variantStats);

            // 3. Get individual knockout type counts
            QueryOptions geneFacet = new QueryOptions()
                    .append(QueryOptions.LIMIT, -1)
                    .append(QueryOptions.FACET, RgaDataModel.NUM_PARENTS + ">>" + RgaDataModel.INDIVIDUAL_ID + ">>"
                            + RgaDataModel.KNOCKOUT_TYPES);
            facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, geneFacet);
            VariantStats noParents = new VariantStats();
            VariantStats singleParent = new VariantStats();
            VariantStats bothParents = new VariantStats();

            for (FacetField.Bucket numParentsBucket : facetFieldDataResult.first().getBuckets()) {
                VariantStats auxVariantStats;
                switch (numParentsBucket.getValue()) {
                    case "0":
                        auxVariantStats = noParents;
                        break;
                    case "1":
                        auxVariantStats = singleParent;
                        break;
                    case "2":
                        auxVariantStats = bothParents;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + numParentsBucket.getValue());
                }
                for (FacetField.Bucket individualBucket : numParentsBucket.getFacetFields().get(0).getBuckets()) {
                    for (FacetField.Bucket knockoutTypeBucket : individualBucket.getFacetFields().get(0).getBuckets()) {
                        KnockoutVariant.KnockoutType knockoutType = KnockoutVariant.KnockoutType.valueOf(knockoutTypeBucket.getValue());
                        switch (knockoutType) {
                            case HOM_ALT:
                                auxVariantStats.setNumHomAlt(auxVariantStats.getNumHomAlt() + 1);
                                break;
                            case COMP_HET:
                                auxVariantStats.setNumCompHet(auxVariantStats.getNumCompHet() + 1);
                                break;
                            case HET_ALT:
                                auxVariantStats.setNumHetAlt(auxVariantStats.getNumHetAlt() + 1);
                                break;
                            case DELETION_OVERLAP:
                                auxVariantStats.setNumDelOverlap(auxVariantStats.getNumDelOverlap() + 1);
                                break;
                            default:
                                throw new IllegalStateException("Unexpected value: " + knockoutType);
                        }
                        auxVariantStats.setCount(auxVariantStats.getNumCompHet() + auxVariantStats.getNumHomAlt()
                                + auxVariantStats.getNumHetAlt() + auxVariantStats.getNumDelOverlap());
                    }
                }
            }

            geneSummary.setIndividualStats(new IndividualStats(noParents, singleParent, bothParents));

            knockoutByGeneSummaryList.add(geneSummary);
        }
        return knockoutByGeneSummaryList;
    }

    private List<KnockoutByIndividualSummary> calculateIndividualSummary(String collection, Query query, List<String> sampleIdList)
            throws RgaException, IOException {
        List<KnockoutByIndividualSummary> knockoutByIndividualSummaryList = new ArrayList<>(sampleIdList.size());
        for (String sampleId : sampleIdList) {
            Query auxQuery = new Query(query);
            auxQuery.put(RgaQueryParams.SAMPLE_ID.key(), sampleId);

            // 1. Get KnockoutByIndividual information
            Query individualQuery = new Query(RgaQueryParams.SAMPLE_ID.key(), sampleId);
            QueryOptions options = new QueryOptions()
                    .append(QueryOptions.LIMIT, 1)
                    .append(QueryOptions.EXCLUDE, "genes");
            RgaIterator rgaIterator = rgaEngine.individualQuery(collection, individualQuery, options);

            if (!rgaIterator.hasNext()) {
                throw new RgaException("Unexpected error. Sample '" + sampleId + "' not found");
            }
            RgaDataModel rgaDataModel = rgaIterator.next();

            KnockoutByIndividual knockoutByIndividual = AbstractRgaConverter.fillIndividualInfo(rgaDataModel);
            KnockoutByIndividualSummary knockoutByIndividualSummary = new KnockoutByIndividualSummary(knockoutByIndividual);

            // 2. Get KnockoutType counts
            QueryOptions knockoutTypeFacet = new QueryOptions()
                    .append(QueryOptions.LIMIT, -1)
                    .append(QueryOptions.FACET, RgaDataModel.FULL_VARIANT_INFO);
            DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, knockoutTypeFacet);
            KnockoutTypeCount knockoutTypeCount = new KnockoutTypeCount(auxQuery);
            for (FacetField.Bucket variantBucket : facetFieldDataResult.first().getBuckets()) {
                knockoutTypeCount.processVariant(variantBucket.getValue());
            }
            VariantStats variantStats = new VariantStats(knockoutTypeCount.getNumVariants(), knockoutTypeCount.getNumHomVariants(),
                    knockoutTypeCount.getNumCompHetVariants(), knockoutTypeCount.getNumHetVariants(),
                    knockoutTypeCount.getNumDelOverlapVariants());
            knockoutByIndividualSummary.setVariantStats(variantStats);

            // 3. Get gene id list
            QueryOptions geneFacet = new QueryOptions()
                    .append(QueryOptions.LIMIT, -1)
                    .append(QueryOptions.FACET, RgaDataModel.GENE_ID + ">>" + RgaDataModel.GENE_NAME);
            facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, geneFacet);
            List<KnockoutByIndividualSummary.Gene> geneIds = facetFieldDataResult.first().getBuckets()
                    .stream()
                    .map((bucket) -> new KnockoutByIndividualSummary.Gene(bucket.getValue(),
                            bucket.getFacetFields().get(0).getBuckets().get(0).getValue()))
                    .collect(Collectors.toList());
            knockoutByIndividualSummary.setGenes(geneIds);

            knockoutByIndividualSummaryList.add(knockoutByIndividualSummary);
        }

        return knockoutByIndividualSummaryList;
    }

    private static class KnockoutTypeCount {

        private Set<String> knockoutTypeQuery;
        private List<Set<String>> popFreqQuery;
        private Set<String> typeQuery;
        private Set<String> consequenceTypeQuery;

        private Set<String> variantIds;
        private Set<String> compHetVariantIds;
        private Set<String> homVariantIds;
        private Set<String> hetVariantIds;
        private Set<String> delOverlapVariantIds;

        public KnockoutTypeCount(Query query) throws RgaException {
            knockoutTypeQuery = new HashSet<>();
            popFreqQuery = new LinkedList<>();
            typeQuery = new HashSet<>();
            consequenceTypeQuery = new HashSet<>();
            variantIds = new HashSet<>();
            compHetVariantIds = new HashSet<>();
            homVariantIds = new HashSet<>();
            hetVariantIds = new HashSet<>();
            delOverlapVariantIds = new HashSet<>();

            query = ParamUtils.defaultObject(query, Query::new);
            knockoutTypeQuery.addAll(query.getAsStringList(RgaQueryParams.KNOCKOUT.key()));
            typeQuery.addAll(query.getAsStringList(RgaQueryParams.TYPE.key()));
            consequenceTypeQuery.addAll(query.getAsStringList(RgaQueryParams.CONSEQUENCE_TYPE.key())
                    .stream()
                    .map(VariantQueryUtils::parseConsequenceType)
                    .map(String::valueOf)
                    .collect(Collectors.toList()));
            List<String> popFreqs = query.getAsStringList(RgaQueryParams.POPULATION_FREQUENCY.key(), ";");
            if (!popFreqs.isEmpty()) {
                Map<String, List<String>> popFreqList = RgaUtils.parsePopulationFrequencyQuery(popFreqs);
                for (List<String> values : popFreqList.values()) {
                    popFreqQuery.add(new HashSet<>(values));
                }
            }
        }

        public void processVariant(String variant) throws RgaException {
            RgaUtils.CodedVariant codedVariant = new RgaUtils.CodedVariant(variant);

            if (!knockoutTypeQuery.isEmpty() && !knockoutTypeQuery.contains(codedVariant.getKnockoutType())) {
                return;
            }
            if (!popFreqQuery.isEmpty()) {
                for (Set<String> popFreq : popFreqQuery) {
                    if (codedVariant.getPopulationFrequencies().stream().noneMatch(popFreq::contains)) {
                        return;
                    }
                }
            }
            if (!typeQuery.isEmpty() && !typeQuery.contains(codedVariant.getType())) {
                return;
            }
            if (!consequenceTypeQuery.isEmpty()
                    && codedVariant.getConsequenceType().stream().noneMatch((ct) -> consequenceTypeQuery.contains(ct))) {
                return;
            }

            variantIds.add(codedVariant.getVariantId());
            KnockoutVariant.KnockoutType knockoutType = KnockoutVariant.KnockoutType.valueOf(codedVariant.getKnockoutType());
            switch (knockoutType) {
                case HOM_ALT:
                    homVariantIds.add(codedVariant.getVariantId());
                    break;
                case COMP_HET:
                    compHetVariantIds.add(codedVariant.getVariantId());
                    break;
                case HET_ALT:
                    hetVariantIds.add(codedVariant.getVariantId());
                    break;
                case DELETION_OVERLAP:
                    delOverlapVariantIds.add(codedVariant.getVariantId());
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + codedVariant.getKnockoutType());
            }
        }

        public int getNumVariants() {
            return variantIds.size();
        }

        public int getNumCompHetVariants() {
            return compHetVariantIds.size();
        }

        public int getNumHomVariants() {
            return homVariantIds.size();
        }

        public int getNumHetVariants() {
            return hetVariantIds.size();
        }

        public int getNumDelOverlapVariants() {
            return delOverlapVariantIds.size();
        }
    }

    private Preprocess individualQueryPreprocess(Study study, Query query, QueryOptions options, String token)
            throws RgaException, CatalogException, IOException {

        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);

        Preprocess preprocessResult = new Preprocess();
        preprocessResult.setQuery(query != null ? new Query(query) : new Query());
        preprocessResult.setQueryOptions(setDefaultLimit(options));
        QueryOptions queryOptions = preprocessResult.getQueryOptions();

        int limit = queryOptions.getInt(QueryOptions.LIMIT, AbstractManager.DEFAULT_LIMIT);
        int skip = queryOptions.getInt(QueryOptions.SKIP);

        List<String> sampleIds;
        boolean count = queryOptions.getBoolean(QueryOptions.COUNT);

        if (preprocessResult.getQuery().isEmpty()) {
            QueryOptions catalogOptions = new QueryOptions(SampleManager.INCLUDE_SAMPLE_IDS)
                    .append(QueryOptions.LIMIT, limit)
                    .append(QueryOptions.SKIP, skip)
                    .append(QueryOptions.COUNT, queryOptions.getBoolean(QueryOptions.COUNT));
            Query catalogQuery = new Query(SampleDBAdaptor.QueryParams.INTERNAL_RGA_STATUS.key(), RgaIndex.Status.INDEXED);
            if (!isOwnerOrAdmin) {
                catalogQuery.put(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS);
            }

            OpenCGAResult<Sample> search = catalogManager.getSampleManager().search(study.getFqn(), catalogQuery, catalogOptions, token);
            if (search.getNumResults() == 0) {
                throw RgaException.noResultsMatching();
            }

            sampleIds = search.getResults().stream().map(Sample::getId).collect(Collectors.toList());
            preprocessResult.setNumTotalResults(search.getNumMatches());
        } else {
            if (!preprocessResult.getQuery().containsKey("sampleId")
                    && !preprocessResult.getQuery().containsKey("individualId")) {
                // 1st. we perform a facet to get the different sample ids matching the user query
                DataResult<FacetField> result = rgaEngine.facetedQuery(collection, preprocessResult.getQuery(),
                        new QueryOptions(QueryOptions.FACET, RgaDataModel.SAMPLE_ID).append(QueryOptions.LIMIT, -1));
                if (result.getNumResults() == 0) {
                    throw RgaException.noResultsMatching();
                }
                List<String> samples = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
                preprocessResult.getQuery().put("sampleId", samples);
            }

            // From the list of sample ids the user wants to retrieve data from, we filter those for which the user has permissions
            Query sampleQuery = new Query();
            if (!isOwnerOrAdmin) {
                sampleQuery.put(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS);
            }

            List<String> authorisedSamples = new LinkedList<>();
            if (!isOwnerOrAdmin || !preprocessResult.getQuery().containsKey("sampleId")) {
                int maxSkip = 10000;
                if (skip > maxSkip) {
                    throw new RgaException("Cannot paginate further than " + maxSkip + " individuals. Please, narrow down your query.");
                }

                int batchSize = 1000;
                int currentBatch = 0;
                boolean queryNextBatch = true;

                String sampleQueryField;
                List<String> values;
                if (preprocessResult.getQuery().containsKey("individualId")) {
                    sampleQueryField = SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key();
                    values = preprocessResult.getQuery().getAsStringList("individualId");
                } else {
                    sampleQueryField = SampleDBAdaptor.QueryParams.ID.key();
                    values = preprocessResult.getQuery().getAsStringList("sampleId");
                }

                if (count && values.size() > maxSkip) {
                    preprocessResult.setEvent(new Event(Event.Type.WARNING, "numMatches value is approximated considering the "
                            + "individuals that are accessible for the user from the first batch of 10000 individuals matching the solr "
                            + "query."));
                }

                while (queryNextBatch) {
                    List<String> tmpValues = values.subList(currentBatch, Math.min(values.size(), batchSize + currentBatch));

                    sampleQuery.put(sampleQueryField, tmpValues);
                    OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                            SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
                    authorisedSamples.addAll((Collection<? extends String>) authorisedSampleIdResult.getResults());

                    if (values.size() < batchSize + currentBatch) {
                        queryNextBatch = false;
                        preprocessResult.setNumTotalResults(authorisedSamples.size());
                    } else if (count && currentBatch < maxSkip) {
                        currentBatch += batchSize;
                    } else if (authorisedSamples.size() > skip + limit) {
                        queryNextBatch = false;
                        // We will get an approximate number of total results
                        preprocessResult.setNumTotalResults(Math.round(((float) authorisedSamples.size() * values.size())
                                / (currentBatch + batchSize)));
                    } else {
                        currentBatch += batchSize;
                    }
                }
            } else {
                authorisedSamples = preprocessResult.getQuery().getAsStringList("sampleId");
                preprocessResult.setNumTotalResults(authorisedSamples.size());
            }

            if (skip == 0 && limit > authorisedSamples.size()) {
                sampleIds = authorisedSamples;
            } else if (skip > authorisedSamples.size()) {
                throw RgaException.noResultsMatching();
            } else {
                int to = Math.min(authorisedSamples.size(), skip + limit);
                sampleIds = authorisedSamples.subList(skip, to);
            }
        }
        preprocessResult.getQuery().put("sampleId", sampleIds);

        return preprocessResult;
    }

    private class Preprocess {

        private Query query;
        private QueryOptions queryOptions;
        private long numTotalResults;
        private Event event;

        public Preprocess() {
        }

        public Query getQuery() {
            return query;
        }

        public Preprocess setQuery(Query query) {
            this.query = query;
            return this;
        }

        public QueryOptions getQueryOptions() {
            return queryOptions;
        }

        public Preprocess setQueryOptions(QueryOptions queryOptions) {
            this.queryOptions = queryOptions;
            return this;
        }

        public long getNumTotalResults() {
            return numTotalResults;
        }

        public Preprocess setNumTotalResults(long numTotalResults) {
            this.numTotalResults = numTotalResults;
            return this;
        }

        public Event getEvent() {
            return event;
        }

        public Preprocess setEvent(Event event) {
            this.event = event;
            return this;
        }
    }

    public OpenCGAResult<FacetField> aggregationStats(String studyStr, Query query, QueryOptions options, String fields, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);

        catalogManager.getAuthorizationManager().checkCanViewStudy(study.getUid(), userId);

        String collection = getCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }
        ParamUtils.checkObj(fields, "Missing mandatory field 'field");

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(QueryOptions.FACET, fields);
        return new OpenCGAResult<>(rgaEngine.facetedQuery(collection, query, queryOptions));
    }

    private QueryOptions setDefaultLimit(QueryOptions options) {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        if (!queryOptions.containsKey(QueryOptions.LIMIT)) {
            queryOptions.put(QueryOptions.LIMIT, AbstractManager.DEFAULT_LIMIT);
        }
        return queryOptions;
    }

    public void index(String studyStr, String fileStr, String token) throws CatalogException, RgaException, IOException {
        File file = catalogManager.getFileManager().get(studyStr, fileStr, FileManager.INCLUDE_FILE_URI_PATH, token).first();
        Path filePath = Paths.get(file.getUri());
        index(studyStr, filePath, token);
    }

    public void index(String studyStr, Path file, String token) throws CatalogException, IOException, RgaException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        try {
            catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error(e.getMessage(), e);
            throw new CatalogException("Only owners or admins can index", e.getCause());
        }

        load(study.getFqn(), file, token);
    }

    /**
     * Load a multi KnockoutByIndividual JSON file into the Solr core/collection.
     *
     * @param path Path to the JSON file
     * @param path Path to the JSON file
     * @param path Path to the JSON file
     * @param path Path to the JSON file
     * @throws IOException
     * @throws SolrException
     */
    private void load(String study, Path path, String token) throws IOException, RgaException {
        String fileName = path.getFileName().toString();
        if (fileName.endsWith("json") || fileName.endsWith("json.gz")) {
            String collection = getCollectionName(study);

            try {
                if (!rgaEngine.exists(collection)) {
                    rgaEngine.create(collection);
                }
            } catch (RgaException e) {
                logger.error("Could not perform RGA index in collection {}", collection, e);
                throw new RgaException("Could not perform RGA index in collection '" + collection + "'.");
            }

            try {
                IOConnectorProvider ioConnectorProvider = new IOConnectorProvider(storageConfiguration);

                // This opens json and json.gz files automatically
                try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
                        ioConnectorProvider.newInputStream(path.toUri())))) {

                    List<KnockoutByIndividual> knockoutByIndividualList = new ArrayList<>(KNOCKOUT_INSERT_BATCH_SIZE);
                    int count = 0;
                    String line;
                    ObjectReader objectReader = new ObjectMapper().readerFor(KnockoutByIndividual.class);
                    while ((line = bufferedReader.readLine()) != null) {
                        KnockoutByIndividual knockoutByIndividual = objectReader.readValue(line);
                        knockoutByIndividualList.add(knockoutByIndividual);
                        count++;
                        if (count % KNOCKOUT_INSERT_BATCH_SIZE == 0) {
                            List<RgaDataModel> rgaDataModelList = individualRgaConverter.convertToStorageType(knockoutByIndividualList);
                            rgaEngine.insert(collection, rgaDataModelList);
                            logger.debug("Loaded {} knockoutByIndividual entries from '{}'", count, path);

                            // Update RGA Index status
                            try {
                                updateRgaInternalIndexStatus(study, knockoutByIndividualList.stream()
                                                .map(KnockoutByIndividual::getSampleId).collect(Collectors.toList()),
                                        RgaIndex.Status.INDEXED, token);
                                logger.debug("Updated sample RGA index statuses");
                            } catch (CatalogException e) {
                                logger.warn("Sample RGA index status could not be updated: {}", e.getMessage(), e);
                            }

                            knockoutByIndividualList.clear();
                        }
                    }

                    // Insert the remaining entries
                    if (CollectionUtils.isNotEmpty(knockoutByIndividualList)) {
                        List<RgaDataModel> rgaDataModelList = individualRgaConverter.convertToStorageType(knockoutByIndividualList);
                        rgaEngine.insert(collection, rgaDataModelList);
                        logger.debug("Loaded remaining {} knockoutByIndividual entries from '{}'", count, path);

                        // Update RGA Index status
                        try {
                            updateRgaInternalIndexStatus(study, knockoutByIndividualList.stream()
                                            .map(KnockoutByIndividual::getSampleId).collect(Collectors.toList()),
                                    RgaIndex.Status.INDEXED, token);
                            logger.debug("Updated sample RGA index statuses");
                        } catch (CatalogException e) {
                            logger.warn("Sample RGA index status could not be updated: {}", e.getMessage(), e);
                        }

                    }
                }
            } catch (SolrServerException e) {
                throw new RgaException("Error loading KnockoutIndividual from JSON file.", e);
            }
        } else {
            throw new RgaException("File format " + path + " not supported. Please, use JSON file format.");
        }
    }

    public void testConnection() throws StorageEngineException {
        rgaEngine.isAlive("test");
    }

    private String getCollectionName(String study) {
        return catalogManager.getConfiguration().getDatabasePrefix() + "-rga-" + study.replace("@", "_").replace(":", "_");
    }

    @Override
    public void close() throws Exception {
        rgaEngine.close();
    }

}
