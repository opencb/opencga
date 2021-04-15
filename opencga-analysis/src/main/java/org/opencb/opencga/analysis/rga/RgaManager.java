package org.opencb.opencga.analysis.rga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.variant.Variant;
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
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.analysis.knockout.RgaKnockoutByGene;
import org.opencb.opencga.core.models.common.RgaIndex;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
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
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        Query auxQuery = query != null ? new Query(query) : new Query();

        QueryOptions queryOptions = setDefaultLimit(options);

        int limit = queryOptions.getInt(QueryOptions.LIMIT, AbstractManager.DEFAULT_LIMIT);
        int skip = queryOptions.getInt(QueryOptions.SKIP);

        long numTotalResults = 0;
        List<String> sampleIds;
        Event event = null;
        boolean count = queryOptions.getBoolean(QueryOptions.COUNT);

        if (auxQuery.isEmpty()) {
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
                stopWatch.stop();
                return OpenCGAResult.empty(KnockoutByIndividual.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }

            sampleIds = search.getResults().stream().map(Sample::getId).collect(Collectors.toList());
            numTotalResults = search.getNumMatches();
        } else {
            if (!auxQuery.containsKey("sampleId") && !auxQuery.containsKey("individualId")) {
                // 1st. we perform a facet to get the different sample ids matching the user query
                DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                        new QueryOptions(QueryOptions.FACET, RgaDataModel.SAMPLE_ID).append(QueryOptions.LIMIT, -1));
                if (result.getNumResults() == 0) {
                    stopWatch.stop();
                    return OpenCGAResult.empty(KnockoutByIndividual.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
                }
                List<String> samples = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
                auxQuery.put("sampleId", samples);
            }

            // From the list of sample ids the user wants to retrieve data from, we filter those for which the user has permissions
            Query sampleQuery = new Query();
            if (!isOwnerOrAdmin) {
                sampleQuery.put(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS);
            }

            List<String> authorisedSamples = new LinkedList<>();
            if (!isOwnerOrAdmin || !auxQuery.containsKey("sampleId")) {
                int maxSkip = 10000;
                if (skip > maxSkip) {
                    throw new RgaException("Cannot paginate further than " + maxSkip + " individuals. Please, narrow down your query.");
                }

                int batchSize = 1000;
                int currentBatch = 0;
                boolean queryNextBatch = true;

                String sampleQueryField;
                List<String> values;
                if (auxQuery.containsKey("individualId")) {
                    sampleQueryField = SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key();
                    values = auxQuery.getAsStringList("individualId");
                } else {
                    sampleQueryField = SampleDBAdaptor.QueryParams.ID.key();
                    values = auxQuery.getAsStringList("sampleId");
                }

                if (count && values.size() > maxSkip) {
                    event = new Event(Event.Type.WARNING, "numMatches value is approximated considering the individuals that are "
                            + "accessible for the user from the first batch of 10000 individuals matching the solr query.");
                }

                while (queryNextBatch) {
                    List<String> tmpValues = values.subList(currentBatch, Math.min(values.size(), batchSize + currentBatch));

                    sampleQuery.put(sampleQueryField, tmpValues);
                    OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                            SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
                    authorisedSamples.addAll((Collection<? extends String>) authorisedSampleIdResult.getResults());

                    if (values.size() < batchSize + currentBatch) {
                        queryNextBatch = false;
                        numTotalResults = authorisedSamples.size();
                    } else if (count && currentBatch < maxSkip) {
                        currentBatch += batchSize;
                    } else if (authorisedSamples.size() > skip + limit) {
                        queryNextBatch = false;
                        // We will get an approximate number of total results
                        numTotalResults = Math.round(((float) authorisedSamples.size() * values.size()) / (currentBatch + batchSize));
                    } else {
                        currentBatch += batchSize;
                    }
                }
            } else {
                authorisedSamples = auxQuery.getAsStringList("sampleId");
                numTotalResults = authorisedSamples.size();
            }

            if (skip == 0 && limit > authorisedSamples.size()) {
                sampleIds = authorisedSamples;
            } else if (skip > authorisedSamples.size()) {
                stopWatch.stop();
                return OpenCGAResult.empty(KnockoutByIndividual.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            } else {
                int to = Math.min(authorisedSamples.size(), skip + limit);
                sampleIds = authorisedSamples.subList(skip, to);
            }
        }
        auxQuery.put("sampleId", sampleIds);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<VariantDBIterator> variantFuture = executor.submit(
                () -> variantStorageQuery(study.getFqn(), sampleIds, auxQuery, options, token)
        );

        Future<OpenCGAResult<RgaDataModel>> tmpResultFuture = executor.submit(
                () -> rgaEngine.individualQuery(collection, auxQuery, queryOptions)
        );

        VariantDBIterator variantDBIterator;
        try {
            variantDBIterator = variantFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        OpenCGAResult<RgaDataModel> tmpResult;
        try {
            tmpResult = tmpResultFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        List<KnockoutByIndividual> knockoutByIndividuals = individualRgaConverter.convertToDataModelType(tmpResult.getResults(),
                variantDBIterator);
        OpenCGAResult<KnockoutByIndividual> result = new OpenCGAResult<>(tmpResult.getTime(), tmpResult.getEvents(),
                knockoutByIndividuals.size(), knockoutByIndividuals, -1);

        if (count) {
            result.setNumMatches(numTotalResults);
        }
        if (event != null) {
            result.setEvents(Collections.singletonList(event));
        }
        result.setTime((int) stopWatch.getTime(TimeUnit.MILLISECONDS));

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

        ExecutorService executor = Executors.newFixedThreadPool(3);

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
                () -> rgaEngine.geneQueryIterator(collection, auxQuery, queryOptions)
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
        List<RgaKnockoutByGene> knockoutResultList = geneConverter.convertToDataModelType(rgaIterator, variantDBIterator, skipIndividuals,
                limitIndividuals);
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
            ExecutorService executor = Executors.newSingleThreadExecutor();
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

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<VariantDBIterator> variantFuture = executor.submit(
                () -> variantStorageQuery(study.getFqn(), new ArrayList<>(includeSampleIds), auxQuery, options, token)
        );

        Future<OpenCGAResult<RgaDataModel>> tmpResultFuture = executor.submit(
                () -> rgaEngine.geneQuery(collection, auxQuery, queryOptions)
        );

        VariantDBIterator variantDBIterator;
        try {
            variantDBIterator = variantFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        OpenCGAResult<RgaDataModel> tmpResult;
        try {
            tmpResult = tmpResultFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        // 4. Solr gene query
        List<KnockoutByVariant> knockoutResultList = variantConverter.convertToDataModelType(tmpResult.getResults(), variantDBIterator,
                query.getAsStringList(RgaQueryParams.VARIANTS.key()));
        OpenCGAResult<KnockoutByVariant> knockoutResult = new OpenCGAResult<>(tmpResult.getTime(), tmpResult.getEvents(),
                knockoutResultList.size(), knockoutResultList, -1);

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
                    if (includeSampleIds.contains(individual.getId())) {
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

//        if (variantIds.size() > 1000) {
//            // TODO: Batches
//            variantIds = variantIds.subList(0, 100);
//        }

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

//    public List<KnockoutVariant> PetterQuery(List<String> variants, Set<String> transcripts, String study, ArrayList<String> samples,
//    String token, KnockoutVariant.KnockoutType knockoutType)
//            throws CatalogException, IOException, StorageEngineException {
//        Query query = new Query(VariantQueryParam.ID.key(), variants)
//                .append(VariantQueryParam.STUDY.key(), study)
//                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), samples)
//                .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT,DP");
//        List<Variant> result = get(query, new QueryOptions(), token).getResults();
//        List<KnockoutVariant> knockoutVariants = new LinkedList<>();
//        for (Variant variant : result) {
//            VariantAnnotation annotation = variant.getAnnotation();
////
////            for (PopulationFrequency populationFrequency : annotation.getPopulationFrequencies()) {
//////                populationFrequency.getStudy()
//////                populationFrequency.getPopulation()
////            }
////
////            // Clinvar and Cosmic
////            for (EvidenceEntry evidenceEntry : annotation.getTraitAssociation()) {
////                ClinicalSignificance clinicalSignificance = evidenceEntry.getVariantClassification().getClinicalSignificance();
////            }
//            List<ConsequenceType> myConsequenceTypes = new ArrayList<>(transcripts.size());
//            for (ConsequenceType consequenceType : annotation.getConsequenceTypes()) {
//                String geneName = consequenceType.getGeneName();
//                String transcriptId = consequenceType.getEnsemblTranscriptId();
//                if (transcripts.contains(transcriptId)) {
////                    for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
////
////                    }
//                    myConsequenceTypes.add(consequenceType);
//                }
//            }
//            StudyEntry studyEntry = variant.getStudies().get(0);
//            int samplePosition = 0;
//            for (SampleEntry sample : studyEntry.getSamples()) {
//                String sampleId = samples.get(samplePosition++);
//                for (ConsequenceType ct : myConsequenceTypes) {
//                    knockoutVariants.add(new KnockoutVariant(variant, studyEntry, studyEntry.getFile(sample.getFileIndex()), sample,
//                    annotation, ct, knockoutType));
//                }
//            }
//        }
//        return knockoutVariants;
//    }

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
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        try {
            catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error(e.getMessage(), e);
            throw new CatalogException("Only owners or admins can index", e.getCause());
        }

        File file = catalogManager.getFileManager().get(studyStr, fileStr, FileManager.INCLUDE_FILE_URI_PATH, token).first();

        load(study.getFqn(), Paths.get(file.getUri()), token);
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
