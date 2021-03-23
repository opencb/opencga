package org.opencb.opencga.clinical.rga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.managers.SampleManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.clinical.StorageManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.analysis.knockout.RgaKnockoutByGene;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.rga.RgaDataModel;
import org.opencb.opencga.storage.core.rga.RgaEngine;
import org.opencb.opencga.storage.core.rga.RgaQueryParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.api.ParamConstants.ACL_PARAM;

public class RgaManager extends StorageManager implements AutoCloseable {

    private final RgaEngine rgaEngine;

    private static final int KNOCKOUT_INSERT_BATCH_SIZE = 25;

    public RgaManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);
        this.rgaEngine = new RgaEngine(getStorageConfiguration());
    }

    public RgaManager(Configuration configuration, StorageConfiguration storageConfiguration) throws CatalogException {
        super(configuration, storageConfiguration);
        this.rgaEngine = new RgaEngine(storageConfiguration);
    }

    public RgaManager(Configuration configuration, StorageConfiguration storageConfiguration, RgaEngine rgaEngine) throws CatalogException {
        super(configuration, storageConfiguration);
        this.rgaEngine = rgaEngine;
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
            // TODO: We need to query only for the samples indexed in Solr
            QueryOptions catalogOptions = new QueryOptions(SampleManager.INCLUDE_SAMPLE_IDS)
                    .append(QueryOptions.LIMIT, limit)
                    .append(QueryOptions.SKIP, skip)
                    .append(QueryOptions.COUNT, queryOptions.getBoolean(QueryOptions.COUNT));
            Query catalogQuery = new Query();
            if (!isOwnerOrAdmin) {
                catalogQuery.put(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS);
            }

            OpenCGAResult<Sample> search = catalogManager.getSampleManager().search(study.getFqn(), catalogQuery, catalogOptions, token);
            if (search.getNumResults() == 0) {
                return OpenCGAResult.empty(KnockoutByIndividual.class);
            }

            sampleIds = search.getResults().stream().map(Sample::getId).collect(Collectors.toList());
            numTotalResults = search.getNumMatches();
        } else {
            if (!auxQuery.containsKey("sampleId") && !auxQuery.containsKey("individualId")) {
                // 1st. we perform a facet to get the different sample ids matching the user query
                DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                        new QueryOptions(QueryOptions.FACET, RgaDataModel.SAMPLE_ID).append(QueryOptions.LIMIT, -1));
                List<String> samples = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());

                if (samples.isEmpty()) {
                    return OpenCGAResult.empty(KnockoutByIndividual.class);
                }
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
                return OpenCGAResult.empty(KnockoutByIndividual.class);
            } else {
                int to = Math.min(authorisedSamples.size(), skip + limit);
                sampleIds = authorisedSamples.subList(skip, to);
            }
        }
        auxQuery.put("sampleId", sampleIds);

        OpenCGAResult<KnockoutByIndividual> result = rgaEngine.individualQuery(collection, auxQuery, queryOptions);
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

        Future<Integer> numTotalResults = null;
        if (queryOptions.getBoolean(QueryOptions.COUNT)) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
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
                return OpenCGAResult.empty(RgaKnockoutByGene.class);
            }
            List<String> geneIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
            auxQuery.put(RgaDataModel.GENE_ID, geneIds);
        }

        Set<String> includeIndividualIds;
        if (!isOwnerOrAdmin) {
            if (!includeIndividuals.isEmpty()) {
                // 3. Get list of individual ids for which the user has permissions from the list of includeInviduals provided
                Query sampleQuery = new Query(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), includeIndividuals);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), sampleQuery, token);
                includeIndividualIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            } else {
                // 2. Check permissions
                DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                        new QueryOptions(QueryOptions.FACET, RgaDataModel.SAMPLE_ID).append(QueryOptions.LIMIT, -1));
                if (result.getNumResults() == 0) {
                    return OpenCGAResult.empty(RgaKnockoutByGene.class);
                }
                List<String> sampleIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());

                // 3. Get list of individual ids for which the user has permissions
                Query sampleQuery = new Query(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), sampleQuery, token);
                includeIndividualIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            }
        } else {
            includeIndividualIds = new HashSet<>(includeIndividuals);
        }

        // 4. Solr gene query
        OpenCGAResult<RgaKnockoutByGene> knockoutResult = rgaEngine.geneQuery(collection, auxQuery, queryOptions);
        knockoutResult.setTime((int) stopWatch.getTime(TimeUnit.MILLISECONDS));
        try {
            knockoutResult.setNumMatches(numTotalResults != null ? numTotalResults.get() : -1);
        } catch (InterruptedException | ExecutionException e) {
            knockoutResult.setNumMatches(-1);
        }
        if (isOwnerOrAdmin && includeIndividualIds.isEmpty()) {
            return knockoutResult;
        } else {
            // 5. Filter out individual or samples for which user does not have permissions
            for (RgaKnockoutByGene knockout : knockoutResult.getResults()) {
                List<RgaKnockoutByGene.KnockoutIndividual> individualList = new ArrayList<>(knockout.getIndividuals().size());
                for (RgaKnockoutByGene.KnockoutIndividual individual : knockout.getIndividuals()) {
                    if (includeIndividualIds.contains(individual.getId())) {
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
                return OpenCGAResult.empty(KnockoutByVariant.class);
            }
            List<String> variantIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
            auxQuery.put(RgaDataModel.VARIANTS, variantIds);
        }

        Set<String> includeIndividualIds;
        if (!isOwnerOrAdmin) {
            if (!includeIndividuals.isEmpty()) {
                // 3. Get list of individual ids for which the user has permissions from the list of includeInviduals provided
                Query sampleQuery = new Query(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), includeIndividuals);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), sampleQuery, token);
                includeIndividualIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            } else {
                // 2. Check permissions
                DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                        new QueryOptions(QueryOptions.FACET, RgaDataModel.INDIVIDUAL_ID).append(QueryOptions.LIMIT, -1));
                if (result.getNumResults() == 0) {
                    return OpenCGAResult.empty(KnockoutByVariant.class);
                }
                List<String> individualIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue)
                        .collect(Collectors.toList());

                // 3. Get list of individual ids for which the user has permissions
                Query sampleQuery = new Query(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                        + SampleAclEntry.SamplePermissions.VIEW_VARIANTS)
                        .append(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualIds);
                OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                        SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), sampleQuery, token);
                includeIndividualIds = new HashSet<>((List<String>) authorisedSampleIdResult.getResults());
            }
        } else {
            includeIndividualIds = new HashSet<>(includeIndividuals);
        }

        // 4. Solr gene query
        OpenCGAResult<KnockoutByVariant> knockoutResult = rgaEngine.variantQuery(collection, auxQuery, queryOptions);
        knockoutResult.setTime((int) stopWatch.getTime(TimeUnit.MILLISECONDS));
        try {
            knockoutResult.setNumMatches(numTotalResults != null ? numTotalResults.get() : -1);
        } catch (InterruptedException | ExecutionException e) {
            knockoutResult.setNumMatches(-1);
        }
        if (isOwnerOrAdmin && includeIndividualIds.isEmpty()) {
            return knockoutResult;
        } else {
            // 5. Filter out individual or samples for which user does not have permissions
            for (KnockoutByVariant knockout : knockoutResult.getResults()) {
                List<KnockoutByIndividual> individualList = new ArrayList<>(knockout.getIndividuals().size());
                for (KnockoutByIndividual individual : knockout.getIndividuals()) {
                    if (includeIndividualIds.contains(individual.getId())) {
                        individualList.add(individual);
                    }
                }
                knockout.setIndividuals(individualList);
            }

            return knockoutResult;
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
                            rgaEngine.insert(collection, knockoutByIndividualList);
                            logger.debug("Loaded {} knockoutByIndividual entries from '{}'", count, path);
                            knockoutByIndividualList.clear();
                        }
                    }

                    // Insert the remaining entries
                    if (CollectionUtils.isNotEmpty(knockoutByIndividualList)) {
                        rgaEngine.insert(collection, knockoutByIndividualList);
                        logger.debug("Loaded remaining {} knockoutByIndividual entries from '{}'", count, path);
                    }
                }
            } catch (SolrServerException e) {
                throw new RgaException("Error loading KnockoutIndividual from JSON file.", e);
            }
        } else {
            throw new RgaException("File format " + path + " not supported. Please, use JSON file format.");
        }
    }

    @Override
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
