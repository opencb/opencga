package org.opencb.opencga.clinical.rga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.clinical.StorageManager;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByVariant;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.sample.SampleAclEntry;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.RgaException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.rga.GeneRgaConverter;
import org.opencb.opencga.storage.core.rga.RgaDataModel;
import org.opencb.opencga.storage.core.rga.RgaEngine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.api.ParamConstants.ACL_PARAM;

public class RgaManager extends StorageManager implements AutoCloseable {

    private final RgaEngine rgaEngine;

    private static final int KNOCKOUT_INSERT_BATCH_SIZE = 25;
    public static final String INCLUDE_INDIVIDUAL_OPTION = "includeIndividual";

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

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        Query auxQuery = query != null ? new Query(query) : new Query();

        if (!auxQuery.containsKey("sampleId") && !auxQuery.containsKey("individualId")) {
            // 1st. we perform a facet to get the different sample ids matching the user query
            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery,
                    new QueryOptions(QueryOptions.FACET, RgaDataModel.SAMPLE_ID).append(QueryOptions.LIMIT, -1));
            List<String> sampleIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());

            if (sampleIds.isEmpty()) {
                return OpenCGAResult.empty(KnockoutByIndividual.class);
            }
            auxQuery.put("sampleId", sampleIds);
        }

        // From the list of sample ids the user wants to retrieve data from, we filter those for which the user has permissions
        Query sampleQuery = new Query();
        if (!isOwnerOrAdmin) {
            sampleQuery.put(ACL_PARAM, userId + ":" + SampleAclEntry.SamplePermissions.VIEW + ","
                    + SampleAclEntry.SamplePermissions.VIEW_VARIANTS);
        }
        List<String> samples;
        if (!isOwnerOrAdmin || !auxQuery.containsKey("sampleId")) {
            if (auxQuery.containsKey("individualId")) {
                sampleQuery.put(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), auxQuery.get("individualId"));
            } else {
                sampleQuery.put(SampleDBAdaptor.QueryParams.ID.key(), auxQuery.get("sampleId"));
            }

            OpenCGAResult<?> authorisedSampleIdResult = catalogManager.getSampleManager().distinct(study.getFqn(),
                    SampleDBAdaptor.QueryParams.ID.key(), sampleQuery, token);
            if (authorisedSampleIdResult.getNumResults() == 0) {
                return OpenCGAResult.empty(KnockoutByIndividual.class);
            }
            samples = (List<String>) authorisedSampleIdResult.getResults();
        } else {
            samples = auxQuery.getAsStringList("sampleId");
        }

        int limit = options.getInt(QueryOptions.LIMIT, AbstractManager.DEFAULT_LIMIT);
        int skip = options.getInt(QueryOptions.SKIP);

        List<String> sampleIds;
        if (skip == 0 && limit > samples.size()) {
            sampleIds = samples;
        } else if (skip > samples.size()) {
            return OpenCGAResult.empty(KnockoutByIndividual.class);
        } else {
            int to = Math.min(samples.size(), skip + limit);
            sampleIds = samples.subList(skip, to);
        }
        auxQuery.put("sampleId", sampleIds);

        return rgaEngine.individualQuery(collection, auxQuery, options);
    }

    public OpenCGAResult<KnockoutByGene> geneQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getCollectionName(study.getFqn());

        List<String> includeIndividuals = options.getAsStringList(INCLUDE_INDIVIDUAL_OPTION);

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        Query auxQuery = query != null ? new Query(query) : new Query();

        // If the user is querying by gene id, we don't need to do a facet first
        if (!auxQuery.containsKey(RgaDataModel.GENE_ID)) {
            // 1st. we perform a facet to get the different gene ids matching the user query and using the skip and limit values
            QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, RgaDataModel.GENE_ID);
            facetOptions.putIfNotNull(QueryOptions.LIMIT, options.get(QueryOptions.LIMIT));
            facetOptions.putIfNotNull(QueryOptions.SKIP, options.get(QueryOptions.SKIP));

            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
            if (result.getNumResults() == 0) {
                return OpenCGAResult.empty(KnockoutByGene.class);
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
        OpenCGAResult<KnockoutByGene> knockoutResult = rgaEngine.geneQuery(collection, auxQuery, options);
        if (isOwnerOrAdmin && includeIndividualIds.isEmpty()) {
            return knockoutResult;
        } else {
            // 5. Filter out individual or samples for which user does not have permissions
            for (KnockoutByGene knockout : knockoutResult.getResults()) {
                List<KnockoutByGene.KnockoutIndividual> individualList = new ArrayList<>(knockout.getIndividuals().size());
                for (KnockoutByGene.KnockoutIndividual individual : knockout.getIndividuals()) {
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

        List<String> includeIndividuals = options.getAsStringList(INCLUDE_INDIVIDUAL_OPTION);

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        Query auxQuery = query != null ? new Query(query) : new Query();

        // If the user is querying by gene id, we don't need to do a facet first
        if (!auxQuery.containsKey(RgaDataModel.VARIANTS)) {
            // 1st. we perform a facet to get the different variant ids matching the user query and using the skip and limit values
            QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, RgaDataModel.VARIANTS);
            facetOptions.putIfNotNull(QueryOptions.LIMIT, options.get(QueryOptions.LIMIT));
            facetOptions.putIfNotNull(QueryOptions.SKIP, options.get(QueryOptions.SKIP));

            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, auxQuery, facetOptions);
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
                List<String> individualIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());

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
        OpenCGAResult<KnockoutByVariant> knockoutResult = rgaEngine.variantQuery(collection, auxQuery, options);
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
