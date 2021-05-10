package org.opencb.opencga.analysis.rga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinicalSignificance;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.analysis.rga.RgaUtils.CodedIndividual;
import org.opencb.opencga.analysis.rga.RgaUtils.CodedVariant;
import org.opencb.opencga.analysis.rga.RgaUtils.KnockoutTypeCount;
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
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.response.OpenCGAResult;
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

import static org.opencb.opencga.analysis.rga.RgaUtils.convertToKnockoutVariant;
import static org.opencb.opencga.analysis.rga.RgaUtils.decode;
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

    // Data load

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
            String collection = getMainCollectionName(study);

            try {
                if (!rgaEngine.exists(collection)) {
                    rgaEngine.createMainCollection(collection);
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

    public void generateAuxiliarCollection(String studyStr, String token) throws CatalogException, RgaException, IOException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        try {
            catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);
        } catch (CatalogException e) {
            logger.error(e.getMessage(), e);
            throw new CatalogException("Only owners or admins can generate the auxiliar RGA collection", e.getCause());
        }

        String auxCollection = getAuxCollectionName(study.getFqn());
        if (rgaEngine.isAlive(auxCollection)) {
            throw new RgaException("Auxiliar RGA collection already exists");
        }

        String mainCollection = getMainCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(mainCollection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        StopWatch stopWatch = StopWatch.createStarted();
        try {
            if (!rgaEngine.exists(auxCollection)) {
                rgaEngine.createAuxCollection(auxCollection);
            }
        } catch (RgaException e) {
            logger.error("Could create auxiliar RGA collection '{}'", auxCollection, e);
            throw new RgaException("Could not create auxiliar RGA collection '" + auxCollection + "'.");
        }

        // Get list of variants that will be inserted
        QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, RgaDataModel.VARIANTS);
        facetOptions.put(QueryOptions.LIMIT, -1);
        DataResult<FacetField> result = rgaEngine.facetedQuery(mainCollection, new Query(), facetOptions);
        logger.info("Took {} ms to get the complete list of variants", stopWatch.getTime(TimeUnit.MILLISECONDS));


        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<AuxiliarRgaDataModel>> futureList = new ArrayList<>(result.first().getBuckets().size());
        for (FacetField.Bucket bucket : result.first().getBuckets()) {
            futureList.add(executor.submit(() -> getAuxiliarRgaDataModel(mainCollection, bucket.getValue())));
        }

        int count = 0;
        List<AuxiliarRgaDataModel> auxiliarRgaDataModelList = new ArrayList<>(KNOCKOUT_INSERT_BATCH_SIZE * 4);
        for (Future<AuxiliarRgaDataModel> future : futureList) {
            try {
                auxiliarRgaDataModelList.add(future.get());
                if (auxiliarRgaDataModelList.size() == KNOCKOUT_INSERT_BATCH_SIZE * 4) {
                    // Insert in solr
                    rgaEngine.insert(auxCollection, auxiliarRgaDataModelList);

                    count += auxiliarRgaDataModelList.size();
                    logger.info("Loading batch {} in RGA auxiliar collection", count);

                    // Empty array
                    auxiliarRgaDataModelList = new ArrayList<>(KNOCKOUT_INSERT_BATCH_SIZE * 4);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RgaException("Error reading from future: " + e.getMessage(), e);
            } catch (SolrServerException e) {
                throw new RgaException("Error inserting in Solr: " + e.getMessage(), e);
            }
        }

        if (!auxiliarRgaDataModelList.isEmpty()) {
            // Insert remaining documents in solr
            try {
                rgaEngine.insert(auxCollection, auxiliarRgaDataModelList);
                count += auxiliarRgaDataModelList.size();
                logger.info("Loading final batch {} in RGA auxiliar collection", count);
            } catch (SolrServerException e) {
                throw new RgaException("Error inserting in Solr: " + e.getMessage(), e);
            }
        }

    }

    private AuxiliarRgaDataModel getAuxiliarRgaDataModel(String mainCollection, String variantId) throws RgaException, IOException {
        Query query = new Query(RgaQueryParams.VARIANTS.key(), variantId);
        StopWatch stopWatch = StopWatch.createStarted();

        String dbSnp = "";
        String type = "";
        Set<String> knockoutTypes = new HashSet<>();
        Set<String> consequenceTypes = new HashSet<>();
        Map<String, String> populationFrequencyMap = new HashMap<>();
        Set<String> clinicalSignificances = new HashSet<>();
        Set<String> geneIds = new HashSet<>();
        Set<String> geneNames = new HashSet<>();
        Set<String> transcripts = new HashSet<>();

        // 3. Get allele pairs and CT from Variant summary
        QueryOptions knockoutTypeFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.VARIANT_SUMMARY);
        DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(mainCollection, query, knockoutTypeFacet);

        for (FacetField.Bucket bucket : facetFieldDataResult.first().getBuckets()) {
            CodedVariant codedVariant = CodedVariant.parseEncodedId(bucket.getValue());
            if (variantId.equals(codedVariant.getId())) {
                knockoutTypes.add(codedVariant.getKnockoutType());
                consequenceTypes.addAll(codedVariant.getConsequenceType());
                clinicalSignificances.addAll(codedVariant.getClinicalSignificances());
                clinicalSignificances.add(codedVariant.getTranscriptId());

                if (populationFrequencyMap.isEmpty()) {
                    dbSnp = codedVariant.getDbSnp();
                    type = codedVariant.getType();

                    String pfKey = RgaDataModel.POPULATION_FREQUENCIES.replace("*", "");
                    String thousandGenomeKey = pfKey + RgaUtils.THOUSAND_GENOMES_STUDY;
                    String gnomadGenomeKey = pfKey + RgaUtils.GNOMAD_GENOMES_STUDY;

                    populationFrequencyMap.put(thousandGenomeKey, codedVariant.getThousandGenomesFrequency());
                    populationFrequencyMap.put(gnomadGenomeKey, codedVariant.getGnomadFrequency());
                }
            }
        }

        knockoutTypeFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.GENE_ID);
        facetFieldDataResult = rgaEngine.facetedQuery(mainCollection, query, knockoutTypeFacet);
        geneIds.addAll(facetFieldDataResult.first().getBuckets()
                .stream()
                .map(FacetField.Bucket::getValue)
                .map(String::valueOf)
                .collect(Collectors.toSet()));

        knockoutTypeFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.GENE_NAME);
        facetFieldDataResult = rgaEngine.facetedQuery(mainCollection, query, knockoutTypeFacet);
        geneNames.addAll(facetFieldDataResult.first().getBuckets()
                .stream()
                .map(FacetField.Bucket::getValue)
                .map(String::valueOf)
                .collect(Collectors.toSet()));

        logger.info("Processing variant '{}' took {} milliseconds", variantId, stopWatch.getTime(TimeUnit.MILLISECONDS));

        return new AuxiliarRgaDataModel(variantId, dbSnp, type, new ArrayList<>(knockoutTypes),
                new ArrayList<>(consequenceTypes), populationFrequencyMap, new ArrayList<>(clinicalSignificances),
                new ArrayList<>(geneIds), new ArrayList<>(geneNames), new ArrayList<>(transcripts));
    }

    public OpenCGAResult<Long> updateRgaInternalIndexStatus(String studyStr, List<String> sampleIds, RgaIndex.Status status,
                                                            String token) throws CatalogException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getMainCollectionName(study.getFqn());

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

    public OpenCGAResult<Long> updateRgaInternalIndexStatus(String studyStr, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getMainCollectionName(study.getFqn());

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

    // Queries

    public OpenCGAResult<KnockoutByIndividual> individualQuery(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String collection = getMainCollectionName(study.getFqn());

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

        RgaIterator rgaIterator = rgaEngine.individualQuery(collection, preprocess.getQuery(), QueryOptions.empty());

        List<KnockoutByIndividual> knockoutByIndividuals = individualRgaConverter.convertToDataModelType(rgaIterator);

        if (!preprocess.isOwnerOrAdmin) {
            // Extract all parent sample ids
            Set<String> parentSampleIds = new HashSet<>();
            for (KnockoutByIndividual knockoutByIndividual : knockoutByIndividuals) {
                if (StringUtils.isNotEmpty(knockoutByIndividual.getFatherSampleId())) {
                    parentSampleIds.add(knockoutByIndividual.getFatherSampleId());
                }
                if (StringUtils.isNotEmpty(knockoutByIndividual.getMotherSampleId())) {
                    parentSampleIds.add(knockoutByIndividual.getMotherSampleId());
                }
            }
            // Check parent permissions...
            Set<String> authorisedSamples = getAuthorisedSamples(study.getFqn(), parentSampleIds, null, preprocess.getUserId(), token);
            if (authorisedSamples.size() < parentSampleIds.size()) {
                // Filter out parent sample ids
                for (KnockoutByIndividual knockoutByIndividual : knockoutByIndividuals) {
                    if (StringUtils.isNotEmpty(knockoutByIndividual.getFatherSampleId())
                            && !authorisedSamples.contains(knockoutByIndividual.getFatherSampleId())) {
                        knockoutByIndividual.setFatherId("");
                        knockoutByIndividual.setFatherSampleId("");
                    }
                    if (StringUtils.isNotEmpty(knockoutByIndividual.getMotherSampleId())
                            && !authorisedSamples.contains(knockoutByIndividual.getMotherSampleId())) {
                        knockoutByIndividual.setMotherId("");
                        knockoutByIndividual.setMotherSampleId("");
                    }
                }
            }
        }

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
        String collection = getMainCollectionName(study.getFqn());
        String auxCollection = getAuxCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }
        if (!rgaEngine.isAlive(auxCollection)) {
            throw new RgaException("Missing auxiliar RGA collection for study '" + study.getFqn() + "'");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        ExecutorService executor = Executors.newFixedThreadPool(4);

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

        List<String> geneIds;
        try {
            geneIds = getIds(collection, auxCollection, RgaQueryParams.GENE_ID.key(), auxQuery, queryOptions);
            auxQuery.put(RgaQueryParams.GENE_ID.key(), geneIds);
        } catch (RgaException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getMessage())) {
                return OpenCGAResult.empty(RgaKnockoutByGene.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            throw e;
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

        RgaIterator rgaIterator = rgaEngine.geneQuery(collection, auxQuery, queryOptions);

        int skipIndividuals = queryOptions.getInt(RgaQueryParams.SKIP_INDIVIDUAL);
        int limitIndividuals = queryOptions.getInt(RgaQueryParams.LIMIT_INDIVIDUAL, RgaQueryParams.DEFAULT_INDIVIDUAL_LIMIT);

        // 4. Solr gene query
        List<RgaKnockoutByGene> knockoutResultList = geneConverter.convertToDataModelType(rgaIterator, includeIndividuals, skipIndividuals,
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
        String collection = getMainCollectionName(study.getFqn());
        String auxCollection = getAuxCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }
        if (!rgaEngine.isAlive(auxCollection)) {
            throw new RgaException("Missing auxiliar RGA collection for study '" + study.getFqn() + "'");
        }

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        ExecutorService executor = Executors.newFixedThreadPool(4);

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

        List<String> variantIds;
        try {
            variantIds = getIds(collection, auxCollection, RgaQueryParams.VARIANTS.key(), auxQuery, queryOptions);
            auxQuery.put(RgaDataModel.VARIANTS, variantIds);
        } catch (RgaException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getMessage())) {
                return OpenCGAResult.empty(KnockoutByVariant.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            throw e;
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

                // TODO: Batches of samples to query catalog
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
                auxQuery.getAsStringList(RgaQueryParams.VARIANTS.key()), includeIndividuals, skipIndividuals, limitIndividuals);

        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        OpenCGAResult<KnockoutByVariant> knockoutResult = new OpenCGAResult<>(time, Collections.emptyList(), knockoutResultList.size(),
                knockoutResultList, -1);
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

    public OpenCGAResult<KnockoutByIndividualSummary> individualSummary(String studyStr, Query query, QueryOptions options, String token)
            throws RgaException, CatalogException, IOException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String collection = getMainCollectionName(study.getFqn());

        StopWatch stopWatch = StopWatch.createStarted();
        ExecutorService executor = Executors.newFixedThreadPool(4);

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

        catalogManager.getAuthorizationManager().checkStudyPermission(study.getUid(), preprocess.getUserId(),
                StudyAclEntry.StudyPermissions.VIEW_AGGREGATED_VARIANTS);

        // Check number of individuals matching query without checking their permissions
        Future<Integer> totalIndividualsFuture = executor.submit(() -> {
            QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, "unique(" + RgaDataModel.INDIVIDUAL_ID + ")");
            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, query, facetOptions);
            return ((Number) result.first().getAggregationValues().get(0)).intValue();
        });

        List<String> sampleIds = preprocess.getQuery().getAsStringList(RgaQueryParams.SAMPLE_ID.key());
        preprocess.getQuery().remove(RgaQueryParams.SAMPLE_ID.key());
        List<KnockoutByIndividualSummary> knockoutByIndividualSummaryList = new ArrayList<>(sampleIds.size());

        List<Future<KnockoutByIndividualSummary>> futureList = new ArrayList<>(sampleIds.size());

        for (String sampleId : sampleIds) {
            futureList.add(executor.submit(() -> calculateIndividualSummary(collection, preprocess.getQuery(), sampleId)));
        }

        Set<String> parentSampleIds = new HashSet<>();
        try {
            for (Future<KnockoutByIndividualSummary> summaryFuture : futureList) {
                KnockoutByIndividualSummary knockoutByIndividualSummary = summaryFuture.get();
                if (!preprocess.isOwnerOrAdmin()) {
                    if (StringUtils.isNotEmpty(knockoutByIndividualSummary.getFatherSampleId())) {
                        parentSampleIds.add(knockoutByIndividualSummary.getFatherSampleId());
                    }
                    if (StringUtils.isNotEmpty(knockoutByIndividualSummary.getMotherSampleId())) {
                        parentSampleIds.add(knockoutByIndividualSummary.getMotherSampleId());
                    }
                }

                knockoutByIndividualSummaryList.add(knockoutByIndividualSummary);
            }
        } catch (InterruptedException | ExecutionException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getCause().getMessage())) {
                return OpenCGAResult.empty(KnockoutByIndividualSummary.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            throw new RgaException(e.getMessage(), e);
        }

        if (!parentSampleIds.isEmpty()) {
            // Check parent permissions...
            Set<String> authorisedSamples = getAuthorisedSamples(study.getFqn(), parentSampleIds, null, preprocess.getUserId(), token);
            // Filter out parent sample ids
            if (authorisedSamples.size() < parentSampleIds.size()) {
                for (KnockoutByIndividualSummary knockoutByIndividualSummary : knockoutByIndividualSummaryList) {
                    if (StringUtils.isNotEmpty(knockoutByIndividualSummary.getFatherSampleId())
                            && !authorisedSamples.contains(knockoutByIndividualSummary.getFatherSampleId())) {
                        knockoutByIndividualSummary.setFatherId("");
                        knockoutByIndividualSummary.setFatherSampleId("");
                    }
                    if (StringUtils.isNotEmpty(knockoutByIndividualSummary.getMotherSampleId())
                            && !authorisedSamples.contains(knockoutByIndividualSummary.getMotherSampleId())) {
                        knockoutByIndividualSummary.setMotherId("");
                        knockoutByIndividualSummary.setMotherSampleId("");
                    }
                }
            }
        }

        ObjectMap resultAttributes = new ObjectMap();
        try {
            resultAttributes.put("totalIndividuals", totalIndividualsFuture.get());
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Unexpected error getting total number of individuals without checking permissions: {}", e.getMessage(), e);
        }
        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        OpenCGAResult<KnockoutByIndividualSummary> result = new OpenCGAResult<>(time, Collections.emptyList(),
                knockoutByIndividualSummaryList.size(), knockoutByIndividualSummaryList, -1);
        result.setAttributes(resultAttributes);

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
        String collection = getMainCollectionName(study.getFqn());
        String auxCollection = getAuxCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }
        if (!rgaEngine.isAlive(auxCollection)) {
            throw new RgaException("Missing auxiliar RGA collection for study '" + study.getFqn() + "'");
        }

        catalogManager.getAuthorizationManager().checkStudyPermission(study.getUid(), userId,
                StudyAclEntry.StudyPermissions.VIEW_AGGREGATED_VARIANTS);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ExecutorService executor = Executors.newFixedThreadPool(4);

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

        List<String> geneIds;
        try {
            geneIds = getIds(collection, auxCollection, RgaQueryParams.GENE_ID.key(), auxQuery, queryOptions);
            auxQuery.remove(RgaQueryParams.GENE_ID.key());
        } catch (RgaException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getMessage())) {
                return OpenCGAResult.empty(KnockoutByGeneSummary.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            throw e;
        }

        List<Future<KnockoutByGeneSummary>> geneSummaryFutureList = new ArrayList<>(geneIds.size());
        for (String geneId : geneIds) {
            geneSummaryFutureList.add(executor.submit(() -> calculateGeneSummary(collection, auxQuery, geneId)));
        }

        List<KnockoutByGeneSummary> knockoutByGeneSummaryList = new ArrayList<>(geneIds.size());
        try {
            for (Future<KnockoutByGeneSummary> summaryFuture : geneSummaryFutureList) {
                knockoutByGeneSummaryList.add(summaryFuture.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getCause().getMessage())) {
                return OpenCGAResult.empty(KnockoutByGeneSummary.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
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

    public OpenCGAResult<KnockoutByVariantSummary> variantSummary(String studyStr, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getMainCollectionName(study.getFqn());
        String auxCollection = getAuxCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }
        if (!rgaEngine.isAlive(auxCollection)) {
            throw new RgaException("Missing auxiliar RGA collection for study '" + study.getFqn() + "'");
        }

        catalogManager.getAuthorizationManager().checkStudyPermission(study.getUid(), userId,
                StudyAclEntry.StudyPermissions.VIEW_AGGREGATED_VARIANTS);

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        ExecutorService executor = Executors.newFixedThreadPool(4);

        QueryOptions queryOptions = setDefaultLimit(options);

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

        List<String> variantIds;
        try {
            variantIds = getIds(collection, auxCollection, RgaQueryParams.VARIANTS.key(), auxQuery, queryOptions);
            auxQuery.put(RgaDataModel.VARIANTS, variantIds);
        } catch (RgaException e) {
            if (RgaException.NO_RESULTS_FOUND.equals(e.getMessage())) {
                return OpenCGAResult.empty(KnockoutByVariantSummary.class, (int) stopWatch.getTime(TimeUnit.MILLISECONDS));
            }
            throw e;
        }

        Future<VariantDBIterator> variantFuture = executor.submit(
                () -> variantStorageQuery(study.getFqn(), Collections.emptyList(), auxQuery, QueryOptions.empty(), token)
        );

        List<Future<KnockoutByVariantSummary>> variantSummaryList = new ArrayList<>(variantIds.size());
        for (String variantId : variantIds) {
            variantSummaryList.add(executor.submit(() -> calculatePartialSolrVariantSummary(collection, auxQuery, variantId)));
        }

        Map<String, KnockoutByVariantSummary> variantSummaryMap = new HashMap<>();
        try {
            for (Future<KnockoutByVariantSummary> summaryFuture : variantSummaryList) {
                KnockoutByVariantSummary knockoutByVariantSummary = summaryFuture.get();
                variantSummaryMap.put(knockoutByVariantSummary.getId(), knockoutByVariantSummary);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        VariantDBIterator variantDBIterator;
        try {
            variantDBIterator = variantFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RgaException(e.getMessage(), e);
        }

        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();

            VariantAnnotation variantAnnotation = variant.getAnnotation();
            Set<String> geneNames = new HashSet<>();
            for (ConsequenceType consequenceType : variantAnnotation.getConsequenceTypes()) {
                if (consequenceType.getGeneName() != null) {
                    geneNames.add(consequenceType.getGeneName());
                }
            }

            KnockoutByVariantSummary knockoutByVariantSummary = variantSummaryMap.get(variant.getId());
            knockoutByVariantSummary.setDbSnp(variantAnnotation.getId());
            knockoutByVariantSummary.setChromosome(variant.getChromosome());
            knockoutByVariantSummary.setStart(variant.getStart());
            knockoutByVariantSummary.setEnd(variant.getEnd());
            knockoutByVariantSummary.setLength(variant.getLength());
            knockoutByVariantSummary.setReference(variant.getReference());
            knockoutByVariantSummary.setAlternate(variant.getAlternate());
            knockoutByVariantSummary.setType(variant.getType());
            knockoutByVariantSummary.setPopulationFrequencies(variantAnnotation.getPopulationFrequencies());
            knockoutByVariantSummary.setGenes(new ArrayList<>(geneNames));
        }

        List<KnockoutByVariantSummary> knockoutByVariantSummaryList = new ArrayList<>(variantSummaryMap.values());

        int time = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);
        logger.info("Variant summary: {} milliseconds", time);
        OpenCGAResult<KnockoutByVariantSummary> result = new OpenCGAResult<>(time, Collections.emptyList(),
                knockoutByVariantSummaryList.size(), knockoutByVariantSummaryList, -1);

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

    public OpenCGAResult<FacetField> aggregationStats(String studyStr, Query query, QueryOptions options, String fields, String token)
            throws CatalogException, IOException, RgaException {
        Study study = catalogManager.getStudyManager().get(studyStr, QueryOptions.empty(), token).first();
        String userId = catalogManager.getUserManager().getUserId(token);

        catalogManager.getAuthorizationManager().checkCanViewStudy(study.getUid(), userId);

        String collection = getMainCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }
        ParamUtils.checkObj(fields, "Missing mandatory field 'field");

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(QueryOptions.FACET, fields);
        return new OpenCGAResult<>(rgaEngine.facetedQuery(collection, query, queryOptions));
    }

    /***
     * Fetch a list of ids (facetField) considering the limit/skip, using the auxiliary collection to improve performance.
     *
     * @param mainCollection Main RGA collection name.
     * @param auxCollection Auxiliary RGA collection name.
     * @param facetField Field from where ids will be retrieved.
     * @param query User query object.
     * @param options User query options object.
     * @return the list of expected ids.
     * @throws RgaException RgaException.
     * @throws IOException IOException.
     */
    private List<String> getIds(String mainCollection, String auxCollection, String facetField, Query query, QueryOptions options)
            throws RgaException, IOException {
        String auxiliarId = AuxiliarRgaDataModel.MAIN_TO_AUXILIAR_DATA_MODEL_MAP.get(facetField);
        List<String> ids = query.getAsStringList(facetField);
        // If the user is querying by variant id, we don't need to do a facet first
        if (ids.isEmpty()) {
            Query mainCollQuery = generateQuery(query, AuxiliarRgaDataModel.MAIN_TO_AUXILIAR_DATA_MODEL_MAP.keySet(), true);
            Query auxCollQuery = generateQuery(query, AuxiliarRgaDataModel.MAIN_TO_AUXILIAR_DATA_MODEL_MAP.keySet(), false);

            if (!mainCollQuery.isEmpty()) {
                // Perform a facet in the main collection because the user is filtering by other fields not present in the auxiliar coll
                QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, facetField);
                facetOptions.putIfNotNull(QueryOptions.LIMIT, -1);

                DataResult<FacetField> result = rgaEngine.facetedQuery(mainCollection, query, facetOptions);
                if (result.getNumResults() == 0) {
                    throw RgaException.noResultsMatching();
                }

                ids = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
                auxCollQuery.put(facetField, ids);
            }

            // Perform a facet to get the different variant ids matching the user query and using the skip and limit values
            QueryOptions facetOptions = new QueryOptions(QueryOptions.FACET, auxiliarId);
            facetOptions.putIfNotNull(QueryOptions.LIMIT, options.get(QueryOptions.LIMIT));
            facetOptions.putIfNotNull(QueryOptions.SKIP, options.get(QueryOptions.SKIP));

            DataResult<FacetField> result = rgaEngine.auxFacetedQuery(auxCollection, auxCollQuery, facetOptions);
            if (result.getNumResults() == 0) {
                throw RgaException.noResultsMatching();
            }
            ids = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
        }

        return ids;
    }

    /**
     * Generate a new query based on the original query.
     *
     * @param query Original query from where it will be generated the new query.
     * @param fields Fields to be added in the new query (unless inverse is true).
     * @param inverse Flag indicating to generate a new query with the fields passed or absent.
     * @return a new query object.
     */
    private Query generateQuery(Query query, Set<String> fields, boolean inverse) {
        Query newQuery = new Query();
        for (Map.Entry<String, Object> entry : query.entrySet()) {
            if ((fields.contains(entry.getKey()) && !inverse) || (!fields.contains(entry.getKey()) && inverse)) {
                newQuery.put(entry.getKey(), entry.getValue());
            }
        }
        return newQuery;
    }

    private VariantDBIterator variantStorageQuery(String study, List<String> sampleIds, Query query, QueryOptions options, String token)
            throws CatalogException, IOException, StorageEngineException, RgaException {
        String collection = getMainCollectionName(study);

        List<String> variantIds = query.getAsStringList(RgaDataModel.VARIANTS);
        if (variantIds.isEmpty()) {
            DataResult<FacetField> result = rgaEngine.facetedQuery(collection, query,
                    new QueryOptions(QueryOptions.FACET, RgaDataModel.VARIANTS).append(QueryOptions.LIMIT, -1));
            if (result.getNumResults() == 0) {
                return VariantDBIterator.EMPTY_ITERATOR;
            }
            variantIds = result.first().getBuckets().stream().map(FacetField.Bucket::getValue).collect(Collectors.toList());
        }

        if (variantIds.size() > RgaQueryParams.DEFAULT_INDIVIDUAL_LIMIT) {
            throw new RgaException("Too many variants requested");
        }

        Query variantQuery = new Query(VariantQueryParam.ID.key(), variantIds)
                .append(VariantQueryParam.STUDY.key(), study);
        List<VariantField> excludeList = new LinkedList<>();
        excludeList.add(VariantField.ANNOTATION_CYTOBAND);
        excludeList.add(VariantField.ANNOTATION_CONSERVATION);
        excludeList.add(VariantField.ANNOTATION_DRUGS);
        excludeList.add(VariantField.ANNOTATION_GENE_EXPRESSION);

        if (!sampleIds.isEmpty()) {
            variantQuery.append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleIds)
                    .append(VariantQueryParam.INCLUDE_SAMPLE_DATA.key(), "GT,DP");
        } else {
            excludeList.add(VariantField.STUDIES_SAMPLES);
            excludeList.add(VariantField.STUDIES_SAMPLE_DATA_KEYS);
            excludeList.add(VariantField.STUDIES_FILES);
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE, excludeList);
        return variantStorageManager.iterator(variantQuery, queryOptions, token);
    }

    private KnockoutByVariantSummary calculatePartialSolrVariantSummary(String collection, Query query, String variantId)
            throws IOException, RgaException {
        KnockoutByVariantSummary knockoutByVariantSummary = new KnockoutByVariantSummary().setId(variantId);

        Query auxQuery = new Query(query);
        auxQuery.put(RgaDataModel.VARIANTS, variantId);

        // 1. Get clinical significances
        QueryOptions knockoutTypeFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.CLINICAL_SIGNIFICANCES);
        DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, knockoutTypeFacet);
        knockoutByVariantSummary.setClinicalSignificances(facetFieldDataResult.first()
                .getBuckets()
                .stream()
                .map(FacetField.Bucket::getValue)
                .map(ClinicalSignificance::valueOf)
                .collect(Collectors.toList())
        );

        // 2. Get individual knockout type counts
        QueryOptions geneFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.INDIVIDUAL_SUMMARY);
        facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, geneFacet);
        KnockoutTypeCount noParentsCount = new KnockoutTypeCount(auxQuery);
        KnockoutTypeCount singleParentCount = new KnockoutTypeCount(auxQuery);
        KnockoutTypeCount bothParentsCount = new KnockoutTypeCount(auxQuery);

        for (FacetField.Bucket bucket : facetFieldDataResult.first().getBuckets()) {
            CodedIndividual codedIndividual = CodedIndividual.parseEncodedId(bucket.getValue());
            KnockoutTypeCount auxKnockoutType;
            switch (codedIndividual.getNumParents()) {
                case 0:
                    auxKnockoutType = noParentsCount;
                    break;
                case 1:
                    auxKnockoutType = singleParentCount;
                    break;
                case 2:
                    auxKnockoutType = bothParentsCount;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + codedIndividual.getNumParents());
            }

            auxKnockoutType.processFeature(codedIndividual);
        }
        KnockoutStats noParentIndividualStats = new KnockoutStats(noParentsCount.getNumIds(), noParentsCount.getNumHomIds(),
                noParentsCount.getNumCompHetIds(), noParentsCount.getNumHetIds(), noParentsCount.getNumDelOverlapIds());
        KnockoutStats singleParentIndividualStats = new KnockoutStats(singleParentCount.getNumIds(), singleParentCount.getNumHomIds(),
                singleParentCount.getNumCompHetIds(), singleParentCount.getNumHetIds(), singleParentCount.getNumDelOverlapIds());
        KnockoutStats bothParentIndividualStats = new KnockoutStats(bothParentsCount.getNumIds(), bothParentsCount.getNumHomIds(),
                bothParentsCount.getNumCompHetIds(), bothParentsCount.getNumHetIds(), bothParentsCount.getNumDelOverlapIds());

        knockoutByVariantSummary.setIndividualStats(new IndividualKnockoutStats(noParentIndividualStats, singleParentIndividualStats,
                bothParentIndividualStats));

        // 3. Get allele pairs and CT from Variant summary
        knockoutTypeFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.VARIANT_SUMMARY);
        facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, knockoutTypeFacet);

        Set<String> sequenceOntologyTerms = new HashSet<>();
        boolean isCH = false;
        Set<KnockoutVariant> currentVariantSet = new HashSet<>();
        Set<KnockoutVariant> otherVariantSet = new HashSet<>();

        // Generate this new query object so if user is filtering by any id, we still get all the possible CH variant pairs
        Query knockoutTypeQuery = new Query(query);
        knockoutTypeQuery.remove(RgaQueryParams.VARIANTS.key());
        knockoutTypeQuery.remove(RgaQueryParams.DB_SNPS.key());
        KnockoutTypeCount knockoutTypeCount = new KnockoutTypeCount(knockoutTypeQuery);

        for (FacetField.Bucket bucket : facetFieldDataResult.first().getBuckets()) {
            CodedVariant codedVariant = CodedVariant.parseEncodedId(bucket.getValue());
            knockoutTypeCount.processFeature(codedVariant);
            KnockoutVariant auxKnockoutVariant = convertToKnockoutVariant(new Variant(codedVariant.getId()));
            auxKnockoutVariant.setKnockoutType(KnockoutVariant.KnockoutType.valueOf(codedVariant.getKnockoutType()));

            if (variantId.equals(auxKnockoutVariant.getId())) {
                sequenceOntologyTerms.addAll(codedVariant.getConsequenceType());
                currentVariantSet.add(auxKnockoutVariant);
                if (auxKnockoutVariant.getKnockoutType() == KnockoutVariant.KnockoutType.COMP_HET) {
                    isCH = true;
                }
            } else if (auxKnockoutVariant.getKnockoutType() == KnockoutVariant.KnockoutType.COMP_HET) {
                // We only store it otherwise if it is CH
                otherVariantSet.add(auxKnockoutVariant);
            }
        }
        List<SequenceOntologyTerm> sequenceOntologyTermList = new ArrayList<>(sequenceOntologyTerms.size());
        for (String ct : sequenceOntologyTerms) {
            String ctName = decode(ct);
            String ctId = String.format("SO:%0" + (7 - ct.length()) + "d%s", 0, ct);
            sequenceOntologyTermList.add(new SequenceOntologyTerm(ctId, ctName));
        }
        knockoutByVariantSummary.setSequenceOntologyTerms(sequenceOntologyTermList);

        if (isCH) {
            List<KnockoutVariant> allelePairList = new ArrayList<>(currentVariantSet.size() + otherVariantSet.size());
            allelePairList.addAll(currentVariantSet);
            allelePairList.addAll(otherVariantSet);
            knockoutByVariantSummary.setAllelePairs(allelePairList);
            knockoutByVariantSummary.setTranscriptChPairs(knockoutTypeCount.getTranscriptCompHetIdsMap());
        } else {
            knockoutByVariantSummary.setAllelePairs(new ArrayList<>(currentVariantSet));
        }

        return knockoutByVariantSummary;
    }

    private KnockoutByGeneSummary calculateGeneSummary(String collection, Query query, String geneId) throws RgaException, IOException {
        Query auxQuery = new Query(query);
        auxQuery.put(RgaQueryParams.GENE_ID.key(), geneId);

        // 1. Get KnockoutByGene information
        Query individualQuery = new Query(RgaQueryParams.GENE_ID.key(), geneId);
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 1)
                .append(QueryOptions.EXCLUDE, "individuals");
        RgaIterator rgaIterator = rgaEngine.geneQuery(collection, individualQuery, options);

        if (!rgaIterator.hasNext()) {
            throw RgaException.noResultsMatching();
        }
        RgaDataModel rgaDataModel = rgaIterator.next();
        KnockoutByGeneSummary geneSummary = new KnockoutByGeneSummary(rgaDataModel.getGeneId(), rgaDataModel.getGeneName(),
                rgaDataModel.getChromosome(), rgaDataModel.getStart(), rgaDataModel.getEnd(), rgaDataModel.getStrand(),
                rgaDataModel.getGeneBiotype(), null, null);

        // 2. Get KnockoutType counts
        QueryOptions knockoutTypeFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.VARIANT_SUMMARY);
        DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, knockoutTypeFacet);
        KnockoutTypeCount knockoutTypeCount = new KnockoutTypeCount(auxQuery);
        for (FacetField.Bucket variantBucket : facetFieldDataResult.first().getBuckets()) {
            CodedVariant codedFeature = CodedVariant.parseEncodedId(variantBucket.getValue());
            knockoutTypeCount.processFeature(codedFeature);
        }
        VariantKnockoutStats variantStats = new VariantKnockoutStats(knockoutTypeCount.getNumIds(), knockoutTypeCount.getNumHomIds(),
                knockoutTypeCount.getNumCompHetIds(), knockoutTypeCount.getNumPairedCompHetIds(), knockoutTypeCount.getNumHetIds(),
                knockoutTypeCount.getNumDelOverlapIds());
        geneSummary.setVariantStats(variantStats);

        // 3. Get individual knockout type counts
        QueryOptions geneFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.INDIVIDUAL_SUMMARY);
        facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, geneFacet);
        KnockoutTypeCount noParentsCount = new KnockoutTypeCount(auxQuery);
        KnockoutTypeCount singleParentCount = new KnockoutTypeCount(auxQuery);
        KnockoutTypeCount bothParentsCount = new KnockoutTypeCount(auxQuery);

        for (FacetField.Bucket bucket : facetFieldDataResult.first().getBuckets()) {
            CodedIndividual codedIndividual = CodedIndividual.parseEncodedId(bucket.getValue());
            KnockoutTypeCount auxKnockoutType;
            switch (codedIndividual.getNumParents()) {
                case 0:
                    auxKnockoutType = noParentsCount;
                    break;
                case 1:
                    auxKnockoutType = singleParentCount;
                    break;
                case 2:
                    auxKnockoutType = bothParentsCount;
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + codedIndividual.getNumParents());
            }

            auxKnockoutType.processFeature(codedIndividual);
        }
        KnockoutStats noParentIndividualStats = new KnockoutStats(noParentsCount.getNumIds(), noParentsCount.getNumHomIds(),
                noParentsCount.getNumCompHetIds(), noParentsCount.getNumHetIds(), noParentsCount.getNumDelOverlapIds());
        KnockoutStats singleParentIndividualStats = new KnockoutStats(singleParentCount.getNumIds(), singleParentCount.getNumHomIds(),
                singleParentCount.getNumCompHetIds(), singleParentCount.getNumHetIds(), singleParentCount.getNumDelOverlapIds());
        KnockoutStats bothParentIndividualStats = new KnockoutStats(bothParentsCount.getNumIds(), bothParentsCount.getNumHomIds(),
                bothParentsCount.getNumCompHetIds(), bothParentsCount.getNumHetIds(), bothParentsCount.getNumDelOverlapIds());

        geneSummary.setIndividualStats(new IndividualKnockoutStats(noParentIndividualStats, singleParentIndividualStats,
                bothParentIndividualStats));

        return geneSummary;
    }

    private KnockoutByIndividualSummary calculateIndividualSummary(String collection, Query query, String sampleId)
            throws RgaException, IOException {
        Query auxQuery = new Query(query);
        auxQuery.put(RgaQueryParams.SAMPLE_ID.key(), sampleId);

        // 1. Get KnockoutByIndividual information
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.LIMIT, 1)
                .append(QueryOptions.EXCLUDE, "genes");
        RgaIterator rgaIterator = rgaEngine.individualQuery(collection, auxQuery, options);

        if (!rgaIterator.hasNext()) {
            throw RgaException.noResultsMatching();
        }
        RgaDataModel rgaDataModel = rgaIterator.next();

        KnockoutByIndividual knockoutByIndividual = AbstractRgaConverter.fillIndividualInfo(rgaDataModel);
        KnockoutByIndividualSummary knockoutByIndividualSummary = new KnockoutByIndividualSummary(knockoutByIndividual);

        // 2. Get KnockoutType counts
        QueryOptions knockoutTypeFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.VARIANT_SUMMARY);
        DataResult<FacetField> facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, knockoutTypeFacet);
        KnockoutTypeCount knockoutTypeCount = new KnockoutTypeCount(auxQuery);
        for (FacetField.Bucket variantBucket : facetFieldDataResult.first().getBuckets()) {
            CodedVariant codedFeature = CodedVariant.parseEncodedId(variantBucket.getValue());
            knockoutTypeCount.processFeature(codedFeature);
        }
        VariantKnockoutStats variantStats = new VariantKnockoutStats(knockoutTypeCount.getNumIds(), knockoutTypeCount.getNumHomIds(),
                knockoutTypeCount.getNumCompHetIds(), knockoutTypeCount.getNumPairedCompHetIds(), knockoutTypeCount.getNumHetIds(),
                knockoutTypeCount.getNumDelOverlapIds());
        knockoutByIndividualSummary.setVariantStats(variantStats);

        // 3. Get gene name list
        QueryOptions geneFacet = new QueryOptions()
                .append(QueryOptions.LIMIT, -1)
                .append(QueryOptions.FACET, RgaDataModel.GENE_NAME);
        facetFieldDataResult = rgaEngine.facetedQuery(collection, auxQuery, geneFacet);
        List<String> geneIds = facetFieldDataResult.first().getBuckets()
                .stream()
                .map(FacetField.Bucket::getValue)
                .collect(Collectors.toList());
        knockoutByIndividualSummary.setGenes(geneIds);

        return knockoutByIndividualSummary;
    }

    private Set<String> getAuthorisedSamples(String study, Set<String> sampleIds, List<SampleAclEntry.SamplePermissions> otherPermissions,
                                             String userId, String token) throws CatalogException {
        Query query = new Query(SampleDBAdaptor.QueryParams.ID.key(), sampleIds);
        if (org.apache.commons.collections4.CollectionUtils.isNotEmpty(otherPermissions)) {
            query.put(ACL_PARAM, userId + ":" + StringUtils.join(otherPermissions, ","));
        }

        OpenCGAResult<?> distinct = catalogManager.getSampleManager().distinct(study, SampleDBAdaptor.QueryParams.ID.key(), query, token);
        return distinct.getResults().stream().map(String::valueOf).collect(Collectors.toSet());
    }

    private Preprocess individualQueryPreprocess(Study study, Query query, QueryOptions options, String token)
            throws RgaException, CatalogException, IOException {

        String userId = catalogManager.getUserManager().getUserId(token);
        String collection = getMainCollectionName(study.getFqn());
        if (!rgaEngine.isAlive(collection)) {
            throw new RgaException("Missing RGA indexes for study '" + study.getFqn() + "' or solr server not alive");
        }

        Boolean isOwnerOrAdmin = catalogManager.getAuthorizationManager().isOwnerOrAdmin(study.getUid(), userId);

        Preprocess preprocessResult = new Preprocess();
        preprocessResult.setUserId(userId);
        preprocessResult.setOwnerOrAdmin(isOwnerOrAdmin);
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

    private QueryOptions setDefaultLimit(QueryOptions options) {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        if (!queryOptions.containsKey(QueryOptions.LIMIT)) {
            queryOptions.put(QueryOptions.LIMIT, AbstractManager.DEFAULT_LIMIT);
        }
        return queryOptions;
    }

    public void testConnection() throws StorageEngineException {
        rgaEngine.isAlive("test");
    }

    private String getMainCollectionName(String study) {
        return catalogManager.getConfiguration().getDatabasePrefix() + "-rga-" + study.replace("@", "_").replace(":", "_");
    }

    private String getAuxCollectionName(String study) {
        return catalogManager.getConfiguration().getDatabasePrefix() + "-rga-aux-" + study.replace("@", "_").replace(":", "_");
    }

    @Override
    public void close() throws Exception {
        rgaEngine.close();
    }

    private boolean includeVariants(AbstractRgaConverter converter, QueryOptions queryOptions) {
        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            for (String include : converter.getIncludeFields(queryOptions.getAsStringList(QueryOptions.INCLUDE))) {
                if (include.contains("variant")) {
                    return true;
                }
            }
            return false;
        } else if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
            for (String include : converter.getIncludeFromExcludeFields(queryOptions.getAsStringList(QueryOptions.EXCLUDE))) {
                if (include.contains("variant")) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    private class Preprocess {
        private String userId;
        private boolean isOwnerOrAdmin;

        private Query query;
        private QueryOptions queryOptions;

        private long numTotalResults;
        private Event event;

        public Preprocess() {
        }

        public String getUserId() {
            return userId;
        }

        public Preprocess setUserId(String userId) {
            this.userId = userId;
            return this;
        }

        public boolean isOwnerOrAdmin() {
            return isOwnerOrAdmin;
        }

        public Preprocess setOwnerOrAdmin(boolean ownerOrAdmin) {
            isOwnerOrAdmin = ownerOrAdmin;
            return this;
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
}
