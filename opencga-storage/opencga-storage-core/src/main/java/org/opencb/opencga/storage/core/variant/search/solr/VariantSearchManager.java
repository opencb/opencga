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

package org.opencb.opencga.storage.core.variant.search.solr;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.cellbase.core.result.CellBaseDataResponse;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.solr.FacetQueryParser;
import org.opencb.commons.datastore.solr.SolrCollection;
import org.opencb.commons.datastore.solr.SolrManager;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.common.IOUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.SearchConfiguration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.utils.CellBaseUtils;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.io.db.VariantDBReader;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.search.VariantSearchModel;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUpdateDocument;
import org.opencb.opencga.storage.core.variant.search.VariantToSolrBeanConverterTask;
import org.opencb.opencga.storage.core.variant.search.solr.models.SolrCollectionStatus;
import org.opencb.opencga.storage.core.variant.search.solr.models.SolrCoreIndexStatus;
import org.opencb.opencga.storage.core.variant.search.solr.models.SolrReplicaCoreStatus;
import org.opencb.opencga.storage.core.variant.search.solr.models.SolrShardStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED;


/**
 * Created by imedina on 09/11/16.
 * Created by wasim on 09/11/16.
 */
public class VariantSearchManager {

    private final VariantStorageMetadataManager metadataManager;
    private SolrManager solrManager;
    private CellBaseClient cellBaseClient;
    private final ObjectMap options;
    private final String dbName;
    private int insertBatchSize;
    private String defaultConfigSet;

    private Logger logger;

    public static final String SEARCH_ENGINE_ID = "solr";
    public static final String USE_SEARCH_INDEX = "useSearchIndex";
    public static final int DEFAULT_INSERT_BATCH_SIZE = 10000;

    public VariantSearchManager(String dbName, VariantStorageMetadataManager variantStorageMetadataManager,
                                CellBaseUtils cellBaseUtils, StorageConfiguration storageConfiguration, ObjectMap options) {
        this.dbName = dbName;
        metadataManager = variantStorageMetadataManager;
        this.cellBaseClient = cellBaseUtils.getCellBaseClient();
        this.options = options;
        initSolr(storageConfiguration.getSearch());

        logger = LoggerFactory.getLogger(VariantSearchManager.class);
    }

    public boolean isAlive(String collection) {
        return solrManager.isAlive(collection);
    }

    public boolean isAlive(SearchIndexMetadata indexMetadata) {
        return indexMetadata != null && solrManager.isAlive(buildCollectionName(indexMetadata));
    }

    public void createCollections(SearchIndexMetadata indexMetadata) throws VariantSearchException {
        String mainCollection = buildCollectionName(indexMetadata);
        createCollection(mainCollection, indexMetadata.getConfigSetId(), null, true, 2);
        solrManager.checkExists(mainCollection);
    }

    private String createCollection(String collection, String configSet, String withCollection) throws VariantSearchException {
        return createCollection(collection, configSet, withCollection, true, 2);
    }

    private String createCollection(String collection, String configSet, String withCollection, boolean sharding, Integer maxReplicas)
            throws VariantSearchException {
        try {
            if (this.existsCollection(collection)) {
                this.logger.info("Solr cloud collection {} already exists", collection);
                SolrCollectionStatus status = getCollectionStatus(collection);
                if (status.getShards().stream().flatMap(shard -> shard.getReplicas().stream())
                        .anyMatch(replica -> !replica.isLeader() && replica.isPreferredLeader())) {
                    logger.warn("Collection {} has replicas that are not leaders but preferred leaders. "
                                    + "Ensure shards are properly balanced before continuing.",
                            collection);
                    balanceCollectionLeaders(collection);
                }
            } else {
                int numNodes = getLiveNodes().size();
                int numReplicas;
                int numShards;
                int maxShardsPerNode;
                if (maxReplicas != null) {
                    // If there are multiple nodes, create collection with at most M replicas
                    numReplicas = Math.min(maxReplicas, numNodes);
                } else {
                    // No max replicas specified, create one replica per node
                    numReplicas = numNodes;
                }
                if (sharding) {
                    int shardsPerNode = options.getInt(VariantStorageOptions.SEARCH_LOAD_SHARDS_PER_NODE.key(),
                            VariantStorageOptions.SEARCH_LOAD_SHARDS_PER_NODE.defaultValue());
                    // Create N shard per node
                    numShards = numNodes * shardsPerNode;
                    maxShardsPerNode = numReplicas * shardsPerNode;
                } else {
                    // Create one shard
                    numShards = 1;
                    maxShardsPerNode = numReplicas;
                }
                logger.info("Create collection with {} shards and {} Tlog replicas", numShards, numReplicas);
                CollectionAdminRequest
                        .createCollection(collection, configSet, numShards, 0, numReplicas, 0)
                        .setMaxShardsPerNode(maxShardsPerNode)
                        .setWithCollection(withCollection)
                        .process(getSolrClient());

                // Ensure shard leaders are correctly balanced
                balanceCollectionLeaders(collection);
            }
            logCollectionStatus(collection);
            return collection;
        } catch (SolrException | SolrServerException | IOException e) {
            throw new VariantSearchException("Error creating Solr collection '" + collection + "'", e);
        }
    }

    public void balanceCollectionLeaders(String collection) throws SolrServerException, IOException {
        // http://solr-node-0:8983/solr/admin/collections?action=BALANCESHARDUNIQUE&collection=my_coll&property=preferredLeader&wt=json
        CollectionAdminRequest.balanceReplicaProperty(collection, "preferredLeader").process(getSolrClient());

        // http://solr-node-0:8983/solr/admin/collections?action=RELOAD&collection=my_coll&wt=json
        CollectionAdminRequest.reloadCollection(collection).process(getSolrClient());

        // http://solr-node-0:8983/solr/admin/collections?action=REBALANCELEADERS&collection=my_coll&wt=json
        CollectionAdminResponse rebalanceResponse = CollectionAdminRequest.rebalanceLeaders(collection).process(getSolrClient());
        logger.info("REBALANCELEADERS({}) = {}", collection, rebalanceResponse);
    }

    public void waitForReplicasInSync(SearchIndexMetadata indexMetadata, int timeout, TimeUnit timeUnit) throws VariantSearchException {
        waitForReplicasInSync(indexMetadata, timeout, timeUnit, true);
    }

    public boolean waitForReplicasInSync(SearchIndexMetadata indexMetadata, int timeout, TimeUnit timeUnit, boolean throwException)
            throws VariantSearchException {
        String collectionName = buildCollectionName(indexMetadata);

        return waitForReplicasInSync(collectionName, timeout, timeUnit, throwException);
    }

    private boolean waitForReplicasInSync(String collectionName, int timeout, TimeUnit timeUnit, boolean throwException)
            throws VariantSearchException {
        long sleepMs = TimeUnit.SECONDS.toMillis(5);

        // FIXME: TASK-6217 - Remove this hardcoded sleep. Check for the last _version of the core
        try {
            Thread.sleep(sleepMs * 2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new VariantSearchException("Interrupted while waiting for TLOG replicas to be in sync", e);
        }
        long timeoutMs = timeUnit.toMillis(timeout);
        while (!tlogReplicasInSync(collectionName)) {
            logger.info("Waiting for TLOG replicas in collection '{}' to be in sync...", collectionName);
            if (timeoutMs < 0) {
                if (throwException) {
                    throw new VariantSearchException("Timeout while waiting for TLOG replicas to be in sync for collection "
                            + "'" + collectionName + "'");
                } else {
                    logger.warn("Timeout while waiting for TLOG replicas to be in sync for collection '{}'", collectionName);
                    return false;
                }
            }
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new VariantSearchException("Interrupted while waiting for TLOG replicas to be in sync", e);
            }
            timeoutMs -= sleepMs;
        }
        return true;
    }

    public boolean tlogReplicasInSync(SearchIndexMetadata indexMetadata) throws VariantSearchException {
        return tlogReplicasInSync(buildCollectionName(indexMetadata));
    }

    public boolean tlogReplicasInSync(String collection) throws VariantSearchException {
        try {
            boolean sync = true;
            SolrCollectionStatus status = getCollectionStatus(collection);

            for (SolrShardStatus shard : status.getShards()) {
                // Get the leader's document count
                SolrReplicaCoreStatus leader = shard.getReplicas().stream()
                        .filter(SolrReplicaCoreStatus::isLeader)
                        .findFirst()
                        .orElseThrow(() -> new VariantSearchException("No leader found for shard: " + shard.getName()));

                long leaderDocCount = getCoreIndexStatus(leader.getBaseUrl(), leader.getCoreName()).getNumDocs();

                // Check TLOG replicas
                for (SolrReplicaCoreStatus replica : shard.getReplicas()) {
                    if ("TLOG".equalsIgnoreCase(replica.getType())) {
                        long replicaDocCount = getCoreIndexStatus(replica.getBaseUrl(), replica.getCoreName()).getNumDocs();
                        if (replicaDocCount != leaderDocCount) {
                            logger.info("TLOG replica '{}' in shard '{}' is not in sync with the leader '{}'. "
                                            + "Leader document count: {}, Replica document count: {}",
                                    replica.getName(), shard.getName(), leader.getName(), leaderDocCount, replicaDocCount);
                            sync = false;
                        }
                    }
                }
            }
            return sync;
        } catch (SolrServerException | IOException e) {
            throw new VariantSearchException("Error checking TLOG replicas for collection '" + collection + "'", e);
        }
    }

    private SolrCoreIndexStatus getCoreIndexStatus(String nodeName, String coreName) throws SolrServerException, IOException {
        try (HttpSolrClient solrClient = new HttpSolrClient.Builder(nodeName).build()) {
            CoreAdminRequest req = new CoreAdminRequest();
            req.setAction(CoreAdminParams.CoreAdminAction.STATUS);
            req.setIndexInfoNeeded(true);
            CoreAdminResponse response = req.process(solrClient);
            ObjectMap map = (ObjectMap) response.getCoreStatus(coreName).toMap(new ObjectMap());
            return new SolrCoreIndexStatus(coreName, map.getNestedMap("index"));
        }
    }

    public SolrCollectionStatus getCollectionStatus(String collection) throws VariantSearchException {
        try {
            ObjectMap responseMap = (ObjectMap) CollectionAdminRequest.getClusterStatus()
                    .setCollectionName(collection)
                    .process(getSolrClient()).toMap(new ObjectMap());

            List<SolrShardStatus> shardsList = new ArrayList<>();
            ObjectMap collectionObject = responseMap.getNestedMap("cluster.collections." + collection);
            String configName = collectionObject.getString("configName");
            ObjectMap shards = collectionObject.getNestedMap("shards");
            for (String shard : shards.keySet()) {
                ObjectMap shardMap = shards.getNestedMap(shard);
                ObjectMap replicas = shardMap.getNestedMap("replicas");

                List<SolrReplicaCoreStatus> replicaList = new ArrayList<>();
                for (String replica : replicas.keySet()) {
                    ObjectMap replicaInfo = replicas.getNestedMap(replica);
                    String replicaType = replicaInfo.getString("type");
                    String nodeName = replicaInfo.getString("node_name");
                    String baseUrl = replicaInfo.getString("base_url");
                    String coreName = replicaInfo.getString("core");
                    String state = replicaInfo.getString("state");
                    boolean leader = replicaInfo.getBoolean("leader");
                    boolean preferredLeader = replicaInfo.getBoolean("property.preferredleader", false);

                    replicaList.add(new SolrReplicaCoreStatus(
                            replica,
                            replicaType,
                            nodeName,
                            baseUrl,
                            coreName,
                            state,
                            leader,
                            preferredLeader,
                            getCoreIndexStatus(baseUrl, coreName)));
                }
                replicaList.sort(Comparator.comparing(SolrReplicaCoreStatus::getNodeName));
                shardsList.add(new SolrShardStatus(shard, replicaList));
            }
            shardsList.sort(Comparator.comparing(SolrShardStatus::getName));
            return new SolrCollectionStatus(collection, configName, shardsList);
        } catch (SolrServerException | IOException e) {
            throw new VariantSearchException("Error getting Solr collection '" + collection + "' status", e);
        }
    }

    public void logCollectionsStatus(SearchIndexMetadata indexMetadata) throws VariantSearchException {
        logCollectionStatus(buildCollectionName(indexMetadata));
    }

    public void logCollectionStatus(String collection) throws VariantSearchException {
        SolrCollectionStatus collectionStatus = getCollectionStatus(collection);
        logCollectionStatus(collectionStatus);
    }

    private void logCollectionStatus(SolrCollectionStatus collectionStatus) {
        logger.info("Collection '{}'", collectionStatus.getName());
        logger.info(" - Size: {} ({} bytes)", IOUtils.humanReadableByteCount(collectionStatus.getSizeInBytes(), false),
                collectionStatus.getSizeInBytes());
        logger.info(" - ConfigSet: {}", collectionStatus.getConfigName());
        logger.info(" - NumDocs: {}", collectionStatus.getNumDocs());

        for (SolrShardStatus shardStatus : collectionStatus.getShards()) {
            logger.info(" * Shard '{}'", shardStatus.getName());
            logger.info("   - Size: {} ({} bytes)", IOUtils.humanReadableByteCount(shardStatus.getSizeInBytes(), false),
                    shardStatus.getSizeInBytes());
            logger.info("   - NumDocs: {}", shardStatus.getNumDocs());
            for (SolrReplicaCoreStatus replicaStatus : shardStatus.getReplicas()) {
                String leaderStatus;
                boolean warn = false;
                if (replicaStatus.isLeader()) {
                    if (replicaStatus.isPreferredLeader()) {
                        leaderStatus = " (leader)";
                    } else {
                        leaderStatus = " (leader, NOT PREFERRED)";
                        warn = true;
                    }
                } else {
                    if (replicaStatus.isPreferredLeader()) {
                        leaderStatus = " (preferred, NOT LEADER)";
                        warn = true;
                    } else {
                        leaderStatus = "";
                    }
                }
                if (warn) {
                    logger.warn("   * Replica '{}'{} <<<<<", replicaStatus.getName(), leaderStatus);
                } else {
                    logger.info("   * Replica '{}'{}", replicaStatus.getName(), leaderStatus);
                }
                logger.info("     - CoreName: {}", replicaStatus.getCoreName());
                logger.info("     - Node: {}", replicaStatus.getNodeName());
                logger.info("     - Type: {}", replicaStatus.getType());
                logger.info("     - Size: {} ({} bytes)",
                        IOUtils.humanReadableByteCount(replicaStatus.getIndexStatus().getSizeInBytes(), false),
                        replicaStatus.getIndexStatus().getSizeInBytes());
                logger.info("     - NumDocs: {}", replicaStatus.getIndexStatus().getNumDocs());
                if (!"active".equals(replicaStatus.getState())) {
                    logger.info("     - State: {}", replicaStatus.getState());
                }
            }
        }
    }

    public List<String> getLiveNodes() throws VariantSearchException {
        // Get number of solr nodes
        try {
            ObjectMap result = new ObjectMap();
            CollectionAdminResponse response = CollectionAdminRequest.getClusterStatus()
                    .process(getSolrClient());
            response.toMap(result);
            return new ObjectMap(result.getMap("cluster")).getAsStringList("live_nodes");
        } catch (SolrServerException | IOException e) {
            throw new VariantSearchException("Error getting Solr live nodes", e);
        }
    }

//    public void createCore(String coreName, String configSet) throws VariantSearchException {
//        try {
//            solrManager.createCore(coreName, configSet);
//        } catch (SolrException e) {
//            throw new VariantSearchException("Error creating Solr core '" + coreName + "'", e);
//        }
//    }
//
//    public void createCollection(String collectionName, String configSet) throws VariantSearchException {
//        try {
//            solrManager.createCollection(collectionName, configSet);
//        } catch (SolrException e) {
//            throw new VariantSearchException("Error creating Solr collection '" + collectionName + "'", e);
//        }
//    }

    public boolean exists(SearchIndexMetadata indexMetadata) throws VariantSearchException {
        String collectionName = buildCollectionName(indexMetadata);
        try {
            return solrManager.exists(collectionName);
        } catch (SolrException e) {
            throw new VariantSearchException("Error asking if Solr collection '" + collectionName + "' exists", e);
        }
    }

    public boolean existsCore(String coreName) throws VariantSearchException {
        try {
            return solrManager.existsCore(coreName);
        } catch (SolrException e) {
            throw new VariantSearchException("Error asking if Solr core '" + coreName + "' exists", e);
        }
    }

    public boolean existsCollection(String collectionName) throws VariantSearchException {
        try {
            return solrManager.existsCollection(collectionName);
        } catch (SolrException e) {
            throw new VariantSearchException("Error asking if Solr collection '" + collectionName + "' exists", e);
        }
    }

    public String buildCollectionName(SearchIndexMetadata indexMetadata) {
        if (indexMetadata == null) {
            throw new NullPointerException("Missing index metadata");
        }
        if (indexMetadata.getCollectionNameSuffix() == null || indexMetadata.getCollectionNameSuffix().isEmpty()) {
            // Backward compatibility
            return dbName;
        } else {
            return dbName + "_" + indexMetadata.getCollectionNameSuffix();
        }
    }

    /**
     * Insert a list of variants into the given Solr collection.
     *
     * @param indexMetadata solr collection metadata
     * @param variants List of variants to insert
     * @throws Exception   On any error
     */
    public void insert(SearchIndexMetadata indexMetadata, List<Variant> variants) throws Exception {
        createCollections(indexMetadata);

        if (CollectionUtils.isNotEmpty(variants)) {
            VariantToSolrBeanConverterTask converterTask = new VariantToSolrBeanConverterTask(
                    solrManager.getSolrClient().getBinder(),
                    metadataManager);

            converterTask.pre();
            List<VariantSearchUpdateDocument> documents = converterTask.apply(variants);
            converterTask.post();

            VariantSolrInputDocumentDataWriter writer = new VariantSolrInputDocumentDataWriter(this, indexMetadata);
            writer.open();
            writer.pre();
            writer.write(documents);
            writer.post();
            writer.close();
        }
    }

    /**
     * Load a Solr core/collection from a variant DB iterator.
     *
     * @param indexMetadata     solr collection metadata
     * @param variantDBIterator Iterator to retrieve the variants to load
     * @param writer Data Writer
     * @return VariantSearchLoadResult
     * @throws VariantSearchException VariantSearchException
     */
    public VariantSearchLoadResult load(SearchIndexMetadata indexMetadata,
                                        VariantDBIterator variantDBIterator,
                                        VariantSolrInputDocumentDataWriter writer)
            throws VariantSearchException {
        if (variantDBIterator == null) {
            throw new VariantSearchException("Missing variant DB iterator when loading Solr variant collection");
        }

        // first, create the collection if it does not exist
        createCollections(indexMetadata);

        int batchSize = options.getInt(
                VariantStorageOptions.SEARCH_LOAD_BATCH_SIZE.key(),
                VariantStorageOptions.SEARCH_LOAD_BATCH_SIZE.defaultValue());
        int numThreads = options.getInt(
                VariantStorageOptions.SEARCH_LOAD_THREADS.key(),
                VariantStorageOptions.SEARCH_LOAD_THREADS.defaultValue());

        ProgressLogger progressLogger = new ProgressLogger("Variants loaded in Solr:");

        VariantToSolrBeanConverterTask converterTask = new VariantToSolrBeanConverterTask(solrManager.getSolrClient().getBinder(),
                metadataManager);

        ParallelTaskRunner<Variant, VariantSearchUpdateDocument> ptr = new ParallelTaskRunner<>(
                new VariantDBReader(variantDBIterator),
                progressLogger
                        .<Variant>asTask(d -> "up to position " + d)
                        .then(converterTask),
                writer,
                ParallelTaskRunner.Config.builder()
                        .setSorted(true)
                        .setBatchSize(batchSize)
                        .setCapacity(2)
                        .setNumTasks(numThreads)
                        .build());

        StopWatch stopWatch = StopWatch.createStarted();
        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new VariantSearchException("Error loading secondary index", e);
        }
        int count = variantDBIterator.getCount();
        logger.info("Variant Search loading done. " + count + " variants indexed in " + TimeUtils.durationToString(stopWatch));
        float ratePerHour = count / (stopWatch.getTime() / 1000f) * 3600;
        logger.info("Insertion rate: " + String.format("%.2f", ratePerHour) + " variants/hour "
                + "(" + String.format("%.2f", ratePerHour / 1000000) + " M/h)");

        waitForReplicasInSync(indexMetadata, 5, TimeUnit.MINUTES, false);
        logCollectionsStatus(indexMetadata);
        return new VariantSearchLoadResult(count, count, 0, writer.getInsertedDocuments(), writer.getPartiallyUpdatedDocuments());
    }

    public boolean isStatsFunctionalQueryEnabled(SearchIndexMetadata indexMetadata) {
        return indexMetadata.getAttributes().getBoolean(SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED.key(), false);
    }

    /**
     * Delete variants a Solr core/collection from a variant DB iterator.
     *
     * @param indexMetadata    solr collection metadata
     * @param variantDBIterator Iterator to retrieve the variants to remove
     * @param progressLogger    Progress logger
     * @return VariantSearchLoadResult
     * @throws VariantSearchException VariantSearchException
     * @throws IOException IOException
     */
    public int delete(SearchIndexMetadata indexMetadata, VariantDBIterator variantDBIterator, ProgressLogger progressLogger)
            throws VariantSearchException, IOException {
        if (variantDBIterator == null) {
            throw new VariantSearchException("Missing variant DB iterator when deleting variants");
        }
        String collection = buildCollectionName(indexMetadata);

        int count = 0;
        List<String> variantList = new ArrayList<>(insertBatchSize);
        while (variantDBIterator.hasNext()) {
            Variant variant = variantDBIterator.next();
            progressLogger.increment(1, () -> "up to position " + variant.toString());
            variantList.add(variant.toString());
            count++;
            if (count % insertBatchSize == 0 || !variantDBIterator.hasNext()) {
                try {
                    delete(collection, variantList);
                } catch (SolrServerException e) {
                    throw new VariantSearchException("Error deleting variants.", e);
                }
                variantList.clear();
            }
        }
        logger.debug("Variant Search delete done: {} variants removed", count);
        return count;
    }

    public void delete(SearchIndexMetadata indexMetadata) throws VariantSearchException {
        String collection = buildCollectionName(indexMetadata);

        logger.info("Removing secondary annotation index {} with configSet {} in status {}",
                collection, indexMetadata.getConfigSetId(), indexMetadata.getStatus());
        try {
            if (solrManager.existsCollection(collection)) {
                solrManager.removeCollection(collection);
                logger.info("Deleted Solr collection '{}'", collection);
            } else {
                logger.warn("Solr collection '{}' does not exist. Nothing to delete.", collection);
            }
            metadataManager.updateProjectMetadata(projectMetadata -> {
                projectMetadata.getSecondaryAnnotationIndex().getIndexMetadata(indexMetadata.getVersion()).setStatus(
                        SearchIndexMetadata.Status.REMOVED);
            });
        } catch (SolrException | StorageEngineException e) {
            throw new VariantSearchException("Error deleting Solr collection '" + collection + "'", e);
        }
    }

    /**
     * Return the list of Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param indexMetadata solr collection metadata
     * @param variantQuery Parsed variant query
     * @return List of Variant objects
     * @throws VariantSearchException VariantSearchException
     * @throws IOException   IOException
     */
    public VariantQueryResult<Variant> query(SearchIndexMetadata indexMetadata, ParsedVariantQuery variantQuery)
            throws VariantSearchException, IOException {
        String collection = buildCollectionName(indexMetadata);
        SolrQuery solrQuery = parseQuery(indexMetadata, variantQuery);

        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<Variant> queryResult;
        try {
            queryResult = solrCollection.query(solrQuery, VariantSearchModel.class,
                    new VariantSearchToVariantConverter(variantQuery.getProjection().getFields()));
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error executing variant query", e);
        }

        return new VariantQueryResult<>(queryResult, SEARCH_ENGINE_ID, variantQuery);
    }

    private SolrQuery parseQuery(SearchIndexMetadata indexMetadata, ParsedVariantQuery variantQuery) {
        return parseQuery(indexMetadata, variantQuery.getQuery(), variantQuery.getInputOptions());
    }

    private SolrQuery parseQuery(SearchIndexMetadata indexMetadata, Query query, QueryOptions inputOptions) {
        SolrQueryParser parser = new SolrQueryParser(metadataManager, isStatsFunctionalQueryEnabled(indexMetadata));
        SolrQuery solrQuery = parser.parse(query, inputOptions);

        logger.info("Solr query collection: {}", buildCollectionName(indexMetadata));
        logger.info(" - q: {}", solrQuery.getQuery());
        if (solrQuery.getFilterQueries() != null) {
            for (String filterQuery : solrQuery.getFilterQueries()) {
                logger.info(" - fq: {}", filterQuery);
            }
        }
        logger.info(" - fl: {}", solrQuery.getFields());
        if (solrQuery.getSorts() != null) {
            solrQuery.getSorts().forEach(sort -> logger.info(" - sort: {}", sort));
        }
        logger.info(" - rows: {}", solrQuery.getRows());
        if (solrQuery.getStart() != null && solrQuery.getStart() > 0) {
            logger.info(" - start: {}", solrQuery.getStart());
        }
        return solrQuery;
    }

    /**
     * Return the list of VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param indexMetadata solr collection metadata
     * @param query        Query
     * @param queryOptions Query options
     * @return List of VariantSearchModel objects
     * @throws VariantSearchException VariantSearchException
     * @throws IOException   IOException
     */
    public DataResult<VariantSearchModel> nativeQuery(SearchIndexMetadata indexMetadata, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        String collection = buildCollectionName(indexMetadata);
        SolrQuery solrQuery = parseQuery(indexMetadata, query, queryOptions);
        SolrCollection solrCollection = solrManager.getCollection(collection);
        DataResult<VariantSearchModel> queryResult;
        try {
            queryResult = solrCollection.query(solrQuery, VariantSearchModel.class);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error executing variant query (nativeQuery)", e);
        }

        return queryResult;
    }

    /**
     * Return a Solr variant iterator to retrieve Variant objects from a Solr core/collection
     * according a given query.
     *
     * @param indexMetadata solr collection metadata
     * @param query        Query
     * @param queryOptions Query options
     * @return Solr VariantSearch iterator
     * @throws VariantSearchException VariantSearchException
     * @throws IOException   IOException
     */
    public SolrVariantDBIterator iterator(SearchIndexMetadata indexMetadata, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        String collection = buildCollectionName(indexMetadata);
        try {
            SolrQuery solrQuery = parseQuery(indexMetadata, query, queryOptions);
            return new SolrVariantDBIterator(solrManager.getSolrClient(), collection, solrQuery,
                    new VariantSearchToVariantConverter(VariantField.getIncludeFields(queryOptions)));
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error getting variant iterator", e);
        }
    }

    /**
     * Return a Solr variant iterator to retrieve VariantSearchModel objects from a Solr core/collection
     * according a given query.
     *
     * @param indexMetadata solr collection metadata
     * @param query        Query
     * @param queryOptions Query options
     * @return Solr VariantSearch iterator
     * @throws VariantSearchException VariantSearchException
     */
    public SolrNativeIterator nativeIterator(SearchIndexMetadata indexMetadata, Query query, QueryOptions queryOptions)
            throws VariantSearchException {
        String collection = buildCollectionName(indexMetadata);
        try {
            SolrQuery solrQuery = parseQuery(indexMetadata, query, queryOptions);
            return new SolrNativeIterator(solrManager.getSolrClient(), collection, solrQuery);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error getting variant iterator (native)", e);
        }
    }

    /**
     *
     * @param indexMetadata solr collection metadata
     * @param query      Query
     * @return Number of results
     * @throws VariantSearchException VariantSearchException
     * @throws IOException IOException
     */
    public long count(SearchIndexMetadata indexMetadata, Query query) throws VariantSearchException, IOException {
        String collection = buildCollectionName(indexMetadata);
        SolrQuery solrQuery = parseQuery(indexMetadata, query, new QueryOptions(QueryOptions.INCLUDE, VariantField.ID.fieldName())
                .append(QueryOptions.LIMIT, 0));
        SolrCollection solrCollection = solrManager.getCollection(collection);

        try {
            return solrCollection.count(solrQuery).getResults().get(0);
        } catch (SolrServerException e) {
            throw new VariantSearchException("Error executing count for a given query", e);
        }
    }

    /**
     * Return faceted data from a Solr core/collection
     * according a given query.
     *
     * @param indexMetadata solr collection metadata
     * @param query        Query
     * @param queryOptions Query options (contains the facet and facetRange options)
     * @return List of Variant objects
     * @throws VariantSearchException VariantSearchException
     * @throws IOException IOException
     */
    public DataResult<FacetField> facetedQuery(SearchIndexMetadata indexMetadata, Query query, QueryOptions queryOptions)
            throws VariantSearchException, IOException {
        // Pre-processing
        //   - As "genes" contains, for each gene: gene names, Ensembl gene ID and all its Ensembl transcript IDs,
        //     we do not have to repeat counts for all of them, by default, only for gene names
        //   - consequenceType is replaced by soAcc (i.e., by the field name in the Solr schema)
        boolean replaceSoAcc = false;
        boolean replaceGenes = false;
        Map<String, Set<String>> includingValuesMap = new HashMap<>();
        if (queryOptions.containsKey(QueryOptions.FACET) && StringUtils.isNotEmpty(queryOptions.getString(QueryOptions.FACET))) {
            String facetQuery = queryOptions.getString(QueryOptions.FACET);

            // Gene management
            if (facetQuery.contains("genes[")
                    && (facetQuery.contains("genes;") || facetQuery.contains("genes>>") || facetQuery.endsWith("genes"))) {
                throw new VariantSearchException("Invalid gene facet query: " + facetQuery);
            }

            try {
                includingValuesMap = new FacetQueryParser().getIncludingValuesMap(facetQuery);
            } catch (Exception e) {
                throw new VariantSearchException("Error parsing faceted query", e);
            }

            if (!facetQuery.contains("genes[") && facetQuery.contains("genes")) {
                // Force to query genes by prefix ENSG
                queryOptions.put(QueryOptions.FACET, facetQuery.replace("genes", "genes[ENSG0*]"));
                replaceGenes = true;
            }

            // Consequence type management
            facetQuery = queryOptions.getString(QueryOptions.FACET);
            if (facetQuery.contains("consequenceType")) {
                replaceSoAcc = true;

                facetQuery = facetQuery.replace("consequenceType", "soAcc");
                queryOptions.put(QueryOptions.FACET, facetQuery);

                String[] split = facetQuery.split("soAcc\\[");
                if (split.length > 1 || facetQuery.startsWith("soAcc[")) {
                    int start = 0;
                    StringBuilder newFacetQuery = new StringBuilder();
                    if (!facetQuery.startsWith("soAcc[")) {
                        newFacetQuery.append(split[0]);
                        start = 1;
                    }
                    for (int i = start; i < split.length; i++) {
                        newFacetQuery.append("soAcc");

                        // Manage values to include
                        int index = split[i].indexOf("]");
                        String strValues = split[i].substring(0, index);
                        String[] arrValues = strValues.split(",");
                        List<String> soAccs = new ArrayList<>();
                        for (String value: arrValues) {
                            String val = value.replace("SO:", "");
                            try {
                                // Try to get SO accession, and if it is a valid SO accession
                                int soAcc = Integer.parseInt(val);
                                if (ConsequenceTypeMappings.accessionToTerm.containsKey(soAcc)) {
                                    soAccs.add(String.valueOf(soAcc));
                                }
                            } catch (NumberFormatException e) {
                                // Otherwise, it is treated as a SO term, and check if it is a valid SO term
                                if (ConsequenceTypeMappings.termToAccession.containsKey(val)) {
                                    soAccs.add(String.valueOf(ConsequenceTypeMappings.termToAccession.get(val)));
                                }
                            }
                        }
                        if (ListUtils.isNotEmpty(soAccs)) {
                            newFacetQuery.append("[").append(StringUtils.join(soAccs, ",")).append("]");
                        }
                    }
                    queryOptions.put(QueryOptions.FACET, newFacetQuery.toString());
                }
            }
        }

        // Query
        String collection = buildCollectionName(indexMetadata);
        SolrQuery solrQuery = parseQuery(indexMetadata, query, queryOptions);
        Postprocessing postprocessing = null;
        String jsonFacet = solrQuery.get("json.facet");
        if (StringUtils.isNotEmpty(jsonFacet) && jsonFacet.contains(SolrQueryParser.CHROM_DENSITY)) {
            postprocessing = new Postprocessing().setFacet(jsonFacet);
        }
        SolrCollection solrCollection = solrManager.getCollection(collection);

        DataResult<FacetField> facetResult;
        try {
            facetResult = solrCollection.facet(solrQuery, null, postprocessing);
        } catch (SolrServerException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
        }

        // Post-processing
        Map<String, String> ensemblGeneIdToGeneName = null;
        if (replaceGenes) {
            List<String> ensemblGeneIds = getEnsemblGeneIds(facetResult.getResults());
            CellBaseDataResponse<Gene> geneCellBaseDataResponse = cellBaseClient.getGeneClient().get(ensemblGeneIds, QueryOptions.empty());
            ensemblGeneIdToGeneName = new HashMap<>();
            for (Gene gene: geneCellBaseDataResponse.allResults()) {
                String name = StringUtils.isEmpty(gene.getName()) ? gene.getId() : gene.getName();
                ensemblGeneIdToGeneName.put(gene.getId(), name);
            }
        }

        facetPostProcessing(facetResult.getResults(), includingValuesMap, ensemblGeneIdToGeneName, replaceSoAcc);

        return facetResult;
    }

    public void close() throws IOException {
        solrManager.close();
    }

    public DataWriter<Variant> getVariantDeleter(SearchIndexMetadata indexMetadata) {
        String collection = buildCollectionName(indexMetadata);
        return list -> {
            try {
                if (list != null) {
                    delete(collection, list.stream().map(Variant::toString).collect(Collectors.toList()));
                }
            } catch (IOException | SolrServerException e) {
                throw new RuntimeException(e);
            }
            return true;
        };
    }

    /*-------------------------------------
     *  P R I V A T E    M E T H O D S
     -------------------------------------*/

    private void delete(String collection, List<String> variants) throws IOException, SolrServerException {
        if (CollectionUtils.isNotEmpty(variants)) {
            UpdateResponse updateResponse = solrManager.getSolrClient().deleteById(collection, variants);
            if (updateResponse.getStatus() == 0) {
                solrManager.getSolrClient().commit(collection);
            }
        }
    }

    private List<String> getEnsemblGeneIds(List<FacetField> results) {
        Set<String> ensemblGeneIds = new HashSet<>();
        Queue<FacetField> queue = new LinkedList<>();
        for (FacetField facetField: results) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetField facet = queue.remove();
            for (FacetField.Bucket bucket: facet.getBuckets()) {
                if (bucket.getValue().startsWith("ENSG0")) {
                    ensemblGeneIds.add(bucket.getValue());
                }
                if (ListUtils.isNotEmpty(bucket.getFacetFields())) {
                    for (FacetField facetField: bucket.getFacetFields()) {
                        queue.add(facetField);
                    }
                }

            }
        }
        return new ArrayList<>(ensemblGeneIds);
    }

    private void facetPostProcessing(List<FacetField> results, Map<String, Set<String>> includingValuesMap,
                                     Map<String, String> ensemblGeneIdToGeneName, boolean replaceSoAcc) {
        Queue<FacetField> queue = new LinkedList<>();
        for (FacetField facetField: results) {
            queue.add(facetField);
        }
        while (queue.size() > 0) {
            FacetField facet = queue.remove();
            String facetName = facet.getName();

            boolean toSoTerm = false;
            boolean isGene = false;
            if ("genes".equals(facetName)) {
                isGene = true;
            } else if (replaceSoAcc && "soAcc".equals(facetName)) {
                facet.setName("consequenceType");
                toSoTerm = true;
            }

            List<FacetField.Bucket> validBuckets =  new ArrayList<>();
            Map<String, Set<String>> presentValues = new HashMap<>();
            if (MapUtils.isNotEmpty(includingValuesMap) && CollectionUtils.isNotEmpty(includingValuesMap.get(facetName))) {
                presentValues.put(facetName, new HashSet<>());
            }

            for (FacetField.Bucket bucket : facet.getBuckets()) {
                // We save values for a field name with including values
                if (presentValues.containsKey(facetName)) {
                    presentValues.get(facetName).add(bucket.getValue());
                }

                if (toSoTerm) {
                    bucket.setValue(ConsequenceTypeMappings.accessionToTerm.get(Integer.parseInt(bucket.getValue())));
                } else if (isGene) {
                    if (MapUtils.isNotEmpty(includingValuesMap) && CollectionUtils.isNotEmpty(includingValuesMap.get(facetName))
                            && includingValuesMap.get(facetName).contains(bucket.getValue())) {
                        validBuckets.add(bucket);
                    } else if (ensemblGeneIdToGeneName != null && bucket.getValue().startsWith("ENSG0")) {
                        bucket.setValue(ensemblGeneIdToGeneName.getOrDefault(bucket.getValue(), bucket.getValue()));
                    }
                }

                // Add next fields
                if (ListUtils.isNotEmpty(bucket.getFacetFields())) {
                    for (FacetField facetField: bucket.getFacetFields()) {
                        queue.add(facetField);
                    }
                }
            }
            // For field name 'genes', we have to overwrite the valid buckets (removing Ensembl gene and transcript IDs
            if (CollectionUtils.isNotEmpty(validBuckets)) {
                facet.setBuckets(validBuckets);
            }
            // Check for including values with count equalts to 0, then we include it
            // We save values for a field name with including values
            if (presentValues.containsKey(facetName)) {
                for (String value : includingValuesMap.get(facetName)) {
                    if (!presentValues.get(facetName).contains(value)) {
                        facet.getBuckets().add(new FacetField.Bucket(value, 0, null));
                    }
                }
            }

        }
    }

    public SearchIndexMetadata createMissingIndexMetadata() throws VariantSearchException, IOException,
            StorageEngineException {
        String collectionName = dbName;
        if (existsCollection(collectionName)) {
            CollectionAdminResponse response;
            try {
                response = CollectionAdminRequest.getClusterStatus().setCollectionName(collectionName)
                        .process(getSolrClient());
            } catch (SolrServerException e) {
                throw new VariantSearchException("Error getting cluster status", e);
            }
            NamedList<Object> cluster = (NamedList<Object>) response.getResponse().get("cluster");
            NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
            Map<String, Object>  collection = (Map<String, Object>) collections.get(collectionName);
            String configName = collection.get("configName").toString();
            SearchIndexMetadata indexMetadata = newIndexMetadata(configName, true, new ObjectMap()
                    .append(SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED.key(), false));
            if (metadataManager.getProjectMetadata().getAttributes().containsKey("search.index.last.timestamp")) {
                // If the project metadata has a "search.index.last.timestamp" attribute (old SEARCH_INDEX_LAST_TIMESTAMP),
                // it means that the index was created before the introduction of the SearchIndexMetadata,
                // so we set the last update date to the value of that attribute
                indexMetadata = metadataManager.updateProjectMetadata(projectMetadata -> {
                    projectMetadata.getSecondaryAnnotationIndex().getLastStagingOrActiveIndex().setLastUpdateDate(
                            Date.from(Instant.ofEpochMilli(projectMetadata
                                    .getAttributes().getLong("search.index.last.timestamp"))));
                }).getSecondaryAnnotationIndex().getIndexMetadata(indexMetadata.getVersion());
            }
            return indexMetadata;
        }
        return null;
    }

    public SearchIndexMetadata createIndexMetadataIfEmpty() throws StorageEngineException {
        return newIndexMetadata(true);
    }

    public SearchIndexMetadata newIndexMetadata() throws StorageEngineException {
        return newIndexMetadata(false);
    }

    private SearchIndexMetadata newIndexMetadata(boolean ifNotExists) throws StorageEngineException {
        boolean statsFuncQueryEnabled = options.getBoolean(SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED.key(),
                SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED.defaultValue());
        return newIndexMetadata(defaultConfigSet, ifNotExists, new ObjectMap()
                .append(VariantStorageOptions.SEARCH_STATS_FUNCTIONAL_QUERIES_ENABLED.key(), statsFuncQueryEnabled));
    }

    private SearchIndexMetadata newIndexMetadata(String configSetId, boolean ifNotExists, ObjectMap attributes)
            throws StorageEngineException {
        // Create if it does not exist
        return metadataManager.updateProjectMetadata(projectMetadata -> {
            SearchIndexMetadata indexMetadata = projectMetadata.getSecondaryAnnotationIndex().getLastStagingOrActiveIndex();
            boolean createNewIndexMetadata = false;
            if (ifNotExists) {
                // Create a new index metadata only if it does not exist
                if (indexMetadata == null) {
                    createNewIndexMetadata = true;
                }
            } else {
                // Create a new index metadata
                createNewIndexMetadata = true;
            }
            if (createNewIndexMetadata) {
                newIndexMetadata(projectMetadata, configSetId, attributes);
            }
            return projectMetadata;
        }).getSecondaryAnnotationIndex().getLastStagingOrActiveIndex();
    }

    private void newIndexMetadata(ProjectMetadata projectMetadata, String configSetId, ObjectMap attributes) {
        List<SearchIndexMetadata> values = projectMetadata.getSecondaryAnnotationIndex().getValues();
        int maxVersion = 0;
        for (SearchIndexMetadata value : values) {
            maxVersion = Math.max(maxVersion, value.getVersion());
        }

        int newVersion = maxVersion + 1;
        // Do not add suffix to the collection name if it is the first version for backwards compatibility
        String collectionNameSuffix = newVersion == 1 ? "" : String.valueOf(newVersion);
        SearchIndexMetadata indexMetadata = new SearchIndexMetadata(
                newVersion,
                Date.from(Instant.now()),
                null,
                SearchIndexMetadata.Status.STAGING,
                configSetId,
                collectionNameSuffix,
                attributes
        );
        values.add(indexMetadata);
    }

    public SearchIndexMetadata getSearchIndexMetadata() {
        return metadataManager.getProjectMetadata().getSecondaryAnnotationIndex().getLastStagingOrActiveIndex();
    }

    public ProjectMetadata setActiveIndex(SearchIndexMetadata indexMetadata, long newTimestamp) throws StorageEngineException {
        return metadataManager.updateProjectMetadata(projectMetadata -> {
            for (SearchIndexMetadata value : projectMetadata.getSecondaryAnnotationIndex().getValues()) {
                if (value.getStatus() == SearchIndexMetadata.Status.ACTIVE) {
                    // If there is an active index, update its status to DEPRECATED
                    value.setStatus(SearchIndexMetadata.Status.DEPRECATED);
                }
            }
            projectMetadata.getSecondaryAnnotationIndex().getIndexMetadata(indexMetadata.getVersion())
                    .setStatus(SearchIndexMetadata.Status.ACTIVE)
                    .setLastUpdateDate(Date.from(Instant.ofEpochMilli(newTimestamp)));
            return projectMetadata;
        });
    }

    private class Postprocessing implements SolrCollection.FacetPostprocessing {
        private String facet;

        public Postprocessing setFacet(String facet) {
            this.facet = facet;
            return this;
        }

        @Override
        public org.apache.solr.client.solrj.response.QueryResponse apply(
                org.apache.solr.client.solrj.response.QueryResponse response) {

            // Check buckets for chromosome length
            SimpleOrderedMap solrFacets = (SimpleOrderedMap) response.getResponse().get("facets");
            Iterator iterator = solrFacets.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, SimpleOrderedMap> next = (Map.Entry<String, SimpleOrderedMap>) iterator.next();
                if (next.getValue() instanceof SimpleOrderedMap) {
                    if (next.getKey().startsWith(SolrQueryParser.CHROM_DENSITY)) {
                        SimpleOrderedMap value = next.getValue();
                        if (value.get("chromosome") != null) {
                            List<SimpleOrderedMap<Object>> chromBuckets = (List<SimpleOrderedMap<Object>>)
                                    ((SimpleOrderedMap) value.get("chromosome")).get("buckets");
                            for (SimpleOrderedMap<Object> chromBucket: chromBuckets) {
                                String chrom = chromBucket.get("val").toString();
                                SimpleOrderedMap startMap = (SimpleOrderedMap) chromBucket.get("start");
                                if (startMap != null) {
                                    List<SimpleOrderedMap<Object>> startBuckets =
                                            (List<SimpleOrderedMap<Object>>) startMap.get("buckets");
                                    for (int i = startBuckets.size() - 1; i >= 0; i--) {
                                        int pos = (int) startBuckets.get(i).get("val");
                                        if (pos > SolrQueryParser.getChromosomeMap().get(chrom)) {
                                            startBuckets.remove(i);
                                        } else {
                                            // Should we update "val" to the chromosome length?
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Check for chromosome density range
            return response;
        }
    }

    /*--------------------------------------
     *  toString and GETTERS and SETTERS
     -------------------------------------*/

    public SolrManager getSolrManager() {
        return solrManager;
    }

    public SolrClient getSolrClient() {
        return solrManager.getSolrClient();
    }

    public void initSolr(SearchConfiguration searchConfiguration) {
        SolrManager solrManager = new SolrManager(searchConfiguration.getHosts(),
                searchConfiguration.getMode(),
                searchConfiguration.getTimeout());

        initSolr(searchConfiguration, solrManager);
    }

    public void initSolr(SearchConfiguration searchConfiguration, SolrManager solrManager) {
        // Set internal insert batch size from configuration and default value
        insertBatchSize = searchConfiguration.getInsertBatchSize() > 0
                ? searchConfiguration.getInsertBatchSize()
                : DEFAULT_INSERT_BATCH_SIZE;
        this.defaultConfigSet = searchConfiguration.getConfigSet();

        if (!solrManager.getMode().equals("cloud")) {
            throw new IllegalArgumentException("Search mode '" + solrManager.getMode() + "' is not supported. "
                    + "Only 'cloud' mode is supported for Solr search.");
        }

        this.solrManager = solrManager;
    }

    public int getInsertBatchSize() {
        return insertBatchSize;
    }

}
