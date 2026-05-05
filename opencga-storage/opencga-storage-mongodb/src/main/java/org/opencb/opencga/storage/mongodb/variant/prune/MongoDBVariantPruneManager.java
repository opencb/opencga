package org.opencb.opencga.storage.mongodb.variant.prune;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.prune.VariantPruneManager;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MongoDBVariantPruneManager extends VariantPruneManager {

    private final MongoDBVariantStorageEngine engine;

    public MongoDBVariantPruneManager(MongoDBVariantStorageEngine engine) {
        super(engine);
        this.engine = engine;
    }

    @Override
    protected void runPrune(boolean dryMode, URI outdir) throws StorageEngineException {
        VariantMongoDBAdaptor dbAdaptor = engine.getDBAdaptor();

        // Build defaultCohortIds map: studyId → DEFAULT_COHORT cohortId
        Map<Integer, Integer> defaultCohortIds = new HashMap<>();
        for (Integer studyId : engine.getMetadataManager().getStudies().values()) {
            defaultCohortIds.put(studyId,
                    engine.getMetadataManager().getCohortId(studyId,
                            org.opencb.biodata.models.variant.StudyEntry.DEFAULT_COHORT));
        }

        long timestamp = System.currentTimeMillis();
        long[] result = dbAdaptor.variantsPrune(defaultCohortIds, dryMode, outdir, timestamp);
        long fullCount = result[0];
        long partialCount = result[1];
        long totalCount = fullCount + partialCount;

        if (dryMode) {
            logger.info("Dry run: found {} variants to prune (full: {}, partial: {})",
                    totalCount, fullCount, partialCount);
        } else {
            logger.info("Pruned {} variants (full: {}, partial: {})",
                    totalCount, fullCount, partialCount);
            syncSearchIndex();
        }
    }

    private void syncSearchIndex() throws StorageEngineException {
        VariantSearchManager searchManager = engine.getVariantSearchManager();
        SearchIndexMetadata indexMetadata = searchManager.getSearchIndexMetadataForLoading();
        if (indexMetadata != null && searchManager.isAlive(indexMetadata)) {
            try {
                String collection = searchManager.buildCollectionName(indexMetadata);
                searchManager.getSolrClient().deleteByQuery(collection, "*:*");
                searchManager.getSolrClient().commit(collection);
                engine.secondaryIndex(new Query(), new QueryOptions(), true);
                logger.info("Search index rebuilt after prune");
            } catch (VariantSearchException | IOException e) {
                throw new StorageEngineException("Error syncing search index after prune", e);
            } catch (Exception e) {
                throw new StorageEngineException("Error clearing search index after prune", e);
            }
        }
    }
}
