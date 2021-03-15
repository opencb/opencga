package org.opencb.opencga.storage.core.variant.search;

import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.opencb.opencga.storage.core.variant.search.VariantSearchUtils.inferSpecificSearchIndexSamplesCollection;

/**
 * Created on 01/04/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SamplesSearchIndexVariantQueryExecutor extends AbstractSearchIndexVariantQueryExecutor {

    public SamplesSearchIndexVariantQueryExecutor(VariantDBAdaptor dbAdaptor, VariantSearchManager searchManager, String storageEngineId,
                                                  String dbName, StorageConfiguration configuration, ObjectMap options) {
        super(dbAdaptor, searchManager, storageEngineId, dbName, configuration, options);
    }

    @Override
    public boolean canUseThisExecutor(Query query, QueryOptions options) throws StorageEngineException {
        String samplesCollection = inferSpecificSearchIndexSamplesCollection(query, options, getMetadataManager(), dbName);
        return samplesCollection != null && searchActiveAndAlive(samplesCollection);
    }

    @Override
    public DataResult<Long> count(Query query) {
        try {
            StopWatch watch = StopWatch.createStarted();
            long count = searchManager.count(dbName, query);
            int time = (int) watch.getTime(TimeUnit.MILLISECONDS);
            return new DataResult<>(time, Collections.emptyList(), 1, Collections.singletonList(count), 1);
        } catch (IOException | VariantSearchException e) {
            throw new VariantQueryException("Error querying Solr", e);
        }
    }

    @Override
    protected Object getOrIterator(Query query, QueryOptions options, boolean iterator) throws StorageEngineException {
        String specificSearchIndexSamples = inferSpecificSearchIndexSamplesCollection(query, options, getMetadataManager(), dbName);

        if (specificSearchIndexSamples == null) {
            throw new StorageEngineException("Unable to use executor " + getClass());
        }
        try {
            if (iterator) {
                return searchManager.iterator(specificSearchIndexSamples, query, options);
            } else {
                return searchManager.query(specificSearchIndexSamples, query, options);
            }
        } catch (IOException | VariantSearchException e) {
            throw VariantQueryException.internalException(e);
        }
    }
}
