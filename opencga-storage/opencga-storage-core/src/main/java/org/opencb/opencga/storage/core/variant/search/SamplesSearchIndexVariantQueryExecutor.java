package org.opencb.opencga.storage.core.variant.search;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.VariantSearchException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQuery;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;

import java.io.IOException;

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
    public boolean canUseThisExecutor(ParsedVariantQuery variantQuery, QueryOptions options) throws StorageEngineException {
        VariantQuery query = variantQuery.getQuery();
        String samplesCollection = inferSpecificSearchIndexSamplesCollection(query, options, getMetadataManager(), dbName);
        return samplesCollection != null && searchActiveAndAlive(samplesCollection);
    }

    @Override
    protected Object getOrIterator(ParsedVariantQuery variantQuery, boolean iterator) throws StorageEngineException {
        Query query = variantQuery.getQuery();
        QueryOptions options = variantQuery.getInputOptions();
        String specificSearchIndexSamples = inferSpecificSearchIndexSamplesCollection(query, options, getMetadataManager(), dbName);

        if (specificSearchIndexSamples == null) {
            throw new StorageEngineException("Unable to use executor " + getClass());
        }
        try {
            if (iterator) {
                return searchManager.iterator(specificSearchIndexSamples, query, options);
            } else {
                return searchManager.query(specificSearchIndexSamples, variantQuery);
            }
        } catch (IOException | VariantSearchException e) {
            throw VariantQueryException.internalException(e);
        }
    }
}
