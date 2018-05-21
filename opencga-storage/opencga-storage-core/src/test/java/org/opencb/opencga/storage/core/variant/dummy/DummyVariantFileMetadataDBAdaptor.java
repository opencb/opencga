package org.opencb.opencga.storage.core.variant.dummy;

import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.adaptors.VariantFileMetadataDBAdaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created on 02/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantFileMetadataDBAdaptor implements VariantFileMetadataDBAdaptor {
    @Override
    public QueryResult<Long> count(Query query) {
        return new QueryResult<>();
    }

    @Override
    public void updateVariantFileMetadata(String studyId, VariantFileMetadata variantSource) throws StorageEngineException {

    }

    @Override
    public Iterator<VariantFileMetadata> iterator(Query query, QueryOptions options) throws IOException {
        return Collections.emptyIterator();
    }

    @Override
    public QueryResult updateStats(VariantSourceStats variantSourceStats, StudyConfiguration studyConfiguration, QueryOptions queryOptions) {
        return new QueryResult();
    }

    @Override
    public void delete(int study, int file) {

    }

    @Override
    public void close() throws IOException {

    }
}
