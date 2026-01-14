package org.opencb.opencga.storage.mongodb.variant.index.sample.iterators;

import com.google.common.collect.Iterators;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.query.LocusQuery;
import org.opencb.opencga.storage.core.variant.index.sample.query.RawSampleIndexEntryFilter;
import org.opencb.opencga.storage.core.variant.index.sample.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class MongoDBSingleSampleIndexRawIterator extends CloseableIterator<SampleIndexVariant> {

    private final Iterator<SampleIndexVariant> iterator;

    public MongoDBSingleSampleIndexRawIterator(SingleSampleIndexQuery query, SampleIndexSchema schema,
                                               MongoDBSampleIndexDBAdaptor adaptor) {
        Collection<LocusQuery> locusQueries;
        if (query.getLocusQueries() == null || query.getLocusQueries().isEmpty()) {
            locusQueries = Collections.singletonList(null);
        } else {
            locusQueries = query.getLocusQueries();
        }

        Iterator<Iterator<SampleIndexVariant>> iterators = locusQueries.stream()
                .map(locusQuery -> buildIterator(adaptor, query, locusQuery))
                .iterator();
        iterator = Iterators.concat(iterators);
    }

    private Iterator<SampleIndexVariant> buildIterator(MongoDBSampleIndexDBAdaptor adaptor,
                                                       SingleSampleIndexQuery query,
                                                       LocusQuery locusQuery) {
        MongoDBIterator<org.bson.Document> iterable = adaptor.buildQuery(query, locusQuery, true);
        RawSampleIndexEntryFilter filter = new RawSampleIndexEntryFilter(query, locusQuery);
        return Iterators.concat(Iterators.transform(iterable, document -> {
            SampleIndexEntry entry = adaptor.getConverter().convertToDataModelType(document);
            if (entry == null) {
                return Collections.<SampleIndexVariant>emptyIterator();
            }
            try {
                return filter.filter(entry).iterator();
            } catch (RuntimeException e) {
                throw VariantQueryException.internalException(e);
            }
        }));
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public SampleIndexVariant next() {
        return iterator.next();
    }
}
