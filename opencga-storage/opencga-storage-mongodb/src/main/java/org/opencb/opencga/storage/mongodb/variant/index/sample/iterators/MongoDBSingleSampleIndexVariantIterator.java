package org.opencb.opencga.storage.mongodb.variant.index.sample.iterators;

import com.google.common.collect.Iterators;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexEntry;
import org.opencb.opencga.storage.core.variant.index.sample.query.LocusQuery;
import org.opencb.opencga.storage.core.variant.index.sample.query.SampleIndexEntryFilter;
import org.opencb.opencga.storage.core.variant.index.sample.query.SingleSampleIndexQuery;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;
import org.opencb.opencga.storage.mongodb.variant.index.sample.DocumentToSampleIndexEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.index.sample.MongoDBSampleIndexDBAdaptor;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class MongoDBSingleSampleIndexVariantIterator extends VariantDBIterator {

    private final Iterator<Variant> iterator;
    private int count;

    public MongoDBSingleSampleIndexVariantIterator(SingleSampleIndexQuery query, SampleIndexSchema schema,
                                                   MongoDBSampleIndexDBAdaptor adaptor) {
        Collection<LocusQuery> locusQueries;
        if (query.getLocusQueries() == null || query.getLocusQueries().isEmpty()) {
            locusQueries = Collections.singletonList(null);
        } else {
            locusQueries = query.getLocusQueries();
        }

        Iterator<Iterator<Variant>> iterators = locusQueries.stream()
                .map(locusQuery -> buildIterator(adaptor, query, schema, locusQuery))
                .iterator();
        iterator = Iterators.concat(iterators);
    }

    private Iterator<Variant> buildIterator(MongoDBSampleIndexDBAdaptor adaptor,
                                            SingleSampleIndexQuery query,
                                            SampleIndexSchema schema,
                                            LocusQuery locusQuery) {
        MongoDBIterator<Document> mongoDBIterator = adaptor.buildQuery(query, locusQuery, false);
        SampleIndexEntryFilter filter = adaptor.buildSampleIndexEntryFilter(query, locusQuery);
        DocumentToSampleIndexEntryConverter converter = adaptor.getConverter();
        return Iterators.concat(Iterators.transform(mongoDBIterator, document -> {
            SampleIndexEntry entry = converter.convertToDataModelType(document);
            if (entry == null) {
                return Collections.emptyIterator();
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
        return fetch(iterator::hasNext);
    }

    @Override
    public Variant next() {
        Variant variant = fetch(iterator::next);
        count++;
        return variant;
    }

    @Override
    public int getCount() {
        return count;
    }
}
