package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class FileMongoDBIterator<E> extends MongoDBIterator<E> {

    private Function<Document, Document> sampleFilter;

    public FileMongoDBIterator(MongoCursor mongoCursor, GenericDocumentComplexConverter<E> converter,
                               Function<Document, Document> filter, Function<Document, Document> sampleFilter) {
        super(mongoCursor, converter, filter);
        this.sampleFilter = sampleFilter;
    }

    @Override
    public E next() {
        Document next = (Document) mongoCursor.next();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (sampleFilter != null) {
            // Filter the samples list
            Object origSampleList = next.get(FileDBAdaptor.QueryParams.SAMPLES.key());
            if (origSampleList != null && !((List) origSampleList).isEmpty()) {
                List<Document> sampleList = new ArrayList<>();

                for (Document member : ((List<Document>) origSampleList)) {
                    sampleList.add(sampleFilter.apply(member));
                }

                next.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
            }
        }

        if (converter != null) {
            return converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }

}
