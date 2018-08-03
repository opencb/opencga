package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.core.models.Annotable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class FileMongoDBIterator<E> extends AnnotableMongoDBIterator<E> {

    private Function<Document, Document> sampleFilter;

    public FileMongoDBIterator(MongoCursor mongoCursor,  AnnotableConverter<? extends Annotable> converter,
                               Function<Document, Document> filter, Function<Document, Document> sampleFilter, QueryOptions options) {
        super(mongoCursor, converter, filter, options);
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

        Object origSampleList = next.get(CohortDBAdaptor.QueryParams.SAMPLES.key());
        // If the file contains more than 100 samples, we will only leave the id and version information
        if (origSampleList != null && ((List) origSampleList).size() > 100) {
            List<Document> sampleList = new ArrayList<>();

            for (Document sample : ((List<Document>) origSampleList)) {
                sampleList.add(new Document()
                        .append(SampleDBAdaptor.QueryParams.ID.key(), sample.get(SampleDBAdaptor.QueryParams.ID.key()))
                        .append(SampleDBAdaptor.QueryParams.UID.key(), sample.get(SampleDBAdaptor.QueryParams.UID.key()))
                        .append(SampleDBAdaptor.QueryParams.VERSION.key(), sample.get(SampleDBAdaptor.QueryParams.VERSION.key()))
                );
            }

            next.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }

}
