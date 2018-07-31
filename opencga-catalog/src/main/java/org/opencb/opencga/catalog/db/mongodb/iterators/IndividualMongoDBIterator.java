package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.core.models.Annotable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class IndividualMongoDBIterator<E> extends AnnotableMongoDBIterator<E> {

    private Function<Document, Document> sampleFilter;

    public IndividualMongoDBIterator(MongoCursor mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                 Function<Document, Document> filter, Function<Document, Document> sampleFilter,
                                 QueryOptions options) {
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
            Object origSampleList = next.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
            if (origSampleList != null && !((List) origSampleList).isEmpty()) {
                List<Document> sampleList = new ArrayList<>();

                for (Document member : ((List<Document>) origSampleList)) {
                    sampleList.add(sampleFilter.apply(member));
                }

                next.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
            }
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }

}
