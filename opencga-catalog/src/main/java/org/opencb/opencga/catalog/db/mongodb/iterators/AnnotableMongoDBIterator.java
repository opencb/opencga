package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.core.models.Annotable;

import java.util.function.Function;

public class AnnotableMongoDBIterator<E> extends MongoDBIterator<E> {

    private QueryOptions options;
    private AnnotableConverter<? extends Annotable> converter;

    public AnnotableMongoDBIterator(MongoCursor mongoCursor, QueryOptions options) {
        this(mongoCursor, null, null, options);
    }

    public AnnotableMongoDBIterator(MongoCursor mongoCursor, AnnotableConverter<? extends Annotable> converter, QueryOptions options) {
        this(mongoCursor, converter, null, options);
    }

    public AnnotableMongoDBIterator(MongoCursor mongoCursor, Function<Document, Document> filter, QueryOptions options) {
        this(mongoCursor, null, filter, options);
    }

    public AnnotableMongoDBIterator(MongoCursor mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                    Function<Document, Document> filter, QueryOptions options) {
        super(mongoCursor, null, filter);
        this.options = options;
        this.converter = converter;
    }

    @Override
    public E next() {
        Document next = (Document) mongoCursor.next();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }

}
