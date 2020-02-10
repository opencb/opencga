package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.common.Annotable;

import java.util.function.Function;

public class AnnotableCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    protected QueryOptions options;
    protected AnnotableConverter<? extends Annotable> converter;

    @Deprecated
    public AnnotableCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                           Function<Document, Document> filter, QueryOptions options) {
        super(mongoCursor, null, null, filter);
        this.options = ParamUtils.defaultObject(options, QueryOptions::new);
        this.converter = converter;
    }

    public AnnotableCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, ClientSession clientSession,
                                           AnnotableConverter<? extends Annotable> converter, Function<Document, Document> filter,
                                           QueryOptions options) {
        super(mongoCursor, clientSession, null, filter);
        this.options = ParamUtils.defaultObject(options, QueryOptions::new);
        this.converter = converter;
    }

    @Override
    public E next() {
        Document next = mongoCursor.next();

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
