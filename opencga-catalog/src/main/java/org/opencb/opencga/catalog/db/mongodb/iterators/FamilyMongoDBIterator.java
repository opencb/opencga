package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.core.models.Annotable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class FamilyMongoDBIterator<E> extends AnnotableMongoDBIterator<E> {

    private Function<Document, Document> individualFilter;

    public FamilyMongoDBIterator(MongoCursor mongoCursor, AnnotableConverter<? extends Annotable> converter,
                                 Function<Document, Document> filter, Function<Document, Document> individualFilter,
                                 QueryOptions options) {
        super(mongoCursor, converter, filter, options);
        this.individualFilter = individualFilter;
    }

    @Override
    public E next() {
        Document next = (Document) mongoCursor.next();

        if (filter != null) {
            next = filter.apply(next);
        }

        if (individualFilter != null) {
            // Filter the members list
            Object memberList = next.get(FamilyDBAdaptor.QueryParams.MEMBERS.key());
            if (memberList != null && !((List) memberList).isEmpty()) {
                List<Document> individualList = new ArrayList<>();

                for (Document member : ((List<Document>) memberList)) {
                    individualList.add(individualFilter.apply(member));
                }

                next.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(), individualList);
            }
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }


}
