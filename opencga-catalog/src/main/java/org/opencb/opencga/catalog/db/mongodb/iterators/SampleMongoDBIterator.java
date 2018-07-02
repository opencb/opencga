package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.core.models.Annotable;

import java.util.List;
import java.util.function.Function;

public class SampleMongoDBIterator<E> extends AnnotableMongoDBIterator<E> {

    private Function<Document, Document> individualFilter;

    public SampleMongoDBIterator(MongoCursor mongoCursor, AnnotableConverter<? extends Annotable> converter,
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

        Document attributes = (Document) next.get(SampleDBAdaptor.QueryParams.ATTRIBUTES.key());
        if (attributes != null) {
            Object individual = attributes.get("individual");

            // individual might be a list of individuals sometimes. In those case, we will only take the latest individual (higher
            // version)
            if (individual instanceof List) {
                Document myIndividual = null;
                int version = 0;
                for (Document ind : (List<Document>) individual) {
                    if (ind != null && !ind.isEmpty() && ind.getInteger(IndividualDBAdaptor.QueryParams.VERSION.key()) > version) {
                        myIndividual = ind;
                        version = ind.getInteger(IndividualDBAdaptor.QueryParams.VERSION.key());
                    }
                }
                if (myIndividual != null) {
                    attributes.put("individual", myIndividual);
                } else {
                    attributes.remove("individual");
                }
            }
        }

        if (individualFilter != null) {
            // Filter the individual list
            if (attributes != null) {
                Object individual = attributes.get("individual");

                if (individual != null && !((Document) individual).isEmpty()) {
                    attributes.put("individual", individualFilter.apply((Document) individual));
                }
            }
        }

        if (converter != null) {
            return (E) converter.convertToDataModelType(next, options);
        } else {
            return (E) next;
        }
    }

}
