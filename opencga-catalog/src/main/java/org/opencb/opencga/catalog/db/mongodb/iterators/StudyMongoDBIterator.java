package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.api.DBIterator;

import java.util.function.Function;

/**
 * Created by pfurio on 31/07/17.
 */
public class StudyMongoDBIterator<E> implements DBIterator<E> {

    private MongoCursor mongoCursor;
    private GenericDocumentComplexConverter<E> converter;
    private Function<Document, Boolean> filter;
    private Document previousDocument;

    public StudyMongoDBIterator(MongoCursor mongoCursor) { //Package protected
        this(mongoCursor, null, null);
    }

    public StudyMongoDBIterator(MongoCursor mongoCursor, GenericDocumentComplexConverter<E> converter) { //Package protected
        this(mongoCursor, converter, null);
    }

    public StudyMongoDBIterator(MongoCursor mongoCursor, Function<Document, Boolean> filter) { //Package protected
        this(mongoCursor, null, filter);
    }

    public StudyMongoDBIterator(MongoCursor mongoCursor, GenericDocumentComplexConverter<E> converter, Function<Document, Boolean> filter) {
        //Package protected
        this.mongoCursor = mongoCursor;
        this.converter = converter;
        this.filter = filter;

        getNextStudy();
    }

    private void getNextStudy() {
        if (this.mongoCursor.hasNext()) {
            this.previousDocument = (Document) this.mongoCursor.next();

            if (this.filter != null) {
                while (this.previousDocument != null && !this.filter.apply(this.previousDocument)) {
                       if (this.mongoCursor.hasNext()) {
                           this.previousDocument = (Document) this.mongoCursor.next();
                       } else {
                           this.previousDocument = null;
                       }
                }
            }
        } else {
            this.previousDocument = null;
        }
    }

    @Override
    public boolean hasNext() {
        return this.previousDocument != null;
    }

    @Override
    public E next() {
        Document next = this.previousDocument;
        getNextStudy();

        if (converter != null) {
            return converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }

    @Override
    public void close() {
        mongoCursor.close();
    }


}
