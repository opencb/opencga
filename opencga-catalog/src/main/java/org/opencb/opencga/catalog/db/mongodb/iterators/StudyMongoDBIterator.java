package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.utils.ParamUtils;

import java.util.function.Function;

/**
 * Created by pfurio on 31/07/17.
 */
public class StudyMongoDBIterator<E> extends MongoDBIterator<E> {

    private Function<Document, Boolean> studyFilter;
    private Document previousDocument;
    private QueryOptions options;

    public StudyMongoDBIterator(MongoCursor mongoCursor, QueryOptions options) {
        this(mongoCursor, options, null, null);
    }

    public StudyMongoDBIterator(MongoCursor mongoCursor, QueryOptions options, GenericDocumentComplexConverter<E> converter) {
        this(mongoCursor, options, converter, null);
    }

    public StudyMongoDBIterator(MongoCursor mongoCursor, QueryOptions options, Function<Document, Boolean> studyFilter) {
        this(mongoCursor, options, null, studyFilter);
    }

    public StudyMongoDBIterator(MongoCursor mongoCursor, QueryOptions options, GenericDocumentComplexConverter<E> converter,
                                Function<Document, Boolean> studyFilter) {
        super(mongoCursor, converter, null);
        this.mongoCursor = mongoCursor;
        this.converter = converter;
        this.studyFilter = studyFilter;
        this.options = ParamUtils.defaultObject(options, QueryOptions::new);

        getNextStudy();
    }

    private void getNextStudy() {
        if (this.mongoCursor.hasNext()) {
            this.previousDocument = (Document) this.mongoCursor.next();

            if (this.studyFilter != null) {
                while (this.previousDocument != null && !this.studyFilter.apply(this.previousDocument)) {
                       if (this.mongoCursor.hasNext()) {
                           this.previousDocument = (Document) this.mongoCursor.next();
                       } else {
                           this.previousDocument = null;
                       }
                }
            }

            addAclInformation(previousDocument, options);
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
