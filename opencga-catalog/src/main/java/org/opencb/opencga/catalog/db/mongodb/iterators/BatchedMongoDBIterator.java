package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.function.Function;

public abstract class BatchedMongoDBIterator<T> extends MongoDBIterator<T> {

    private Queue<Document> buffer = new LinkedList<>();

    public BatchedMongoDBIterator(MongoCursor mongoCursor, GenericDocumentComplexConverter<T> converter) {
        super(mongoCursor, converter);
    }

    public BatchedMongoDBIterator(MongoCursor mongoCursor, ClientSession clientSession, GenericDocumentComplexConverter<T> converter,
                                  Function<Document, Document> filter) {
        super(mongoCursor, clientSession, converter, filter);
    }


    @Override
    public boolean hasNext() {
        if (buffer.isEmpty()) {
            fetchNextBatch(buffer, 100);
        }
        return !buffer.isEmpty();
    }

    protected abstract void fetchNextBatch(Queue<Document> buffer, int bufferSize);

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Document next = buffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

        return convert(next);
    }

    protected T convert(Document next) {
        if (converter != null) {
            return converter.convertToDataModelType(next);
        } else {
            return (T) next;
        }
    }
}
