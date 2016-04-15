package org.opencb.opencga.storage.mongodb.variant.load;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * DataReader for Variant stage collection.
 * Given a MongoDBCollection and a studyId, iterates over the collection
 * returning sorted results.
 *
 * Created on 13/04/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoDBVariantStageReader implements DataReader<Document> {
    private final MongoDBCollection stageCollection;
    private final int studyId;
    private MongoCursor<Document> iterator;


    public MongoDBVariantStageReader(MongoDBCollection stageCollection, int studyId) {
        this.stageCollection = stageCollection;
        this.studyId = studyId;
    }

    public Future<Long> countNumVariants() {
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        Future<Long> future = threadPool.submit(() -> stageCollection.nativeQuery().count(
                Filters.exists(Integer.toString(studyId))
        ));

        threadPool.shutdown();
        return future;
    }

    @Override
    public boolean open() {
        //Filter documents with the selected studyId
        //Sorting by _id
        FindIterable<Document> iterable = stageCollection.nativeQuery().find(
                Filters.exists(Integer.toString(studyId)),
                new QueryOptions(MongoDBCollection.SORT, Sorts.ascending("_id"))
        );
        iterable.batchSize(20);
        this.iterator = iterable.iterator();
        return true;
    }

    @Override
    public List<Document> read(int b) {
        List<Document> list = new ArrayList<>(b);
        for (int i = 0; i < b; i++) {
            if (iterator.hasNext()) {
                list.add(iterator.next());
            }
        }
        return list;
    }

    @Override
    public boolean close() {
        iterator.close();
        return true;
    }


}
