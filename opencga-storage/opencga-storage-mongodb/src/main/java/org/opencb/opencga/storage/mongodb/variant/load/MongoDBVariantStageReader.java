package org.opencb.opencga.storage.mongodb.variant.load;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.mongodb.client.model.Filters.*;

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
    private final Collection<String> chromosomes;
    private MongoCursor<Document> iterator;
    private Document next = null;   // Pending variant

    private final Logger logger = LoggerFactory.getLogger(MongoDBVariantStageReader.class);

    public MongoDBVariantStageReader(MongoDBCollection stageCollection, int studyId) {
        this.stageCollection = stageCollection;
        this.studyId = studyId;
        this.chromosomes = Collections.emptyList();
    }

    public MongoDBVariantStageReader(MongoDBCollection stageCollection, int studyId, Collection<String> chromosomes) {
        this.stageCollection = stageCollection;
        this.studyId = studyId;
        this.chromosomes = chromosomes == null ? Collections.emptyList() : chromosomes;
    }

    public Future<Long> countNumVariants() {
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        Future<Long> future = threadPool.submit(() -> stageCollection.nativeQuery().count(getQuery()));

        threadPool.shutdown();
        return future;
    }

    public long countAproxNumVariants() {
        return stageCollection.count().first();
    }

    @Override
    public boolean open() {
        //Filter documents with the selected studyId
        //Sorting by _id
        Bson query = getQuery();
        System.out.println("query = " + query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        FindIterable<Document> iterable = stageCollection.nativeQuery().find(query,
                new QueryOptions(MongoDBCollection.SORT, Sorts.ascending("_id"))
        );
        iterable.batchSize(20);
        this.iterator = iterable.iterator();
        return true;
    }

    protected Bson getQuery() {
        ArrayList<Bson> chrFilters = new ArrayList<>(chromosomes.size());

        for (String chromosome : chromosomes) {
            chrFilters.add(regex("_id", (chromosome.length() == 1 ? "^ " : "^") + chromosome + ":"));
        }
        if (chrFilters.isEmpty()) {
            return exists(Integer.toString(studyId));
        } else {
            return and(exists(Integer.toString(studyId)), or(chrFilters));
        }
    }

    @Override
    public List<Document> read(int b) {
        List<Document> list = new ArrayList<>(b);

        // If there were some pending variant, add to the list.
        Document last = next;
        if (next != null) {
            list.add(next);
            next = null;
        }
        for (int i = list.size(); i < b; i++) {
            if (iterator.hasNext()) {
                last = iterator.next();
                list.add(last);
            }
        }

        if (iterator.hasNext()) {
            // Obtain the LastVariant from the read LastDocument
            Variant lastVar = MongoDBVariantStageLoader.STRING_ID_CONVERTER.convertToDataModelType(last.getString("_id"));
            int start = lastVar.getStart();
            int end = lastVar.getEnd();
            String chr = lastVar.getChromosome();
            while (iterator.hasNext()) {
                // Get the next document. Check if this should be in the current batch.
                // If not, will be added as the first element of the next batch
                next = iterator.next();
                Variant nextVar = MongoDBVariantStageLoader.STRING_ID_CONVERTER.convertToDataModelType(next.getString("_id"));

                // If the last and next variants overlaps, add next to the batch.
                if (nextVar.overlapWith(chr, start, end, true)) {
                    list.add(next);
                    logger.debug("Add overlapping variant last: {}, next: {}", lastVar, nextVar);

                    // Adding next to the batch, next is the new last.
                    last = next;
                    lastVar = nextVar;
                    start = Math.min(start, nextVar.getStart());
                    end = Math.max(end, nextVar.getEnd());
                    next = null;
                } else {
                    // If they are not overlapped, stop looping.
                    break;
                }
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
