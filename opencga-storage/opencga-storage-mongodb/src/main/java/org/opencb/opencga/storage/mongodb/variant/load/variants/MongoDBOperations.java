package org.opencb.opencga.storage.mongodb.variant.load.variants;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.LinkedList;
import java.util.List;

/**
 * Package local class for grouping mongodb operations.
 * Allows thread-safe operations.
 */
public class MongoDBOperations {

//        private List<Document> inserts =  new LinkedList<>();

    // Document may exist, study does not exist
    private NewStudy newStudy = new NewStudy();

    // Document and study exist
    private ExistingStudy existingStudy = new ExistingStudy();

    private int skipped = 0;
    private int nonInserted = 0;
    /** Extra insertions due to overlapped variants. */
    private int overlappedVariants = 0;
    /** Missing variants. See A3) */
    private long missingVariants = 0;

    protected MongoDBOperations() {
    }

    NewStudy getNewStudy() {
        return newStudy;
    }

    MongoDBOperations setNewStudy(NewStudy newStudy) {
        this.newStudy = newStudy;
        return this;
    }

    ExistingStudy getExistingStudy() {
        return existingStudy;
    }

    MongoDBOperations setExistingStudy(ExistingStudy existingStudy) {
        this.existingStudy = existingStudy;
        return this;
    }

    int getSkipped() {
        return skipped;
    }

    MongoDBOperations setSkipped(int skipped) {
        this.skipped = skipped;
        return this;
    }

    int getNonInserted() {
        return nonInserted;
    }

    MongoDBOperations setNonInserted(int nonInserted) {
        this.nonInserted = nonInserted;
        return this;
    }

    int getOverlappedVariants() {
        return overlappedVariants;
    }

    MongoDBOperations setOverlappedVariants(int overlappedVariants) {
        this.overlappedVariants = overlappedVariants;
        return this;
    }

    long getMissingVariants() {
        return missingVariants;
    }

    MongoDBOperations setMissingVariants(long missingVariants) {
        this.missingVariants = missingVariants;
        return this;
    }

    // Document may exist, study does not exist
    class NewStudy {
        private List<String> ids = new LinkedList<>();
        private List<Bson> queries = new LinkedList<>();
        private List<Bson> updates = new LinkedList<>();
        // Used if the document does not exist
        // This collection may be smaller than the previous collections
        private List<Document> variants = new LinkedList<>();

        List<String> getIds() {
            return ids;
        }

        NewStudy setIds(List<String> ids) {
            this.ids = ids;
            return this;
        }

        List<Bson> getQueries() {
            return queries;
        }

        NewStudy setQueries(List<Bson> queries) {
            this.queries = queries;
            return this;
        }

        List<Bson> getUpdates() {
            return updates;
        }

        NewStudy setUpdates(List<Bson> updates) {
            this.updates = updates;
            return this;
        }

        List<Document> getVariants() {
            return variants;
        }

        NewStudy setVariants(List<Document> variants) {
            this.variants = variants;
            return this;
        }
    }

    // Document and study exist
    class ExistingStudy {
        private List<String> ids = new LinkedList<>();
        private List<Bson> queries = new LinkedList<>();
        private List<Bson> updates = new LinkedList<>();

        List<String> getIds() {
            return ids;
        }

        ExistingStudy setIds(List<String> ids) {
            this.ids = ids;
            return this;
        }

        List<Bson> getQueries() {
            return queries;
        }

        ExistingStudy setQueries(List<Bson> queries) {
            this.queries = queries;
            return this;
        }

        List<Bson> getUpdates() {
            return updates;
        }

        ExistingStudy setUpdates(List<Bson> updates) {
            this.updates = updates;
            return this;
        }
    }
}
