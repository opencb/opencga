/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.variant.load.variants;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

/**
 * Package local class for grouping mongodb operations.
 * Allows thread-safe operations.
 */
public class MongoDBOperations {

//        private List<Document> inserts =  new LinkedList<>();

    // Document may exist, study does not exist
    private final NewStudy newStudy = new NewStudy();

    // Document and study exist
    private final ExistingStudy existingStudy = new ExistingStudy();

    private final Set<String> genotypes = new HashSet<>();

    // Stage documents to cleanup
//    private List<Pair<Bson, Bson>> cleanFromStage = new ArrayList<>();
    private final List<String> documentsToCleanStudies = new ArrayList<>();
    private final List<String> documentsToCleanFiles = new ArrayList<>();
    private final StageSecondaryAlternates secondaryAlternates = new StageSecondaryAlternates();

    private int skipped = 0;
    private int nonInserted = 0;
    /** Extra insertions due to overlapped variants. */
    private int overlappedVariants = 0;
    /** Missing variants. See A3) */
    private long missingVariants = 0;

    /** Missing variants. See A3) . No fill gaps needed*/
    private long missingVariantsNoFillGaps = 0;

    public MongoDBOperations() {
    }

    NewStudy getNewStudy() {
        return newStudy;
    }

    ExistingStudy getExistingStudy() {
        return existingStudy;
    }

    List<String> getDocumentsToCleanStudies() {
        return documentsToCleanStudies;
    }

    List<String> getDocumentsToCleanFiles() {
        return documentsToCleanFiles;
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

    long getMissingVariantsNoFillGaps() {
        return missingVariantsNoFillGaps;
    }

    MongoDBOperations setMissingVariantsNoFillGaps(long missingVariantsNoFillGaps) {
        this.missingVariantsNoFillGaps = missingVariantsNoFillGaps;
        return this;
    }

    public Set<String> getGenotypes() {
        return genotypes;
    }

    StageSecondaryAlternates getSecondaryAlternates() {
        return secondaryAlternates;
    }

    // Document may exist, study does not exist
    class NewStudy {
        private final List<String> ids = new LinkedList<>();
        private final List<Bson> queries = new LinkedList<>();
        private final List<Bson> updates = new LinkedList<>();
        // Used if the document does not exist
        // This collection may be smaller than the previous collections
        private final List<Document> variants = new LinkedList<>();

        List<String> getIds() {
            return ids;
        }

        List<Bson> getQueries() {
            return queries;
        }

        List<Bson> getUpdates() {
            return updates;
        }

        List<Document> getVariants() {
            return variants;
        }
    }

    // Document and study exist
    class ExistingStudy {
        private final List<String> ids = new LinkedList<>();
        private final List<Bson> queries = new LinkedList<>();
        private final List<Bson> updates = new LinkedList<>();

        List<String> getIds() {
            return ids;
        }

        List<Bson> getQueries() {
            return queries;
        }

        List<Bson> getUpdates() {
            return updates;
        }
    }

    // Secondary alternates to be updated in the stage collection
    class StageSecondaryAlternates {
        private final List<String> ids = new LinkedList<>();
        private final List<Bson> queries = new LinkedList<>();
        private final List<Bson> updates = new LinkedList<>();

        List<String> getIds() {
            return ids;
        }

        List<Bson> getQueries() {
            return queries;
        }

        List<Bson> getUpdates() {
            return updates;
        }
    }
}
