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

package org.opencb.opencga.storage.mongodb.variant.load.stage;

import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoPersistentCursor;
import org.opencb.commons.io.DataReader;
import org.opencb.opencga.storage.mongodb.variant.converters.VariantStringIdConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static org.opencb.opencga.storage.mongodb.variant.converters.stage.StageDocumentToVariantConverter.STUDY_FILE_FIELD;

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
    private Collection<Integer> fileIds;
    private final Collection<String> chromosomes;
    private MongoPersistentCursor iterator;
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

    public MongoDBVariantStageReader setFileIds(Collection<Integer> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public long countNumVariants() {
        return stageCollection.nativeQuery().count(getQuery());
    }

    public long countAproxNumVariants() {
        return stageCollection.count().first();
    }

    @Override
    public boolean open() {
        //Filter documents with the selected studyId and chromosomes
        //Sorting by _id
//        FindIterable<Document> iterable = stageCollection.nativeQuery().find(getQuery(),
//                new QueryOptions(QueryOptions.SORT, Sorts.ascending("_id"))
//        );
//        iterable.batchSize(20);
//        this.iterator = iterable.iterator();
        QueryOptions options = getQueryOptions();
        Bson query = getQuery();
        iterator = new MongoPersistentCursor(stageCollection, query, null, options)
                .setBatchSize(20);
        return true;
    }

    public QueryOptions getQueryOptions() {
        QueryOptions options = new QueryOptions();
        if (fileIds == null || fileIds.isEmpty()) {
            options.put(QueryOptions.SORT, Sorts.ascending("_id"));
        }
        return options;
    }

    protected Bson getQuery() {
        ArrayList<Bson> chrFilters = new ArrayList<>(chromosomes.size());

        for (String chromosome : chromosomes) {
            addChromosomeFilter(chrFilters, chromosome);
        }
        Bson studyFilter;
        if (fileIds != null && !fileIds.isEmpty()) {
            List<String> files = fileIds.stream()
                    .map(fid -> studyId + "_" + fid)
                    .collect(Collectors.toList());
            studyFilter = in(STUDY_FILE_FIELD, files);
        } else {
            studyFilter = eq(STUDY_FILE_FIELD, String.valueOf(studyId));
        }
        Bson bson;
        if (chrFilters.isEmpty()) {
            bson = studyFilter;
        } else {
            bson = and(studyFilter, or(chrFilters)); // Be in any of these chromosomes
        }
        logger.debug("stage filter: " +  bson.toBsonDocument(Document.class, com.mongodb.MongoClient.getDefaultCodecRegistry()));
        return bson;
    }

    public static void addChromosomeFilter(List<Bson> chrFilters, String chromosome) {
        if (chromosome == null || chromosome.isEmpty()) {
            return;
        }
        chromosome = VariantStringIdConverter.convertChromosome(chromosome);
        chrFilters.add(and(
                gte("_id", chromosome + VariantStringIdConverter.SEPARATOR_CHAR),
                lt("_id", chromosome + (char) (VariantStringIdConverter.SEPARATOR_CHAR + 1))));
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
            Variant lastVar = MongoDBVariantStageLoader.STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(last);
            int start = lastVar.getStart();
            int end = lastVar.getEnd();
            String chr = lastVar.getChromosome();
            while (iterator.hasNext()) {
                // Get the next document. Check if this should be in the current batch.
                // If not, will be added as the first element of the next batch
                next = iterator.next();
                Variant nextVar = MongoDBVariantStageLoader.STAGE_TO_VARIANT_CONVERTER.convertToDataModelType(next);

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
