/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.catalog.db.mongodb.iterators;

import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class ClinicalAnalysisCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private InterpretationDBAdaptor interpretationDBAdaptor;
    private QueryOptions interpretationQueryOptions;

    private QueryOptions options;

    private Queue<Document> clinicalAnalysisListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    private static final String UID = ClinicalAnalysisDBAdaptor.QueryParams.UID.key();
    private static final String VERSION = FamilyDBAdaptor.QueryParams.VERSION.key();

    public ClinicalAnalysisCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, GenericDocumentComplexConverter<E> converter,
                                                  DBAdaptorFactory dbAdaptorFactory, QueryOptions options) {
        this(mongoCursor, converter, dbAdaptorFactory, 0, null, options);
    }

    public ClinicalAnalysisCatalogMongoDBIterator(MongoDBIterator<Document> mongoCursor, GenericDocumentComplexConverter<E> converter,
                                                  DBAdaptorFactory dbAdaptorFactory, long studyUid, String user, QueryOptions options) {
        super(mongoCursor, converter);

        this.user = user;
        this.studyUid = studyUid;

        this.options = options;

        this.interpretationDBAdaptor = dbAdaptorFactory.getInterpretationDBAdaptor();
        this.interpretationQueryOptions = createInnerQueryOptions(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), false);

        this.clinicalAnalysisListBuffer= new LinkedList<>();
        this.logger = LoggerFactory.getLogger(ClinicalAnalysisCatalogMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = clinicalAnalysisListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

        addAclInformation(next, options);

        if (converter != null) {
            return (E) converter.convertToDataModelType(next);
        } else {
            return (E) next;
        }
    }


    @Override
    public boolean hasNext() {
        if (clinicalAnalysisListBuffer.isEmpty()) {
            fetchNextBatch();
        }
        return !clinicalAnalysisListBuffer.isEmpty();
    }

    private void fetchNextBatch() {
        Set<String> interpretationSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document clinicalDocument = (Document) mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = clinicalDocument.getLong(PRIVATE_STUDY_UID);
            }

            clinicalAnalysisListBuffer.add(clinicalDocument);
            counter++;

            // Extract the interpretations
            List<Document> interpretations =
                    (List<Document>) clinicalDocument.get(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key());
            if (ListUtils.isNotEmpty(interpretations) && !options.getBoolean(NATIVE_QUERY)
                    && !options.getBoolean(NATIVE_QUERY + "_" + ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key())) {
                for (Document interpretation : interpretations) {
                    interpretationSet.add(String.valueOf(interpretation.get(UID)));
                }
            }
        }

        Map<String, Document> interpretationMap = new HashMap<>();
        if (!interpretationSet.isEmpty()) {
            // Obtain all those interpretations
            Query query = new Query(InterpretationDBAdaptor.QueryParams.UID.key(), interpretationSet);
            List<Document> interpretationList;
            try {
                if (user != null) {
                    query.put(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                    interpretationList = interpretationDBAdaptor.nativeGet(studyUid, query, interpretationQueryOptions, user).getResults();
                } else {
                    interpretationList = interpretationDBAdaptor.nativeGet(query, interpretationQueryOptions).getResults();
                }
            } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
                logger.warn("Could not obtain the interpretations associated to the clinical analyses: {}", e.getMessage(), e);
                return;
            }

            // Map each interpretation uid to the interpretation entry
            interpretationList.forEach(intepretation -> interpretationMap.put(String.valueOf(intepretation.get(UID)), intepretation));
        }

        if (!interpretationMap.isEmpty()) {

            // Add the interpretations obtained to the corresponding clinical analyses
            clinicalAnalysisListBuffer.forEach(clinicalAnalysis -> {
                List<Document> interpretations = new ArrayList<>();
                List<Document> origInterpretations =
                        (List<Document>) clinicalAnalysis.get(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key());

                // If the interpretations have been returned... (it might have not been fetched due to permissions issues)
                for (Document origInterpretation : origInterpretations) {
                    if (interpretationMap.containsKey(String.valueOf(origInterpretation.get(UID)))) {
                        interpretations.add(new Document(interpretationMap.get(String.valueOf(origInterpretation.get(UID)))));
                    }
                }

                clinicalAnalysis.put(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS.key(), interpretations);
            });
        }
    }

    private QueryOptions createInnerQueryOptions(String fieldProjectionKey, boolean nativeQuery) {
        QueryOptions queryOptions = new QueryOptions(NATIVE_QUERY, nativeQuery);

        if (options.containsKey(QueryOptions.INCLUDE)) {
            List<String> currentIncludeList = options.getAsStringList(QueryOptions.INCLUDE);
            List<String> includeList = new ArrayList<>();
            for (String include : currentIncludeList) {
                if (include.startsWith(fieldProjectionKey + ".")) {
                    includeList.add(include.replace(fieldProjectionKey + ".", ""));
                }
            }
            if (!includeList.isEmpty()) {
                // If we only have include uid, there is no need for an additional query so we will set current options to native query
                boolean includeAdditionalFields = includeList.stream().anyMatch(
                        field -> !field.equals(UID)
                );
                if (includeAdditionalFields) {
                    includeList.add(UID);
                    queryOptions.put(QueryOptions.INCLUDE, includeList);
                } else {
                    // User wants to include fields already retrieved
                    options.put(NATIVE_QUERY + "_" + fieldProjectionKey, true);
                }
            }
        }
        if (options.containsKey(QueryOptions.EXCLUDE)) {
            List<String> currentExcludeList = options.getAsStringList(QueryOptions.EXCLUDE);
            List<String> excludeList = new ArrayList<>();
            for (String exclude : currentExcludeList) {
                if (exclude.startsWith(fieldProjectionKey + ".")) {
                    String replace = exclude.replace(fieldProjectionKey + ".", "");
                    if (!UID.equals(replace) && !VERSION.equals(replace)) {
                        excludeList.add(replace);
                    }
                }
            }
            if (!excludeList.isEmpty()) {
                queryOptions.put(QueryOptions.EXCLUDE, excludeList);
            } else {
                queryOptions.remove(QueryOptions.EXCLUDE);
            }
        }

        return queryOptions;
    }


}
