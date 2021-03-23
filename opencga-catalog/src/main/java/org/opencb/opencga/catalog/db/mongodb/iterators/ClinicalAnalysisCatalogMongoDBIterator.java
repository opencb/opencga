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
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.*;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class ClinicalAnalysisCatalogMongoDBIterator<E> extends CatalogMongoDBIterator<E> {

    private long studyUid;
    private String user;

    private FamilyDBAdaptor familyDBAdaptor;
    private IndividualDBAdaptor individualDBAdaptor;
    private InterpretationDBAdaptor interpretationDBAdaptor;
    private QueryOptions familyQueryOptions;
    private QueryOptions individualQueryOptions;
    private QueryOptions interpretationQueryOptions;

    private QueryOptions options;

    private Queue<Document> clinicalAnalysisListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    private static final String UID = ClinicalAnalysisDBAdaptor.QueryParams.UID.key();
    private static final String VERSION = FamilyDBAdaptor.QueryParams.VERSION.key();

    private static final String UID_VERSION_SEP = "___";

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

        this.familyDBAdaptor = dbAdaptorFactory.getCatalogFamilyDBAdaptor();
        this.individualDBAdaptor = dbAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.interpretationDBAdaptor = dbAdaptorFactory.getInterpretationDBAdaptor();
        this.interpretationQueryOptions = createInnerQueryOptions(INTERPRETATION.key(), false);
        this.familyQueryOptions = createInnerQueryOptions(FAMILY.key(), false);
        this.individualQueryOptions = createInnerQueryOptions(PROBAND.key(), false);

        this.clinicalAnalysisListBuffer = new LinkedList<>();
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
        Set<String> familySet = new HashSet<>();
        Set<String> individualSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document clinicalDocument = mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = clinicalDocument.get(PRIVATE_STUDY_UID, Number.class).longValue();
            }

            clinicalAnalysisListBuffer.add(clinicalDocument);
            counter++;

            if (!options.getBoolean(NATIVE_QUERY)) {
                extractFamilyInfo((Document) clinicalDocument.get(FAMILY.key()), familySet);
                extractIndividualInfo((Document) clinicalDocument.get(PROBAND.key()), individualSet);

                // Extract the interpretations
                Document interpretationDoc = (Document) clinicalDocument.get(INTERPRETATION.key());
                if (interpretationDoc != null && interpretationDoc.get(UID, Number.class).longValue() > 0) {
                    interpretationSet.add(interpretationDoc.get(UID) + UID_VERSION_SEP + interpretationDoc.get(VERSION));
                }

                List<Document> secondaryInterpretations = (List<Document>) clinicalDocument.get(SECONDARY_INTERPRETATIONS.key());
                if (ListUtils.isNotEmpty(secondaryInterpretations)) {
                    for (Document interpretation : secondaryInterpretations) {
                        interpretationSet.add(interpretation.get(UID) + UID_VERSION_SEP + interpretation.get(VERSION));
                    }
                }
            }
        }

        Map<String, Document> interpretationMap = fetchInterpretations(interpretationSet);
        Map<String, Document> familyMap = fetchFamilies(familySet);
        Map<String, Document> individualMap = fetchIndividuals(individualSet);

        if (!interpretationMap.isEmpty() || !familyMap.isEmpty() || !individualMap.isEmpty()) {
            // Fill data in search analyses
            clinicalAnalysisListBuffer.forEach(clinicalAnalysis -> {
                fillInterpretationData(clinicalAnalysis, interpretationMap);
                clinicalAnalysis.put(FAMILY.key(), fillFamilyData((Document) clinicalAnalysis.get(FAMILY.key()), familyMap));
                clinicalAnalysis.put(PROBAND.key(), fillIndividualData((Document) clinicalAnalysis.get(PROBAND.key()), individualMap));
            });
        }
    }

    private Document fillFamilyData(Document familyDocument, Map<String, Document> familyMap) {
        if (familyDocument != null && familyDocument.get(UID, Number.class).longValue() > 0) {
            // Extract the family id
            String familyId = familyDocument.get(UID) + UID_VERSION_SEP + familyDocument.get(VERSION);

            if (familyMap.containsKey(familyId)) {
                Document completeFamilyDocument = familyMap.get(familyId);

                // Search for members
                List<Document> completeMembers = (List<Document>) completeFamilyDocument.get(FamilyDBAdaptor.QueryParams.MEMBERS.key());
                List<Document> members = (List<Document>) familyDocument.get(FamilyDBAdaptor.QueryParams.MEMBERS.key());
                if (members != null && !members.isEmpty() && completeMembers != null && !completeMembers.isEmpty()) {
                    // First, we create a map with references to the complete member information
                    Map<Long, Document> memberMap = new HashMap<>();
                    for (Document completeMember : completeMembers) {
                        memberMap.put(completeMember.get(UID, Number.class).longValue(), completeMember);
                    }

                    List<Document> finalMembers = new ArrayList<>(members.size());
                    for (Document memberDocument : members) {
                        if (memberMap.containsKey(memberDocument.get(UID, Number.class).longValue())) {
                            finalMembers.add(fillIndividualData(memberDocument,
                                    memberMap.get(memberDocument.get(UID, Number.class).longValue())));
                        }
                    }
                    completeFamilyDocument.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(), finalMembers);
                }

                return completeFamilyDocument;
            }
        }

        return familyDocument;
    }

    private Document fillIndividualData(Document individualDoc, Map<String, Document> individualMap) {
        if (individualDoc != null && individualDoc.get(UID, Number.class).longValue() > 0) {
            String individualId = individualDoc.get(UID) + UID_VERSION_SEP + individualDoc.get(VERSION);

            if (individualMap.containsKey(individualId)) {
                return fillIndividualData(individualDoc, individualMap.get(individualId));
            }
        }
        return individualDoc;
    }

    private Document fillIndividualData(Document individualDoc, Document completeIndividualDoc) {
        List<Document> samples = (List<Document>) individualDoc.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
        List<Document> completeSamples = (List<Document>) completeIndividualDoc.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());

        if (samples != null && !samples.isEmpty() && completeSamples != null && !completeSamples.isEmpty()) {
            // First, we store the samples present in the complete individual
            Map<Long, Document> sampleMap = new HashMap<>();
            for (Document completeSample : completeSamples) {
                sampleMap.put(completeSample.get(UID, Number.class).longValue(), completeSample);
            }

            List<Document> finalSamples = new ArrayList<>(samples.size());
            for (Document sample : samples) {
                if (sampleMap.containsKey(sample.get(UID, Number.class).longValue())) {
                    finalSamples.add(sampleMap.get(sample.get(UID, Number.class).longValue()));
                }
            }

            completeIndividualDoc.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), finalSamples);
        }

        return completeIndividualDoc;
    }

    private void fillInterpretationData(Document clinicalAnalysis, Map<String, Document> interpretationMap) {
        if (interpretationMap.isEmpty()) {
            return;
        }

        Document primaryInterpretation = (Document) clinicalAnalysis.get(INTERPRETATION.key());

        if (primaryInterpretation != null
                && interpretationMap.containsKey(primaryInterpretation.get(UID) + UID_VERSION_SEP + primaryInterpretation.get(VERSION))) {
            clinicalAnalysis.put(INTERPRETATION.key(),
                    interpretationMap.get(primaryInterpretation.get(UID) + UID_VERSION_SEP + primaryInterpretation.get(VERSION)));
        }

        List<Document> origSecondaryInterpretations = (List<Document>) clinicalAnalysis.get(SECONDARY_INTERPRETATIONS.key());
        List<Document> secondaryInterpretations = new ArrayList<>();
        // If the interpretations have been returned... (it might have not been fetched due to permissions issues)
        for (Document origInterpretation : origSecondaryInterpretations) {
            String interpretationId = origInterpretation.get(UID) + UID_VERSION_SEP + origInterpretation.get(VERSION);
            if (interpretationMap.containsKey(interpretationId)) {
                secondaryInterpretations.add(new Document(interpretationMap.get(interpretationId)));
            }
        }
        clinicalAnalysis.put(SECONDARY_INTERPRETATIONS.key(), secondaryInterpretations);
    }

    private Map<String, Document> fetchFamilies(Set<String> familySet) {
        Map<String, Document> familyMap = new HashMap<>();

        if (familySet.isEmpty()) {
            return familyMap;
        }

        // Extract list of uids and versions
        List<Long> familyUids = new ArrayList<>(familySet.size());
        List<Integer> familyUidVersions = new ArrayList<>(familySet.size());
        for (String familyId : familySet) {
            String[] split = familyId.split(UID_VERSION_SEP);
            familyUids.add(Long.parseLong(split[0]));
            familyUidVersions.add(Integer.parseInt(split[1]));
        }

        // Build query object
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.UID.key(), familyUids)
                .append(FamilyDBAdaptor.QueryParams.VERSION.key(), familyUidVersions);

        List<Document> familyList;
        try {
            if (user != null) {
                query.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                familyList = familyDBAdaptor.nativeGet(studyUid, query, familyQueryOptions, user).getResults();
            } else {
                familyList = familyDBAdaptor.nativeGet(query, familyQueryOptions).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            logger.warn("Could not obtain the families associated to the search analyses: {}", e.getMessage(), e);
            return familyMap;
        }

        // Map each family uid to the family entry
        familyList.forEach(family -> familyMap.put(family.get(UID) + UID_VERSION_SEP + family.get(VERSION), family));
        return familyMap;
    }

    private Map<String, Document> fetchIndividuals(Set<String> individualSet) {
        Map<String, Document> individualMap = new HashMap<>();

        if (individualSet.isEmpty()) {
            return individualMap;
        }

        // Extract list of uids and versions
        List<Long> singleIndividualUids = new ArrayList<>(individualSet.size());
        List<Long> individualUids = new ArrayList<>(individualSet.size());
        List<Integer> individualUidVersions = new ArrayList<>(individualSet.size());
        for (String individualId : individualSet) {
            String[] split = individualId.split(UID_VERSION_SEP);
            individualUids.add(Long.parseLong(split[0]));
            individualUidVersions.add(Integer.parseInt(split[1]));
        }

        // Fill individuals with version
        List<Document> individualList = queryIndividuals(individualUids, individualUidVersions);
        individualList.forEach(individual
                -> individualMap.put(individual.get(UID) + UID_VERSION_SEP + individual.get(VERSION), individual));

        // Fill individuals without version
        individualList = queryIndividuals(singleIndividualUids, null);
        individualList.forEach(individual -> individualMap.put(String.valueOf(individual.get(UID)), individual));

        return individualMap;
    }

    private List<Document> queryIndividuals(List<Long> individualUids, List<Integer> individualUidVersions) {
        List<Document> individualList = new LinkedList<>();

        if (individualUids.isEmpty()) {
            return individualList;
        }

        // Build query object
        Query query = new Query(IndividualDBAdaptor.QueryParams.UID.key(), individualUids)
                .append(IndividualDBAdaptor.QueryParams.VERSION.key(), individualUidVersions);

        try {
            if (user != null) {
                query.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                individualList = individualDBAdaptor.nativeGet(studyUid, query, individualQueryOptions, user).getResults();
            } else {
                individualList = individualDBAdaptor.nativeGet(query, individualQueryOptions).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            logger.warn("Could not obtain the individuals associated to the search analyses: {}", e.getMessage(), e);
        }
        return individualList;
    }

    private Map<String, Document> fetchInterpretations(Set<String> interpretationSet) {
        Map<String, Document> interpretationMap = new HashMap<>();

        if (interpretationSet.isEmpty()) {
            return interpretationMap;
        }

        // Obtain all those interpretations
        List<Long> interpretationUids = new ArrayList<>(interpretationSet.size());
        List<Integer> interpretationVersions = new ArrayList<>(interpretationSet.size());

        for (String interpretationId : interpretationSet) {
            String[] split = interpretationId.split(UID_VERSION_SEP);
            interpretationUids.add(Long.parseLong(split[0]));
            interpretationVersions.add(Integer.parseInt(split[1]));
        }

        Query query = new Query()
                .append(InterpretationDBAdaptor.QueryParams.UID.key(), interpretationUids)
                .append(InterpretationDBAdaptor.QueryParams.VERSION.key(), interpretationVersions);
        List<Document> interpretationList;
        try {
            if (user != null) {
                query.put(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                interpretationList = interpretationDBAdaptor.nativeGet(studyUid, query, interpretationQueryOptions, user).getResults();
            } else {
                interpretationList = interpretationDBAdaptor.nativeGet(query, interpretationQueryOptions).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            logger.warn("Could not obtain the interpretations associated to the search analyses: {}", e.getMessage(), e);
            return interpretationMap;
        }

        // Map each interpretation uid to the interpretation entry
        interpretationList.forEach(intepretation
                -> interpretationMap.put(intepretation.get(UID) + UID_VERSION_SEP + intepretation.get(VERSION), intepretation));
        return interpretationMap;
    }

    private void extractFamilyInfo(Document familyDocument, Set<String> familySet) {
        // Extract the family id
        if (familyDocument != null && familyDocument.get(UID, Number.class).longValue() > 0) {
            familySet.add(familyDocument.get(UID) + UID_VERSION_SEP + familyDocument.get(VERSION));
        }
    }

    private void extractIndividualInfo(Document memberDocument, Set<String> individualSet) {
        // Extract individual id
        if (memberDocument != null && memberDocument.get(UID, Number.class).longValue() > 0) {
            individualSet.add(memberDocument.get(UID) + UID_VERSION_SEP + memberDocument.get(VERSION));
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
                        field -> !field.equals(UID) && !field.equals(VERSION)
                );
                if (includeAdditionalFields) {
                    includeList.add(UID);
                    includeList.add(VERSION);
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
