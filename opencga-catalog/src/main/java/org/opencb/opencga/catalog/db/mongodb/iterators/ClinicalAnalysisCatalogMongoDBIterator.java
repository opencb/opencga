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
import org.forester.archaeopteryx.Configuration;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.individual.Individual;
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
    private SampleDBAdaptor sampleDBAdaptor;
    private InterpretationDBAdaptor interpretationDBAdaptor;
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
        this.sampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();
        this.interpretationDBAdaptor = dbAdaptorFactory.getInterpretationDBAdaptor();
        this.interpretationQueryOptions = createInnerQueryOptions(INTERPRETATION.key(), false);

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
        Set<String> sampleSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document clinicalDocument = mongoCursor.next();

            if (user != null && studyUid <= 0) {
                studyUid = clinicalDocument.getLong(PRIVATE_STUDY_UID);
            }

            clinicalAnalysisListBuffer.add(clinicalDocument);
            counter++;

            if (!options.getBoolean(NATIVE_QUERY)) {
                extract_family_info((Document) clinicalDocument.get(FAMILY.key()), familySet, individualSet, sampleSet);
                extract_individual_info((Document) clinicalDocument.get(PROBAND.key()), individualSet, sampleSet);

                // Extract the interpretations
                Document interpretationDoc = (Document) clinicalDocument.get(INTERPRETATION.key());
                if (interpretationDoc != null && interpretationDoc.getLong(UID) > 0) {
                    interpretationSet.add(String.valueOf(interpretationDoc.get(UID)));
                }

                List<Document> secondaryInterpretations = (List<Document>) clinicalDocument.get(SECONDARY_INTERPRETATIONS.key());
                if (ListUtils.isNotEmpty(secondaryInterpretations)) {
                    for (Document interpretation : secondaryInterpretations) {
                        interpretationSet.add(String.valueOf(interpretation.get(UID)));
                    }
                }
            }
        }

        Map<String, Document> interpretationMap = fetchInterpretations(interpretationSet);
        Map<String, Document> familyMap = fetchFamilies(familySet);
        Map<String, Document> individualMap = fetchIndividuals(individualSet);
        Map<String, Document> sampleMap = fetchSamples(sampleSet);

        if (!interpretationMap.isEmpty() || !familyMap.isEmpty() || !individualMap.isEmpty() || !sampleMap.isEmpty()) {
            // Fill data in clinical analyses
            clinicalAnalysisListBuffer.forEach(clinicalAnalysis -> {
                fillInterpretationData(clinicalAnalysis, interpretationMap);
                clinicalAnalysis.put(FAMILY.key(), fillFamilyData((Document) clinicalAnalysis.get(FAMILY.key()), familyMap, individualMap,
                        sampleMap));
                clinicalAnalysis.put(PROBAND.key(), fillIndividualData((Document) clinicalAnalysis.get(PROBAND.key()), individualMap,
                        sampleMap));
            });
        }
    }

    private Document fillFamilyData(Document familyDocument, Map<String, Document> familyMap, Map<String, Document> individualMap,
                                    Map<String, Document> sampleMap) {
        if (familyDocument != null && familyDocument.getLong(UID) > 0) {
            // Extract the family id
            String familyId = familyDocument.get(UID) + UID_VERSION_SEP + familyDocument.get(VERSION);

            if (familyMap.containsKey(familyId)) {
                Document completeFamilyDocument = familyMap.get(familyId);

                // Search for members
                List<Document> members = (List<Document>) familyDocument.get(FamilyDBAdaptor.QueryParams.MEMBERS.key());
                if (members != null && !members.isEmpty()) {
                    List<Document> finalMembers = new ArrayList<>(members.size());
                    for (Document memberDocument : members) {
                        finalMembers.add(fillIndividualData(memberDocument, individualMap, sampleMap));
                    }
                    completeFamilyDocument.put(FamilyDBAdaptor.QueryParams.MEMBERS.key(), finalMembers);
                }

                return completeFamilyDocument;
            }
        }

        return familyDocument;
    }

    private Document fillIndividualData(Document individualDoc, Map<String, Document> individualMap, Map<String, Document> sampleMap) {
        if (individualDoc != null && individualDoc.getLong(UID) > 0) {
            // Extract the individual id
            String individualId = individualDoc.get(UID) + UID_VERSION_SEP + individualDoc.get(VERSION);

            if (individualMap.containsKey(individualId)) {
                Document completeIndividualDocument = individualMap.get(individualId);

                // Search for samples
                List<Document> samples = (List<Document>) individualDoc.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
                if (samples != null && !samples.isEmpty()) {
                    List<Document> finalSamples = new ArrayList<>(samples.size());
                    for (Document sampleDoc : samples) {
                        if (sampleDoc != null && sampleDoc.getLong(UID) > 0) {
                            // Extract sample id
                            String sampleId = sampleDoc.get(UID) + UID_VERSION_SEP + sampleDoc.get(VERSION);
                            if (sampleMap.containsKey(sampleId)) {
                                finalSamples.add(sampleMap.get(sampleId));
                            }
                        }
                    }

                    completeIndividualDocument.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), finalSamples);
                }

                return completeIndividualDocument;
            }
        }

        return individualDoc;
    }

    private void fillInterpretationData(Document clinicalAnalysis, Map<String, Document> interpretationMap) {
        Document primaryInterpretation = (Document) clinicalAnalysis.get(INTERPRETATION.key());

        if (primaryInterpretation != null && interpretationMap.containsKey(String.valueOf(primaryInterpretation.get(UID)))) {
            clinicalAnalysis.put(INTERPRETATION.key(), interpretationMap.get(String.valueOf(primaryInterpretation.get(UID))));
        }

        List<Document> origSecondaryInterpretations = (List<Document>) clinicalAnalysis.get(SECONDARY_INTERPRETATIONS.key());
        List<Document> secondaryInterpretations = new ArrayList<>();
        // If the interpretations have been returned... (it might have not been fetched due to permissions issues)
        for (Document origInterpretation : origSecondaryInterpretations) {
            if (interpretationMap.containsKey(String.valueOf(origInterpretation.get(UID)))) {
                secondaryInterpretations.add(new Document(interpretationMap.get(String.valueOf(origInterpretation.get(UID)))));
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
        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, FamilyDBAdaptor.QueryParams.MEMBERS.key());
        try {
            if (user != null) {
                query.put(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                familyList = familyDBAdaptor.nativeGet(studyUid, query, options, user).getResults();
            } else {
                familyList = familyDBAdaptor.nativeGet(query, options).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            logger.warn("Could not obtain the families associated to the clinical analyses: {}", e.getMessage(), e);
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
        List<Long> individualUids = new ArrayList<>(individualSet.size());
        List<Integer> individualUidVersions = new ArrayList<>(individualSet.size());
        for (String individualId : individualSet) {
            String[] split = individualId.split(UID_VERSION_SEP);
            individualUids.add(Long.parseLong(split[0]));
            individualUidVersions.add(Integer.parseInt(split[1]));
        }

        // Build query object
        Query query = new Query()
                .append(IndividualDBAdaptor.QueryParams.UID.key(), individualUids)
                .append(IndividualDBAdaptor.QueryParams.VERSION.key(), individualUidVersions);

        List<Document> individualList;
        QueryOptions options = new QueryOptions(QueryOptions.EXCLUDE, IndividualDBAdaptor.QueryParams.SAMPLES.key());
        try {
            if (user != null) {
                query.put(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                individualList = individualDBAdaptor.nativeGet(studyUid, query, options, user).getResults();
            } else {
                individualList = individualDBAdaptor.nativeGet(query, options).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            logger.warn("Could not obtain the individuals associated to the clinical analyses: {}", e.getMessage(), e);
            return individualMap;
        }

        // Map each individual uid to the individual entry
        individualList.forEach(individual
                -> individualMap.put(individual.get(UID) + UID_VERSION_SEP + individual.get(VERSION), individual));
        return individualMap;
    }

    private Map<String, Document> fetchSamples(Set<String> sampleSet) {
        Map<String, Document> sampleMap = new HashMap<>();

        if (sampleSet.isEmpty()) {
            return sampleMap;
        }

        // Extract list of uids and versions
        List<Long> sampleUids = new ArrayList<>(sampleSet.size());
        List<Integer> sampleUidVersions = new ArrayList<>(sampleSet.size());
        for (String sampleId : sampleSet) {
            String[] split = sampleId.split(UID_VERSION_SEP);
            sampleUids.add(Long.parseLong(split[0]));
            sampleUidVersions.add(Integer.parseInt(split[1]));
        }

        // Build query object
        Query query = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleUids)
                .append(SampleDBAdaptor.QueryParams.VERSION.key(), sampleUidVersions);

        List<Document> sampleList;
        QueryOptions options = new QueryOptions(NATIVE_QUERY, true);
        try {
            if (user != null) {
                query.put(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
                sampleList = sampleDBAdaptor.nativeGet(studyUid, query, options, user).getResults();
            } else {
                sampleList = sampleDBAdaptor.nativeGet(query, options).getResults();
            }
        } catch (CatalogDBException | CatalogAuthorizationException | CatalogParameterException e) {
            logger.warn("Could not obtain the samples associated to the clinical analyses: {}", e.getMessage(), e);
            return sampleMap;
        }

        // Map each sample uid to the sample entry
        sampleList.forEach(sample -> sampleMap.put(sample.get(UID) + UID_VERSION_SEP + sample.get(VERSION), sample));
        return sampleMap;
    }

    private Map<String, Document> fetchInterpretations(Set<String> interpretationSet) {
        Map<String, Document> interpretationMap = new HashMap<>();

        if (interpretationSet.isEmpty()) {
            return interpretationMap;
        }

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
            return interpretationMap;
        }

        // Map each interpretation uid to the interpretation entry
        interpretationList.forEach(intepretation -> interpretationMap.put(String.valueOf(intepretation.get(UID)), intepretation));
        return interpretationMap;
    }

    private void extract_family_info(Document familyDocument, Set<String> familySet, Set<String> individualSet, Set<String> sampleSet) {
        // Extract the family id
        if (familyDocument != null && familyDocument.getLong(UID) > 0) {
            familySet.add(familyDocument.get(UID) + UID_VERSION_SEP + familyDocument.get(VERSION));

            List<Document> members = (List<Document>) familyDocument.get(FamilyDBAdaptor.QueryParams.MEMBERS.key());
            if (members != null && !members.isEmpty()) {
                // Extract individual and sample ids
                for (Document member : members) {
                    extract_individual_info(member, individualSet, sampleSet);
                }
            }
        }
    }

    private void extract_individual_info(Document memberDocument, Set<String> individualSet, Set<String> sampleSet) {
        // Extract individual id
        if (memberDocument != null && memberDocument.getLong(UID) > 0) {
            individualSet.add(memberDocument.get(UID) + UID_VERSION_SEP + memberDocument.get(VERSION));

            List<Document> samples = (List<Document>) memberDocument.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
            if (samples != null && !samples.isEmpty()) {
                // Extract sample ids
                for (Document sampleDocument : samples) {
                    if (sampleDocument != null && sampleDocument.getLong(UID) > 0) {
                        sampleSet.add(sampleDocument.get(UID) + UID_VERSION_SEP + sampleDocument.get(VERSION));
                    }
                }
            }
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
