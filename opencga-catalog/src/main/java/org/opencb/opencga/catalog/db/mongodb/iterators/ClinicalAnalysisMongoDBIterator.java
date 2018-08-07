package org.opencb.opencga.catalog.db.mongodb.iterators;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptor.NATIVE_QUERY;

public class ClinicalAnalysisMongoDBIterator<E> extends MongoDBIterator<E>  {

    private long studyUid;
    private String user;

    private FamilyDBAdaptor familyDBAdaptor;
    private IndividualDBAdaptor individualDBAdaptor;
    private FileDBAdaptor fileDBAdaptor;

    private QueryOptions familyQueryOptions;
    private QueryOptions individualQueryOptions;
    private QueryOptions somaticQueryOptions;
    private QueryOptions germlineQueryOptions;

    private QueryOptions options;

    private Queue<Document> clinicalAnalysisListBuffer;

    private Logger logger;

    private static final int BUFFER_SIZE = 100;

    private static final String UID = ClinicalAnalysisDBAdaptor.QueryParams.UID.key();
    private static final String VERSION = FamilyDBAdaptor.QueryParams.VERSION.key();

    public ClinicalAnalysisMongoDBIterator(MongoCursor mongoCursor, GenericDocumentComplexConverter<E> converter,
                                           DBAdaptorFactory dbAdaptorFactory, long studyUid, String user, QueryOptions options) {
        super(mongoCursor, converter);

        this.user = user;
        this.studyUid = studyUid;

        this.options = options;

        this.familyDBAdaptor = dbAdaptorFactory.getCatalogFamilyDBAdaptor();
        this.familyQueryOptions = createInnerQueryOptions(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), false);

        this.individualDBAdaptor = dbAdaptorFactory.getCatalogIndividualDBAdaptor();
        this.individualQueryOptions = createInnerQueryOptions(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key(), false);

        this.fileDBAdaptor = dbAdaptorFactory.getCatalogFileDBAdaptor();
        this.somaticQueryOptions = createInnerQueryOptions(ClinicalAnalysisDBAdaptor.QueryParams.SOMATIC.key(), true);
        this.germlineQueryOptions = createInnerQueryOptions(ClinicalAnalysisDBAdaptor.QueryParams.GERMLINE.key(), true);

        this.clinicalAnalysisListBuffer= new LinkedList<>();
        this.logger = LoggerFactory.getLogger(ClinicalAnalysisMongoDBIterator.class);
    }

    @Override
    public E next() {
        Document next = clinicalAnalysisListBuffer.remove();

        if (filter != null) {
            next = filter.apply(next);
        }

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
        Set<Long> memberSet = new HashSet<>();
        Set<Long> familySet = new HashSet<>();
        Set<Long> somaticSet = new HashSet<>();
        Set<Long> germlineSet = new HashSet<>();

        // Get next BUFFER_SIZE documents
        int counter = 0;
        while (mongoCursor.hasNext() && counter < BUFFER_SIZE) {
            Document clinicalDocument = (Document) mongoCursor.next();

            clinicalAnalysisListBuffer.add(clinicalDocument);
            counter++;

            // Extract all the subjects
            Object members = clinicalDocument.get(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key());
            if (members != null && !options.getBoolean(NATIVE_QUERY)) {
                List<Document> memberList = (List<Document>) members;
                if (!memberList.isEmpty()) {
                    memberList.forEach(subject -> memberSet.add(subject.getLong(UID)));
                }
            }

            // Extract the family uid
            Object family = clinicalDocument.get(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key());
            if (family != null && !options.getBoolean(NATIVE_QUERY)) {
                familySet.add(((Document) family).getLong(UID));
            }

            // Extract the somatic uid
            Object somatic = clinicalDocument.get(ClinicalAnalysisDBAdaptor.QueryParams.SOMATIC.key());
            if (somatic != null && !options.getBoolean(NATIVE_QUERY)) {
                somaticSet.add(((Document) somatic).getLong(UID));
            }

            // Extract the germline uid
            Object germline = clinicalDocument.get(ClinicalAnalysisDBAdaptor.QueryParams.GERMLINE.key());
            if (germline != null && !options.getBoolean(NATIVE_QUERY)) {
                germlineSet.add(((Document) germline).getLong(UID));
            }
        }

        Map<Long, Document> memberMap = new HashMap<>();
        if (!memberSet.isEmpty()) {
            // Obtain all those members
            Query query = new Query()
                    .append(IndividualDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(IndividualDBAdaptor.QueryParams.UID.key(), memberSet);
            List<Document> memberList;
            try {
                if (user != null) {
                    memberList = individualDBAdaptor.nativeGet(query, individualQueryOptions, user).getResult();
                } else {
                    memberList = individualDBAdaptor.nativeGet(query, individualQueryOptions).getResult();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the subjects associated to the clinical analyses: {}", e.getMessage(), e);
                return;
            }

            // Map each member uid to the member entry
            memberList.forEach(member -> memberMap.put(member.getLong(UID), member));
        }

        Map<Long, Document> familyMap = new HashMap<>();
        if (!familySet.isEmpty()) {
            // Obtain all those families
            Query query = new Query()
                    .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(FamilyDBAdaptor.QueryParams.UID.key(), familySet);
            List<Document> familyList;
            try {
                if (user != null) {
                    familyList = familyDBAdaptor.nativeGet(query, familyQueryOptions, user).getResult();
                } else {
                    familyList = familyDBAdaptor.nativeGet(query, familyQueryOptions).getResult();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the families associated to the clinical analyses: {}", e.getMessage(), e);
                return;
            }

            // Map each member uid to the member entry
            familyList.forEach(family -> familyMap.put(family.getLong(UID), family));
        }

        Map<Long, Document> fileMap = new HashMap<>();
        if (!germlineSet.isEmpty()) {
            // Obtain all those germline files
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(FileDBAdaptor.QueryParams.UID.key(), germlineSet);
            List<Document> germlineList;
            try {
                if (user != null) {
                    germlineList = fileDBAdaptor.nativeGet(query, germlineQueryOptions, user).getResult();
                } else {
                    germlineList = fileDBAdaptor.nativeGet(query, germlineQueryOptions).getResult();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the germline files associated to the clinical analyses: {}", e.getMessage(), e);
                return;
            }

            // Map each member uid to the member entry
            germlineList.forEach(file -> fileMap.put(file.getLong(UID), file));
        }

        if (!germlineSet.isEmpty()) {
            // Obtain all those somatic files
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(FileDBAdaptor.QueryParams.UID.key(), somaticSet);
            List<Document> somaticList;
            try {
                if (user != null) {
                    somaticList = fileDBAdaptor.nativeGet(query, somaticQueryOptions, user).getResult();
                } else {
                    somaticList = fileDBAdaptor.nativeGet(query, somaticQueryOptions).getResult();
                }
            } catch (CatalogDBException | CatalogAuthorizationException e) {
                logger.warn("Could not obtain the somatic files associated to the clinical analyses: {}", e.getMessage(), e);
                return;
            }

            // Map each member uid to the member entry
            somaticList.forEach(file -> fileMap.put(file.getLong(UID), file));
        }

        if (!familyMap.isEmpty() || !memberMap.isEmpty() || !fileMap.isEmpty()) {

            // Add the members and families obtained to the corresponding clinical analyses
            clinicalAnalysisListBuffer.forEach(clinicalAnalysis -> {
                List<Document> tmpMemberList = new ArrayList<>();
                List<Document> members = (List<Document>) clinicalAnalysis.get(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key());

                members.forEach(s -> {
                    // If the members has been returned... (it might have not been fetched due to permissions issues)
                    if (memberMap.containsKey(s.getLong(UID))) {
                        Document subjectCopy = new Document(memberMap.get(s.getLong(UID)));

                        // Get original samples array stored in the clinical analysis collection
                        Object samples = s.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
                        if (samples != null) {
                            // Filter out the samples that are not stored in the clinical analysis from the subject
                            Set<Long> presentSampleIds = new HashSet<>();
                            for (Document document : ((List<Document>) samples)) {
                                presentSampleIds.add(document.getLong(UID));
                            }

                            List<Document> finalSamples = new ArrayList<>(presentSampleIds.size());
                            Object samplesObtained = subjectCopy.get(IndividualDBAdaptor.QueryParams.SAMPLES.key());
                            if (samplesObtained != null) {
                                for (Document document : ((List<Document>) samplesObtained)) {
                                    if (presentSampleIds.contains(document.getLong(UID))) {
                                        finalSamples.add(document);
                                    }
                                }
                            }
                            subjectCopy.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), finalSamples);

                        } else {
                            subjectCopy.put(IndividualDBAdaptor.QueryParams.SAMPLES.key(), Collections.emptyList());
                        }


                        tmpMemberList.add(subjectCopy);
                    }
                });
                clinicalAnalysis.put(ClinicalAnalysisDBAdaptor.QueryParams.SUBJECTS.key(), tmpMemberList);

                Document sourceFamily = (Document) clinicalAnalysis.get(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key());
                if (sourceFamily != null && familyMap.containsKey(sourceFamily.getLong(UID))) {
                    clinicalAnalysis.put(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), familyMap.get(sourceFamily.getLong(UID)));
                }

                Document sourceSomatic = (Document) clinicalAnalysis.get(ClinicalAnalysisDBAdaptor.QueryParams.SOMATIC.key());
                if (sourceSomatic != null && fileMap.containsKey(sourceSomatic.getLong(UID))) {
                    clinicalAnalysis.put(ClinicalAnalysisDBAdaptor.QueryParams.SOMATIC.key(), fileMap.get(sourceSomatic.getLong(UID)));
                }

                Document sourceGermline = (Document) clinicalAnalysis.get(ClinicalAnalysisDBAdaptor.QueryParams.GERMLINE.key());
                if (sourceGermline != null && fileMap.containsKey(sourceGermline.getLong(UID))) {
                    clinicalAnalysis.put(ClinicalAnalysisDBAdaptor.QueryParams.GERMLINE.key(), fileMap.get(sourceGermline.getLong(UID)));
                }
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
                includeList.add(VERSION);
                includeList.add(UID);
                queryOptions.put(QueryOptions.INCLUDE, includeList);
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
        if (options.containsKey(Constants.FLATTENED_ANNOTATIONS)) {
            queryOptions.put(Constants.FLATTENED_ANNOTATIONS, options.getBoolean(Constants.FLATTENED_ANNOTATIONS));
        }

        return queryOptions;
    }


}
