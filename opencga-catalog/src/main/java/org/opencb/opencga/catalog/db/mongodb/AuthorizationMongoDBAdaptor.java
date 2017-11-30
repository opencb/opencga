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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.PermissionRules;
import org.opencb.opencga.core.models.acls.permissions.*;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER_ARRAY;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory.*;

/**
 * Created by pfurio on 20/04/17.
 */
public class AuthorizationMongoDBAdaptor extends MongoDBAdaptor implements AuthorizationDBAdaptor {

    private Map<String, MongoDBCollection> dbCollectionMap = new HashMap<>();
    private Map<String, List<String>> fullPermissionsMap = new HashMap<>();

    private static final String ANONYMOUS = "*";
    private static final String INTERNAL_DELIMITER = "__";

    public AuthorizationMongoDBAdaptor(Configuration configuration) throws CatalogDBException {
        super(LoggerFactory.getLogger(AuthorizationMongoDBAdaptor.class));
        dbAdaptorFactory = new MongoDBAdaptorFactory(configuration);
        initCollectionConnections();
        initPermissions();
    }

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        ACL("_acl", TEXT_ARRAY, "");

        private static Map<String, QueryParams> map = new HashMap<>();

        static {
            for (QueryParams param : QueryParams.values()) {
                map.put(param.key(), param);
            }
        }

        private final String key;
        private Type type;
        private String description;

        QueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public Type type() {
            return type;
        }

        @Override
        public String description() {
            return description;
        }

        public static Map<String, QueryParams> getMap() {
            return map;
        }

        public static QueryParams getParam(String key) {
            return map.get(key);
        }
    }

    private void initCollectionConnections() {
        this.dbCollectionMap.put(STUDY_COLLECTION, dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection());
        this.dbCollectionMap.put(COHORT_COLLECTION, dbAdaptorFactory.getCatalogCohortDBAdaptor().getCohortCollection());
        this.dbCollectionMap.put(DATASET_COLLECTION, dbAdaptorFactory.getCatalogDatasetDBAdaptor().getDatasetCollection());
        this.dbCollectionMap.put(FILE_COLLECTION, dbAdaptorFactory.getCatalogFileDBAdaptor().getFileCollection());
        this.dbCollectionMap.put(INDIVIDUAL_COLLECTION, dbAdaptorFactory.getCatalogIndividualDBAdaptor().getCollection());
        this.dbCollectionMap.put(JOB_COLLECTION, dbAdaptorFactory.getCatalogJobDBAdaptor().getJobCollection());
        this.dbCollectionMap.put(SAMPLE_COLLECTION, dbAdaptorFactory.getCatalogSampleDBAdaptor().getCollection());
        this.dbCollectionMap.put(PANEL_COLLECTION, dbAdaptorFactory.getCatalogPanelDBAdaptor().getCollection());
        this.dbCollectionMap.put(FAMILY_COLLECTION, dbAdaptorFactory.getCatalogFamilyDBAdaptor().getCollection());
        this.dbCollectionMap.put(CLINICAL_ANALYSIS_COLLECTION, dbAdaptorFactory.getClinicalAnalysisDBAdaptor().getClinicalCollection());
    }

    private void initPermissions() {
        this.fullPermissionsMap.put(STUDY_COLLECTION, Arrays.stream(StudyAclEntry.StudyPermissions.values())
                .map(StudyAclEntry.StudyPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(COHORT_COLLECTION, Arrays.stream(CohortAclEntry.CohortPermissions.values())
                .map(CohortAclEntry.CohortPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(DATASET_COLLECTION, Arrays.stream(DatasetAclEntry.DatasetPermissions.values())
                .map(DatasetAclEntry.DatasetPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(FILE_COLLECTION, Arrays.stream(FileAclEntry.FilePermissions.values())
                .map(FileAclEntry.FilePermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(INDIVIDUAL_COLLECTION, Arrays.stream(IndividualAclEntry.IndividualPermissions.values())
                .map(IndividualAclEntry.IndividualPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(JOB_COLLECTION, Arrays.stream(JobAclEntry.JobPermissions.values())
                .map(JobAclEntry.JobPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(SAMPLE_COLLECTION, Arrays.stream(SampleAclEntry.SamplePermissions.values())
                .map(SampleAclEntry.SamplePermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(PANEL_COLLECTION, Arrays.stream(DiseasePanelAclEntry.DiseasePanelPermissions.values())
                .map(DiseasePanelAclEntry.DiseasePanelPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(FAMILY_COLLECTION, Arrays.stream(FamilyAclEntry.FamilyPermissions.values())
                .map(FamilyAclEntry.FamilyPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(CLINICAL_ANALYSIS_COLLECTION,
                Arrays.stream(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.values())
                        .map(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::toString)
                        .collect(Collectors.toList()));

        // Add none as the last permission for all of them
        for (Map.Entry<String, List<String>> stringListEntry : this.fullPermissionsMap.entrySet()) {
            stringListEntry.getValue().add("NONE");
        }
    }

    private void validateCollection(String collection) throws CatalogDBException {
        switch (collection) {
            case STUDY_COLLECTION:
            case COHORT_COLLECTION:
            case INDIVIDUAL_COLLECTION:
            case DATASET_COLLECTION:
            case JOB_COLLECTION:
            case FILE_COLLECTION:
            case SAMPLE_COLLECTION:
            case PANEL_COLLECTION:
            case FAMILY_COLLECTION:
            case CLINICAL_ANALYSIS_COLLECTION:
                return;
            default:
                throw new CatalogDBException("Unexpected parameter received. " + collection + " has been received.");
        }
    }

    /**
     * Internal method to fetch the permissions of every user. Permissions are splitted and returned in a map of user -> list of
     * permissions.
     *
     * @param resourceId Resource id being queried.
     * @param membersList    Members for which we want to fetch the permissions. If empty, it should return the permissions for all members.
     * @param entity     Entity where the query will be performed.
     * @return A map of user -> List of permissions.
     */
    private Map<String, List<String>> internalGet(long resourceId, List<String> membersList, String entity) {

        List<String> members = (membersList == null ? Collections.emptyList() : membersList);

        MongoDBCollection collection = dbCollectionMap.get(entity);

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_ID, resourceId)));
        aggregation.add(Aggregates.project(
                Projections.include(QueryParams.ID.key(), QueryParams.ACL.key())));

        List<Bson> filters = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(members)) {
            List<Pattern> regexMemberList = new ArrayList<>(members.size());
            for (String member : members) {
                if (!member.equals(ANONYMOUS)) {
                    regexMemberList.add(Pattern.compile("^" + member));
                } else {
                    regexMemberList.add(Pattern.compile("^\\*"));
                }
            }
            filters.add(Filters.in(QueryParams.ACL.key(), regexMemberList));
        }

        if (CollectionUtils.isNotEmpty(filters)) {
            Bson filter = filters.size() == 1 ? filters.get(0) : Filters.and(filters);
            aggregation.add(Aggregates.match(filter));
        }

        for (Bson bson : aggregation) {
            logger.debug("Get Acl: {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        }

        QueryResult<Document> aggregate = collection.aggregate(aggregation, null);

        // Code replicated in MongoDBAdaptor
        Map<String, List<String>> permissions = new HashMap<>();
        if (aggregate.getNumResults() > 0) {
            Set<String> memberSet = new HashSet<>();
            memberSet.addAll(members);

            Document document = aggregate.first();
            List<String> memberList = (List<String>) document.get(QueryParams.ACL.key);

            if (memberList != null) {
                // If _acl was not previously defined, it can be null the first time
                for (String memberPermission : memberList) {
                    String[] split = StringUtils.split(memberPermission, INTERNAL_DELIMITER, 2);
//                    String[] split = memberPermission.split(INTERNAL_DELIMITER, 2);
                    if (memberSet.isEmpty() || memberSet.contains(split[0])) {
                        if (!permissions.containsKey(split[0])) {
                            permissions.put(split[0], new ArrayList<>());
                        }
                        if (!("NONE").equals(split[1])) {
                            permissions.get(split[0]).add(split[1]);
                        }
                    }
                }
            }
        }

        return permissions;
    }

    @Override
    public <E extends AbstractAclEntry> QueryResult<E> get(long resourceId, List<String> members, String entity) throws CatalogException {

        validateCollection(entity);
        long startTime = startQuery();

        Map<String, List<String>> myMap = internalGet(resourceId, members, entity);
        List<E> retList;
        switch (entity) {
            case STUDY_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new StudyAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case COHORT_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new CohortAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case INDIVIDUAL_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new IndividualAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case DATASET_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new DatasetAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case JOB_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new JobAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case FILE_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new FileAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case SAMPLE_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new SampleAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case PANEL_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new DiseasePanelAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case FAMILY_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new FamilyAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case CLINICAL_ANALYSIS_COLLECTION:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new ClinicalAnalysisAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            default:
                throw new CatalogException("Unexpected parameter received. " + entity + " has been received.");
        }

        return endQuery(Long.toString(resourceId), startTime, retList);
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> get(List<Long> resourceIds, List<String> members, String entity)
            throws CatalogException {
        List<QueryResult<E>> retList = new ArrayList<>(resourceIds.size());
        for (Long resourceId : resourceIds) {
            retList.add(get(resourceId, members, entity));
        }
        return retList;
    }

    @Override
    public void removeFromStudy(long studyId, String member, String entity) throws CatalogException {
        validateCollection(entity);
        Document query = new Document()
                .append("$isolated", 1)
                .append(PRIVATE_STUDY_ID, studyId);
        List<String> removePermissions = createPermissionArray(Arrays.asList(member), fullPermissionsMap.get(entity));
        Document update = new Document("$pullAll", new Document(QueryParams.ACL.key(), removePermissions));
        logger.debug("Remove all acls for entity {} in study {}. Query: {}, pullAll: {}", entity, studyId,
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        dbCollectionMap.get(entity).update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));

        logger.debug("Remove all the Acls for member {} in study {}", member, studyId);
    }

    @Override
    public void setToMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, String entity)
            throws CatalogDBException {
        validateCollection(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        List<String> permissions = new ArrayList<>(permissionList);
        permissions.add("NONE");

        for (long resourceId : resourceIds) {
            // Get current permissions for resource and override with new ones set for members (already existing or not)
            Map<String, List<String>> currentPermissions = internalGet(resourceId, Collections.emptyList(), entity);
            for (String member : members) {
                currentPermissions.put(member, new ArrayList<>(permissions));
            }

            List<String> permissionArray = createPermissionArray(currentPermissions);
            Document queryDocument = new Document()
                    .append("$isolated", 1)
                    .append(PRIVATE_ID, resourceId);
            Document update = new Document("$set", new Document(QueryParams.ACL.key(), permissionArray));

            logger.debug("Set Acls (set): Query {}, Push {}",
                    queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            collection.update(queryDocument, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }
    }

    @Override
    public void addToMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, String entity)
            throws CatalogDBException {
        validateCollection(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        List<String> permissions = new ArrayList<>(permissionList);
        permissions.add("NONE");

        List<String> myPermissions = createPermissionArray(members, permissions);

        Document queryDocument = new Document()
                .append("$isolated", 1)
                .append(PRIVATE_ID, new Document("$in", resourceIds));
        Document update = new Document("$addToSet", new Document(QueryParams.ACL.key(), new Document("$each", myPermissions)));
        logger.debug("Add Acls (addToSet): Query {}, Push {}",
                queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        collection.update(queryDocument, update, new QueryOptions("multi", true));
    }

    @Override
    public void removeFromMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, String entity)
            throws CatalogDBException {

        if (members == null || members.isEmpty()) {
            return;
        }

        validateCollection(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        List<String> permissions = permissionList;

        if (permissions == null || permissions.isEmpty()) {
            // We get all possible permissions those members will have to do a full reset
            permissions = fullPermissionsMap.get(entity);
        }

        List<String> removePermissions = createPermissionArray(members, permissions);
        Document queryDocument = new Document()
                .append("$isolated", 1)
                .append(PRIVATE_ID, new Document("$in", resourceIds));
        Document update = new Document("$pullAll", new Document(QueryParams.ACL.key(), removePermissions));
        logger.debug("Remove Acls (pullAll): Query {}, Pull {}",
                queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        collection.update(queryDocument, update, new QueryOptions("multi", true));
    }

    @Override
    public void resetMembersFromAllEntries(long studyId, List<String> members) throws CatalogDBException {
        if (members == null || members.isEmpty()) {
            return;
        }

        removePermissions(studyId, members, COHORT_COLLECTION);
        removePermissions(studyId, members, DATASET_COLLECTION);
        removePermissions(studyId, members, FILE_COLLECTION);
        removePermissions(studyId, members, INDIVIDUAL_COLLECTION);
        removePermissions(studyId, members, JOB_COLLECTION);
        removePermissions(studyId, members, SAMPLE_COLLECTION);
        removePermissions(studyId, members, PANEL_COLLECTION);
        removePermissions(studyId, members, FAMILY_COLLECTION);
        removePermissions(studyId, members, CLINICAL_ANALYSIS_COLLECTION);
        removeFromMembers(Arrays.asList(studyId), members, null, STUDY_COLLECTION);

    }

    @Override
    public <E extends AbstractAclEntry> void setAcls(List<Long> resourceIds, List<E> acls, String entity) throws CatalogDBException {
        validateCollection(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        for (long resourceId : resourceIds) {
            // Get current permissions for resource and override with new ones set for members (already existing or not)
            Map<String, List<String>> currentPermissions = internalGet(resourceId, Collections.emptyList(), entity);
            for (E acl : acls) {
                List<String> permissions = (List<String>) acl.getPermissions().stream().map(a -> a.toString()).collect(Collectors.toList());
                // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
                permissions = new ArrayList<>(permissions);
                permissions.add("NONE");
                currentPermissions.put(acl.getMember(), permissions);
            }

            List<String> permissionArray = createPermissionArray(currentPermissions);
            Document queryDocument = new Document()
                    .append("$isolated", 1)
                    .append(PRIVATE_ID, resourceId);
            Document update = new Document("$set", new Document(QueryParams.ACL.key(), permissionArray));

            logger.debug("Set Acls (set): Query {}, Push {}",
                    queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            collection.update(queryDocument, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }
    }

    @Override
    public void applyPermissionRules(long studyId, PermissionRules permissionRule, String entity) throws CatalogException {
        MongoDBCollection collection = dbCollectionMap.get(entity);

        // We will apply the permission rules to all the entries matching the query defined in the permission rules that does not have
        // the permission rules applied yet
        Document rawQuery = new Document()
                .append(PRIVATE_STUDY_ID, studyId)
                .append(PERMISSION_RULES_APPLIED, new Document("$ne", permissionRule.getId()));
        Bson bson = parseQuery(permissionRule.getQuery(), rawQuery, entity);

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        List<String> permissions = new ArrayList<>(permissionRule.getPermissions());
        permissions.add("NONE");
        List<String> myPermissions = createPermissionArray(permissionRule.getMembers(), permissions);

        Document update = new Document()
                .append("$addToSet", new Document()
                        .append(QueryParams.ACL.key(), new Document("$each", myPermissions))
                        .append(PERMISSION_RULES_APPLIED, permissionRule.getId()));

        logger.debug("Apply permission rules: Query {}, Update {}",
                bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        collection.update(bson, update, new QueryOptions("multi", true));
    }

    private Bson parseQuery(Query query, Document rawQuery, String entity) throws CatalogException {
        switch (entity) {
            case COHORT_COLLECTION:
                return dbAdaptorFactory.getCatalogCohortDBAdaptor().parseQuery(query, true, rawQuery);
            case INDIVIDUAL_COLLECTION:
                return dbAdaptorFactory.getCatalogIndividualDBAdaptor().parseQuery(query, true, rawQuery);
            case JOB_COLLECTION:
                return dbAdaptorFactory.getCatalogJobDBAdaptor().parseQuery(query, true, rawQuery);
            case FILE_COLLECTION:
                return dbAdaptorFactory.getCatalogFileDBAdaptor().parseQuery(query, true, rawQuery);
            case SAMPLE_COLLECTION:
                return dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, true, rawQuery);
            case FAMILY_COLLECTION:
                return dbAdaptorFactory.getCatalogFamilyDBAdaptor().parseQuery(query, true, rawQuery);
            case CLINICAL_ANALYSIS_COLLECTION:
                return dbAdaptorFactory.getClinicalAnalysisDBAdaptor().parseQuery(query, true, rawQuery);
            default:
                throw new CatalogException("Unexpected parameter received. " + entity + " has been received.");
        }
    }

    private void removePermissions(long studyId, List<String> users, String entity) {
        List<String> permissions = fullPermissionsMap.get(entity);
        List<String> removePermissions = createPermissionArray(users, permissions);

        MongoDBCollection collection = dbCollectionMap.get(entity);
        Document queryDocument = new Document()
                .append("$isolated", 1)
                .append(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.ACL.key(), new Document("$in", removePermissions));
        Document update = new Document("$pullAll", new Document(QueryParams.ACL.key(), removePermissions));

        collection.update(queryDocument, update, new QueryOptions("multi", true));
    }

    private List<String> createPermissionArray(Map<String, List<String>> memberPermissionsMap) {
        List<String> myPermissions = new ArrayList<>(memberPermissionsMap.size() * 2);
        for (Map.Entry<String, List<String>> stringListEntry : memberPermissionsMap.entrySet()) {
            if (stringListEntry.getValue().isEmpty()) {
                stringListEntry.getValue().add("NONE");
            }

            for (String permission : stringListEntry.getValue()) {
                myPermissions.add(stringListEntry.getKey() + INTERNAL_DELIMITER + permission);
            }
        }

        return myPermissions;
    }

    private List<String> createPermissionArray(List<String> members, List<String> permissions) {
        List<String> writtenPermissions;
        if (permissions.isEmpty()) {
            writtenPermissions = Arrays.asList("NONE");
        } else {
            writtenPermissions = permissions;
        }

        List<String> myPermissions = new ArrayList<>(members.size() * writtenPermissions.size());
        for (String member : members) {
            for (String writtenPermission : writtenPermissions) {
                myPermissions.add(member + INTERNAL_DELIMITER + writtenPermission);
            }
        }
        return myPermissions;
    }
}
