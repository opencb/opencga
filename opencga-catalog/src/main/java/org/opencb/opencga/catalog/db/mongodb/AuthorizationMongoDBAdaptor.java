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
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationDBAdaptor;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.models.PermissionRule;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.*;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryParam.Type.INTEGER_ARRAY;
import static org.opencb.commons.datastore.core.QueryParam.Type.TEXT_ARRAY;

/**
 * Created by pfurio on 20/04/17.
 */
public class AuthorizationMongoDBAdaptor extends MongoDBAdaptor implements AuthorizationDBAdaptor {

    private Map<Entity, MongoDBCollection> dbCollectionMap = new HashMap<>();
    private Map<Entity, List<String>> fullPermissionsMap = new HashMap<>();

    private static final String ANONYMOUS = "*";
    static final String MEMBER_WITH_INTERNAL_ACL = "_withInternalAcls";

    public AuthorizationMongoDBAdaptor(DBAdaptorFactory dbFactory) throws CatalogDBException {
        super(LoggerFactory.getLogger(AuthorizationMongoDBAdaptor.class));
        this.dbAdaptorFactory = (MongoDBAdaptorFactory) dbFactory;
        initCollectionConnections();
        initPermissions();
    }

    enum QueryParams implements QueryParam {
        ID("id", INTEGER_ARRAY, ""),
        ACL("_acl", TEXT_ARRAY, ""),
        USER_DEFINED_ACLS("_userAcls", TEXT_ARRAY, "");

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
        this.dbCollectionMap.put(Entity.STUDY, dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection());
        this.dbCollectionMap.put(Entity.COHORT, dbAdaptorFactory.getCatalogCohortDBAdaptor().getCohortCollection());
        this.dbCollectionMap.put(Entity.DATASET, dbAdaptorFactory.getCatalogDatasetDBAdaptor().getDatasetCollection());
        this.dbCollectionMap.put(Entity.FILE, dbAdaptorFactory.getCatalogFileDBAdaptor().getCollection());
        this.dbCollectionMap.put(Entity.INDIVIDUAL, dbAdaptorFactory.getCatalogIndividualDBAdaptor().getCollection());
        this.dbCollectionMap.put(Entity.JOB, dbAdaptorFactory.getCatalogJobDBAdaptor().getJobCollection());
        this.dbCollectionMap.put(Entity.SAMPLE, dbAdaptorFactory.getCatalogSampleDBAdaptor().getCollection());
        this.dbCollectionMap.put(Entity.PANEL, dbAdaptorFactory.getCatalogPanelDBAdaptor().getPanelCollection());
        this.dbCollectionMap.put(Entity.FAMILY, dbAdaptorFactory.getCatalogFamilyDBAdaptor().getCollection());
        this.dbCollectionMap.put(Entity.CLINICAL_ANALYSIS, dbAdaptorFactory.getClinicalAnalysisDBAdaptor().getClinicalCollection());
    }

    private void initPermissions() {
        this.fullPermissionsMap.put(Entity.STUDY, Arrays.stream(StudyAclEntry.StudyPermissions.values())
                .map(StudyAclEntry.StudyPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.COHORT, Arrays.stream(CohortAclEntry.CohortPermissions.values())
                .map(CohortAclEntry.CohortPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.DATASET, Arrays.stream(DatasetAclEntry.DatasetPermissions.values())
                .map(DatasetAclEntry.DatasetPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.FILE, Arrays.stream(FileAclEntry.FilePermissions.values())
                .map(FileAclEntry.FilePermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.INDIVIDUAL, Arrays.stream(IndividualAclEntry.IndividualPermissions.values())
                .map(IndividualAclEntry.IndividualPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.JOB, Arrays.stream(JobAclEntry.JobPermissions.values())
                .map(JobAclEntry.JobPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.SAMPLE, Arrays.stream(SampleAclEntry.SamplePermissions.values())
                .map(SampleAclEntry.SamplePermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.PANEL, Arrays.stream(PanelAclEntry.PanelPermissions.values())
                .map(PanelAclEntry.PanelPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.FAMILY, Arrays.stream(FamilyAclEntry.FamilyPermissions.values())
                .map(FamilyAclEntry.FamilyPermissions::toString)
                .collect(Collectors.toList()));
        this.fullPermissionsMap.put(Entity.CLINICAL_ANALYSIS,
                Arrays.stream(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions.values())
                        .map(ClinicalAnalysisAclEntry.ClinicalAnalysisPermissions::toString)
                        .collect(Collectors.toList()));

        // Add none as the last permission for all of them
        for (Map.Entry<Entity, List<String>> stringListEntry : this.fullPermissionsMap.entrySet()) {
            stringListEntry.getValue().add("NONE");
        }
    }

    private void validateEntry(Entity entry) throws CatalogDBException {
        switch (entry) {
            case STUDY:
            case COHORT:
            case INDIVIDUAL:
            case DATASET:
            case JOB:
            case FILE:
            case SAMPLE:
            case PANEL:
            case FAMILY:
            case CLINICAL_ANALYSIS:
                return;
            default:
                throw new CatalogDBException("Unexpected parameter received. " + entry + " has been received.");
        }
    }

    /**
     * Internal method to fetch the permissions of every user. Permissions are splitted and returned in a map of user -> list of
     * permissions.
     *
     * @param resourceId Resource id being queried.
     * @param membersList Members for which we want to fetch the permissions. If empty, it should return the permissions for all members.
     * @param entry     Entity where the query will be performed.
     * @return A map of [acl, user_defined_acl] -> user -> List of permissions and the string id of the resource queried.
     */
    private EntryPermission internalGet(long resourceId, List<String> membersList, Entity entry) {
        EntryPermission entryPermission = new EntryPermission();

        List<String> members = (membersList == null ? Collections.emptyList() : membersList);

        MongoDBCollection collection = dbCollectionMap.get(entry);

        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.eq(PRIVATE_UID, resourceId)));
        aggregation.add(Aggregates.project(
                Projections.include(QueryParams.ID.key(), QueryParams.ACL.key(), QueryParams.USER_DEFINED_ACLS.key())));

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

        Map<String, Map<String, List<String>>> permissions = entryPermission.getPermissions();

        if (aggregate.getNumResults() > 0) {
            Set<String> memberSet = new HashSet<>();
            memberSet.addAll(members);

            Document document = aggregate.first();
            entryPermission.setId(document.getString(QueryParams.ID.key()));

            List<String> aclList = (List<String>) document.get(QueryParams.ACL.key());
            if (aclList != null) {
                // If _acl was not previously defined, it can be null the first time
                for (String memberPermission : aclList) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(memberPermission, INTERNAL_DELIMITER, 2);
//                    String[] split = memberPermission.split(INTERNAL_DELIMITER, 2);
                    if (memberSet.isEmpty() || memberSet.contains(split[0])) {
                        if (!permissions.get(QueryParams.ACL.key()).containsKey(split[0])) {
                            permissions.get(QueryParams.ACL.key()).put(split[0], new ArrayList<>());
                        }
                        if (!("NONE").equals(split[1])) {
                            permissions.get(QueryParams.ACL.key()).get(split[0]).add(split[1]);
                        }
                    }
                }
            }

            List<String> userDefinedAcls = (List<String>) document.get(QueryParams.USER_DEFINED_ACLS.key());
            if (userDefinedAcls != null) {
                // If _acl was not previously defined, it can be null the first time
                for (String memberPermission : userDefinedAcls) {
                    String[] split = StringUtils.splitByWholeSeparatorPreserveAllTokens(memberPermission, INTERNAL_DELIMITER, 2);
//                    String[] split = memberPermission.split(INTERNAL_DELIMITER, 2);
                    if (memberSet.isEmpty() || memberSet.contains(split[0])) {
                        if (!permissions.get(QueryParams.USER_DEFINED_ACLS.key()).containsKey(split[0])) {
                            permissions.get(QueryParams.USER_DEFINED_ACLS.key()).put(split[0], new ArrayList<>());
                        }
                        if (!("NONE").equals(split[1])) {
                            permissions.get(QueryParams.USER_DEFINED_ACLS.key()).get(split[0]).add(split[1]);
                        }
                    }
                }
            }
        }

        return entryPermission;
    }

    class EntryPermission {
        /**
         * Entry id.
         */
        private String id;

        /**
         * A map of [acl, user_defined_acl] -> user -> List of permissions.
         */
        private Map<String, Map<String, List<String>>> permissions;

        EntryPermission() {
            this.permissions = new HashMap<>();
            this.permissions.put(QueryParams.ACL.key(), new HashMap<>());
            this.permissions.put(QueryParams.USER_DEFINED_ACLS.key(), new HashMap<>());
        }

        private String getId() {
            return id;
        }

        private EntryPermission setId(String id) {
            this.id = id;
            return this;
        }

        private Map<String, Map<String, List<String>>> getPermissions() {
            return permissions;
        }

        private EntryPermission setPermissions(Map<String, Map<String, List<String>>> permissions) {
            this.permissions = permissions;
            return this;
        }
    }

    @Override
    public <E extends AbstractAclEntry> QueryResult<E> get(long resourceId, List<String> members, Entity entry) throws CatalogException {

        validateEntry(entry);
        long startTime = startQuery();

        EntryPermission entryPermission = internalGet(resourceId, members, entry);

        Map<String, List<String>> myMap = entryPermission.getPermissions().get(QueryParams.ACL.key());
        List<E> retList;
        switch (entry) {
            case STUDY:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new StudyAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case COHORT:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new CohortAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case INDIVIDUAL:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new IndividualAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case DATASET:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new DatasetAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case JOB:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new JobAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case FILE:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new FileAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case SAMPLE:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new SampleAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case PANEL:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new PanelAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case FAMILY:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new FamilyAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            case CLINICAL_ANALYSIS:
                retList = new ArrayList<>(myMap.size());
                for (Map.Entry<String, List<String>> stringListEntry : myMap.entrySet()) {
                    retList.add((E) new ClinicalAnalysisAclEntry(stringListEntry.getKey(), stringListEntry.getValue()));
                }
                break;
            default:
                throw new CatalogException("Unexpected parameter received. " + entry + " has been received.");
        }

        return endQuery(entryPermission.getId(), startTime, retList);
    }

    @Override
    public <E extends AbstractAclEntry> List<QueryResult<E>> get(List<Long> resourceIds, List<String> members, Entity entry)
            throws CatalogException {
        List<QueryResult<E>> retList = new ArrayList<>(resourceIds.size());
        for (Long resourceId : resourceIds) {
            retList.add(get(resourceId, members, entry));
        }
        return retList;
    }

    @Override
    public void removeFromStudy(long studyId, String member, Entity entry) throws CatalogException {
        validateEntry(entry);
        Document query = new Document()
                .append(PRIVATE_STUDY_ID, studyId);
        List<String> removePermissions = createPermissionArray(Arrays.asList(member), fullPermissionsMap.get(entry));
        Document update = new Document("$pullAll", new Document()
                .append(QueryParams.ACL.key(), removePermissions)
                .append(QueryParams.USER_DEFINED_ACLS.key(), removePermissions)
        );
        logger.debug("Remove all acls for entity {} in study {}. Query: {}, pullAll: {}", entry, studyId,
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        dbCollectionMap.get(entry).update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));

        logger.debug("Remove all the Acls for member {} in study {}", member, studyId);
    }

    @Override
    public void setToMembers(long studyId, List<Long> resourceIds, List<Long> resourceIds2, List<String> members,
                             List<String> permissionList, Entity entity, Entity entity2) throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
            long startTime = startQuery();

            try {
                // We obtain which of those members are actually users to add them to the @members group automatically
                addToMembersGroupInStudy(studyId, members, clientSession);

                setToMembers(resourceIds, members, permissionList, entity, clientSession);

                if (ListUtils.isNotEmpty(resourceIds2) && entity2 != null) {
                    setToMembers(resourceIds2, members, permissionList, entity2, clientSession);
                }

                // We store that those members have internal permissions
                setMembersHaveInternalPermissionsDefined(studyId, members, permissionList, entity.name(), clientSession);

            } catch (CatalogDBException e) {
                logger.error("{}", e.getMessage(), e);
                return endWrite("", startTime, -1, 0, Collections.singletonList(new WriteResult.Fail("", e.getMessage())));
            }

            return endWrite("", startTime, 1, 1, null);
        };
        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
    }

    @Override
    public void setToMembers(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
            long startTime = startQuery();

            try {
                for (Long studyId : studyIds) {
                    addToMembersGroupInStudy(studyId, members, clientSession);
                }

                setToMembers(studyIds, members, permissions, Entity.STUDY, clientSession);

            } catch (CatalogDBException e) {
                logger.error("{}", e.getMessage(), e);
                return endWrite("", startTime, -1, 0, Collections.singletonList(new WriteResult.Fail("", e.getMessage())));
            }

            return endWrite("", startTime, 1, 1, null);
        };
        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
    }

    private void setToMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, Entity entity,
                              ClientSession clientSession) throws CatalogDBException {
        validateEntry(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        /* 1. We are going to try to remove all the permissions to those members in first instance */

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        List<String> permissions = fullPermissionsMap.get(entity);
        permissions.add("NONE");
        permissions = createPermissionArray(members, permissions);

        Document queryDocument = new Document()
                .append(PRIVATE_UID, new Document("$in", resourceIds));
        Document update = new Document(QueryParams.ACL.key(), permissions);
        if (isPermissionRuleEntity(entity)) {
            update.put(QueryParams.USER_DEFINED_ACLS.key(), permissions);
        }
        update = new Document("$pullAll", update);
        logger.debug("Pull all acls: Query {}, PullAll {}, entity: {}",
                queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), entity);
        collection.update(clientSession, queryDocument, update, new QueryOptions("multi", true));

        /* 2. We now add the expected permissions to those members */

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        permissions = new ArrayList<>(permissionList);
        permissions.add("NONE");
        permissions = createPermissionArray(members, permissions);

        update = new Document(QueryParams.ACL.key(), new Document("$each", permissions));
        if (isPermissionRuleEntity(entity)) {
            update.put(QueryParams.USER_DEFINED_ACLS.key(), new Document("$each", permissions));
        }

        update = new Document("$addToSet", update);
        logger.debug("Add Acls (addToSet): Query {}, Push {}, entity: {}",
                queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()), entity);

        collection.update(clientSession, queryDocument, update, new QueryOptions("multi", true));
    }

    @Override
    public void addToMembers(long studyId, List<Long> resourceIds, List<Long> resourceIds2, List<String> members,
                             List<String> permissionList, Entity entity, Entity entity2) throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
            long startTime = startQuery();

            try {
                addToMembersGroupInStudy(studyId, members, clientSession);

                addToMembers(resourceIds, members, permissionList, entity, clientSession);

                if (ListUtils.isNotEmpty(resourceIds2) && entity2 != null) {
                    addToMembers(resourceIds2, members, permissionList, entity2, clientSession);
                }

                // We store that those members have internal permissions
                setMembersHaveInternalPermissionsDefined(studyId, members, permissionList, entity.name(), clientSession);
            } catch (CatalogDBException e) {
                logger.error("{}", e.getMessage(), e);
                return endWrite("", startTime, -1, 0, Collections.singletonList(new WriteResult.Fail("", e.getMessage())));
            }

            return endWrite("", startTime, 1, 1, null);
        };
        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
    }

    private void addToMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, Entity entity,
                              ClientSession clientSession) throws CatalogDBException {
        validateEntry(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        List<String> permissions = new ArrayList<>(permissionList);
        permissions.add("NONE");

        List<String> myPermissions = createPermissionArray(members, permissions);

        Document queryDocument = new Document()
                .append(PRIVATE_UID, new Document("$in", resourceIds));
        Document update;
        if (isPermissionRuleEntity(entity)) {
            update = new Document("$addToSet", new Document()
                    .append(QueryParams.ACL.key(), new Document("$each", myPermissions))
                    .append(QueryParams.USER_DEFINED_ACLS.key(), new Document("$each", myPermissions))
            );
        } else {
            update = new Document("$addToSet", new Document(QueryParams.ACL.key(), new Document("$each", myPermissions)));
        }

        logger.debug("Add Acls (addToSet): Query {}, Push {}",
                queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        collection.update(clientSession, queryDocument, update, new QueryOptions("multi", true));
    }

    @Override
    public void addToMembers(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogDBException {
        ClientSession clientSession = getClientSession();
        TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
            long startTime = startQuery();

            try {
                for (Long studyId : studyIds) {
                    addToMembersGroupInStudy(studyId, members, clientSession);
                }

                addToMembers(studyIds, members, permissions, Entity.STUDY, clientSession);

            } catch (CatalogDBException e) {
                logger.error("{}", e.getMessage(), e);
                return endWrite("", startTime, -1, 0, Collections.singletonList(new WriteResult.Fail("", e.getMessage())));
            }

            return endWrite("", startTime, 1, 1, null);
        };
        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
    }

    void addToMembersGroupInStudy(long studyId, List<String> members, ClientSession clientSession) throws CatalogDBException {
        // We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = members.stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(userList)) {
            // We first add the member to the @members group in case they didn't belong already
            dbAdaptorFactory.getCatalogStudyDBAdaptor().addUsersToGroup(studyId, CatalogAuthorizationManager.MEMBERS_GROUP,
                    userList, clientSession);
        }
    }

    @Override
    public void removeFromMembers(List<Long> resourceIds, List<Long> resourceIds2, List<String> members, List<String> permissionList,
                                  Entity entity, Entity entity2) throws CatalogDBException {
        if (members == null || members.isEmpty()) {
            throw new CatalogDBException("Missing members list");
        }

        ClientSession clientSession = getClientSession();
        TransactionBody txnBody = (TransactionBody<WriteResult>) () -> {
            long startTime = startQuery();

            try {
                removeFromMembers(resourceIds, members, permissionList, entity, clientSession);

                if (ListUtils.isNotEmpty(resourceIds2) && entity2 != null) {
                    removeFromMembers(resourceIds2, members, permissionList, entity2, clientSession);
                }
            } catch (CatalogDBException e) {
                logger.error("{}", e.getMessage(), e);
                return endWrite("", startTime, -1, 0, Collections.singletonList(new WriteResult.Fail("", e.getMessage())));
            }

            return endWrite("", startTime, 1, 1, null);
        };
        WriteResult result = commitTransaction(clientSession, txnBody);

        if (result.getNumModified() == 0) {
            throw new CatalogDBException(result.getFailed().get(0).getMessage());
        }
    }

    private void removeFromMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, Entity entity,
                                   ClientSession clientSession) throws CatalogDBException {
        validateEntry(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        List<String> permissions = permissionList;

        if (permissions == null || permissions.isEmpty()) {
            // We get all possible permissions those members will have to do a full reset
            permissions = fullPermissionsMap.get(entity);
        }

        List<String> removePermissions = createPermissionArray(members, permissions);
        Document queryDocument = new Document()
                .append(PRIVATE_UID, new Document("$in", resourceIds));
        Document update;
        if (isPermissionRuleEntity(entity)) {
            update = new Document("$pullAll", new Document()
                    .append(QueryParams.ACL.key(), removePermissions)
                    .append(QueryParams.USER_DEFINED_ACLS.key(), removePermissions)
            );
        } else {
            update = new Document("$pullAll", new Document(QueryParams.ACL.key(), removePermissions));
        }

        logger.debug("Remove Acls (pullAll): Query {}, Pull {}",
                queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        collection.update(clientSession, queryDocument, update, new QueryOptions("multi", true));
    }

    @Override
    public void resetMembersFromAllEntries(long studyId, List<String> members) throws CatalogDBException {
        if (members == null || members.isEmpty()) {
            return;
        }

        removePermissions(studyId, members, Entity.COHORT);
        removePermissions(studyId, members, Entity.DATASET);
        removePermissions(studyId, members, Entity.FILE);
        removePermissions(studyId, members, Entity.INDIVIDUAL);
        removePermissions(studyId, members, Entity.JOB);
        removePermissions(studyId, members, Entity.SAMPLE);
        removePermissions(studyId, members, Entity.PANEL);
        removePermissions(studyId, members, Entity.FAMILY);
        removePermissions(studyId, members, Entity.CLINICAL_ANALYSIS);
        removeFromMembers(Arrays.asList(studyId), members, null, Entity.STUDY);
    }

    @Override
    public <E extends AbstractAclEntry> void setAcls(List<Long> resourceIds, List<E> acls, Entity entity) throws CatalogDBException {
        validateEntry(entity);
        MongoDBCollection collection = dbCollectionMap.get(entity);

        for (long resourceId : resourceIds) {
            // Get current permissions for resource and override with new ones set for members (already existing or not)
            Map<String, Map<String, List<String>>> currentPermissions = internalGet(resourceId, Collections.emptyList(), entity)
                    .getPermissions();
            for (E acl : acls) {
                List<String> permissions = (List<String>) acl.getPermissions().stream().map(a -> a.toString()).collect(Collectors.toList());
                // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
                permissions = new ArrayList<>(permissions);
                permissions.add("NONE");
                currentPermissions.get(QueryParams.ACL.key()).put(acl.getMember(), permissions);
                currentPermissions.get(QueryParams.USER_DEFINED_ACLS.key()).put(acl.getMember(), permissions);
            }
            List<String> permissionArray = createPermissionArray(currentPermissions.get(QueryParams.ACL.key()));
            List<String> manualPermissionArray = createPermissionArray(currentPermissions.get(QueryParams.USER_DEFINED_ACLS.key()));

            Document queryDocument = new Document()
                    .append(PRIVATE_UID, resourceId);
            Document update;
            if (isPermissionRuleEntity(entity)) {
                update = new Document("$set", new Document()
                        .append(QueryParams.ACL.key(), permissionArray)
                        .append(QueryParams.USER_DEFINED_ACLS.key(), manualPermissionArray));
            } else {
                update = new Document("$set", new Document(QueryParams.ACL.key(), permissionArray));
            }

            logger.debug("Set Acls (set): Query {}, Push {}",
                    queryDocument.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

            collection.update(queryDocument, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }
    }

    @Override
    public void setMembersHaveInternalPermissionsDefined(long studyId, List<String> members, List<String> permissions, String entity) {
        // We only store if a member has internal permissions defined if it hasn't been given VIEW permission
        if (permissions.contains("VIEW")) {
            return;
        }

        Document queryDocument = new Document()
                .append(PRIVATE_UID, studyId);

        Document addToSet = new Document();
        for (String member : members) {
            addToSet.append(MEMBER_WITH_INTERNAL_ACL + "." + member, entity);
        }
        Document update = new Document("$addToSet", addToSet);

        MongoDBCollection collection = dbCollectionMap.get(Entity.STUDY);
        collection.update(queryDocument, update, new QueryOptions());
    }

    private void setMembersHaveInternalPermissionsDefined(long studyId, List<String> members, List<String> permissions, String entity,
                                                          ClientSession clientSession) {
        // We only store if a member has internal permissions defined if it hasn't been given VIEW permission
        if (permissions.contains("VIEW")) {
            return;
        }

        Document queryDocument = new Document()
                .append(PRIVATE_UID, studyId);

        Document addToSet = new Document();
        for (String member : members) {
            addToSet.append(MEMBER_WITH_INTERNAL_ACL + "." + member, entity);
        }
        Document update = new Document("$addToSet", addToSet);

        MongoDBCollection collection = dbCollectionMap.get(Entity.STUDY);
        collection.update(clientSession, queryDocument, update, new QueryOptions());
    }

    @Override
    public void applyPermissionRules(long studyId, PermissionRule permissionRule, Study.Entity entry) throws CatalogException {
        MongoDBCollection collection = dbCollectionMap.get(entry.getEntity());

        // We will apply the permission rules to all the entries matching the query defined in the permission rules that does not have
        // the permission rules applied yet
        Document rawQuery = new Document()
                .append(PRIVATE_STUDY_ID, studyId)
                .append(PERMISSION_RULES_APPLIED, new Document("$ne", permissionRule.getId()));
        Bson bson = parseQuery(permissionRule.getQuery(), rawQuery, entry.getEntity());

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

    @Override
    public void removePermissionRuleAndRemovePermissions(Study study, String permissionRuleToDeleteId, Study.Entity entry)
            throws CatalogException {
        // Prepare the permission rule list into a map of permissionRuleId - PermissionRule to make much easier the process
        Map<String, PermissionRule> permissionRuleMap = study.getPermissionRules().get(entry).stream()
                .collect(Collectors.toMap(PermissionRule::getId, p -> p));
        PermissionRule permissionRuleToDelete = permissionRuleMap.get(permissionRuleToDeleteId);

        Set<String> permissionsToRemove =
                createPermissionArray(permissionRuleToDelete.getMembers(), permissionRuleToDelete.getPermissions())
                        .stream().collect(Collectors.toSet());

        MongoDBCollection collection = dbCollectionMap.get(entry.getEntity());

        // Remove the __TODELETE tag...
        String permissionRuleId = permissionRuleToDeleteId.split(INTERNAL_DELIMITER)[0];

        // 1. Get all the entries that have the permission rule to be removed applied
        Document query = new Document()
                .append(PRIVATE_STUDY_ID, study.getUid())
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ACL.key(), QueryParams.USER_DEFINED_ACLS.key(), PERMISSION_RULES_APPLIED, PRIVATE_UID));
        MongoCursor<Document> iterator = collection.nativeQuery().find(query, options).iterator();
        while (iterator.hasNext()) {
            Document myDocument = iterator.next();
            Set<String> effectivePermissions = new HashSet<>();
            Set<String> manualPermissions = new HashSet<>();
            Set<String> permissionRulesApplied = new HashSet<>();

            List<String> currentAclList = (List) myDocument.get(QueryParams.ACL.key());
            List<String> currentManualAclList = (List) myDocument.get(QueryParams.USER_DEFINED_ACLS.key());
            List<String> currentPermissionRulesApplied = (List) myDocument.get(PERMISSION_RULES_APPLIED);

            // TODO: Control that if there are no more permissions set for a user or group, we should also remove the NONE permission
            // Remove permissions from the permission rule
            for (String permission : currentAclList) {
                if (!permissionsToRemove.contains(permission)) {
                    effectivePermissions.add(permission);
                }
            }

            // Remove permissions from the permission rule from the internal manual permissions list
            if (currentManualAclList != null) {
                for (String permission : currentManualAclList) {
                    if (!permissionsToRemove.contains(permission)) {
                        manualPermissions.add(permission);
                    }
                }
            }

            for (String tmpPermissionRuleId : currentPermissionRulesApplied) {
                // We apply the rest of permission rules except the one to be deleted
                if (!tmpPermissionRuleId.equals(permissionRuleId)) {
                    PermissionRule tmpPermissionRule = permissionRuleMap.get(tmpPermissionRuleId);
                    List<String> tmpPermissionList = new ArrayList<>(tmpPermissionRule.getPermissions());
                    tmpPermissionList.add("NONE");
                    List<String> permissionArray = createPermissionArray(tmpPermissionRule.getMembers(), tmpPermissionList);

                    effectivePermissions.addAll(permissionArray);
                    permissionRulesApplied.add(tmpPermissionRuleId);
                }
            }

            Document tmpQuery = new Document()
                    .append(PRIVATE_UID, myDocument.get(PRIVATE_UID))
                    .append(PRIVATE_STUDY_ID, study.getUid());

            Document update = new Document("$set", new Document()
                    .append(QueryParams.ACL.key(), effectivePermissions)
                    .append(QueryParams.USER_DEFINED_ACLS.key(), manualPermissions)
                    .append(PERMISSION_RULES_APPLIED, permissionRulesApplied));

            logger.debug("Remove permission rule id and permissions from {}: Query {}, Update {}", entry,
                    tmpQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            QueryResult<UpdateResult> updateResult = collection.update(tmpQuery, update, new QueryOptions("multi", true));
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogException("Could not update and remove permission rule from entry " + myDocument.get(PRIVATE_UID));
            }
        }

        // 2. Remove the permission rule from the map in the study
        removeReferenceToPermissionRuleInStudy(study.getUid(), permissionRuleToDeleteId, entry);
    }

    @Override
    public void removePermissionRuleAndRestorePermissions(Study study, String permissionRuleToDeleteId, Study.Entity entry)
            throws CatalogException {
        // Prepare the permission rule list into a map of permissionRuleId - PermissionRule to make much easier the process
        Map<String, PermissionRule> permissionRuleMap = study.getPermissionRules().get(entry).stream()
                .collect(Collectors.toMap(PermissionRule::getId, p -> p));
        PermissionRule permissionRuleToDelete = permissionRuleMap.get(permissionRuleToDeleteId);

        Set<String> permissionsToRemove =
                createPermissionArray(permissionRuleToDelete.getMembers(), permissionRuleToDelete.getPermissions())
                        .stream().collect(Collectors.toSet());

        MongoDBCollection collection = dbCollectionMap.get(entry.getEntity());

        // Remove the __TODELETE tag...
        String permissionRuleId = permissionRuleToDeleteId.split(INTERNAL_DELIMITER)[0];

        // 1. Get all the entries that have the permission rule to be removed applied
        Document query = new Document()
                .append(PRIVATE_STUDY_ID, study.getUid())
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ACL.key(), QueryParams.USER_DEFINED_ACLS.key(), PERMISSION_RULES_APPLIED, PRIVATE_UID));
        MongoCursor<Document> iterator = collection.nativeQuery().find(query, options).iterator();
        while (iterator.hasNext()) {
            Document myDocument = iterator.next();
            Set<String> effectivePermissions = new HashSet<>();
            Set<String> permissionRulesApplied = new HashSet<>();

            List<String> currentAclList = (List) myDocument.get(QueryParams.ACL.key());
            List<String> currentManualAclList = (List) myDocument.get(QueryParams.USER_DEFINED_ACLS.key());
            List<String> currentPermissionRulesApplied = (List) myDocument.get(PERMISSION_RULES_APPLIED);

            // TODO: Control that if there are no more permissions set for a user or group, we should also remove the NONE permission
            // Remove permissions from the permission rule
            for (String permission : currentAclList) {
                if (!permissionsToRemove.contains(permission)) {
                    effectivePermissions.add(permission);
                }
            }

            // Restore manual permissions
            if (currentManualAclList != null) {
                for (String permission : currentManualAclList) {
                    effectivePermissions.add(permission);
                }
            }

            for (String tmpPermissionRuleId : currentPermissionRulesApplied) {
                // We apply the rest of permission rules except the one to be deleted
                if (!tmpPermissionRuleId.equals(permissionRuleId)) {
                    PermissionRule tmpPermissionRule = permissionRuleMap.get(tmpPermissionRuleId);
                    List<String> tmpPermissionList = new ArrayList<>(tmpPermissionRule.getPermissions());
                    tmpPermissionList.add("NONE");
                    List<String> permissionArray = createPermissionArray(tmpPermissionRule.getMembers(), tmpPermissionList);

                    effectivePermissions.addAll(permissionArray);
                    permissionRulesApplied.add(tmpPermissionRuleId);
                }
            }

            Document tmpQuery = new Document()
                    .append(PRIVATE_UID, myDocument.get(PRIVATE_UID))
                    .append(PRIVATE_STUDY_ID, study.getUid());

            Document update = new Document("$set", new Document()
                    .append(QueryParams.ACL.key(), effectivePermissions)
                    .append(PERMISSION_RULES_APPLIED, permissionRulesApplied));

            logger.debug("Remove permission rule id and restoring permissions from {}: Query {}, Update {}", entry,
                    tmpQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            QueryResult<UpdateResult> updateResult = collection.update(tmpQuery, update, new QueryOptions("multi", true));
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogException("Could not update and remove permission rule from entry " + myDocument.get(PRIVATE_UID));
            }
        }

        // 2. Remove the permission rule from the map in the study
        removeReferenceToPermissionRuleInStudy(study.getUid(), permissionRuleToDeleteId, entry);
    }

    @Override
    public void removePermissionRule(long studyId, String permissionRuleToDelete, Study.Entity entry) throws CatalogException {
        // Remove the __TODELETE tag...
        String permissionRuleId = permissionRuleToDelete.split(INTERNAL_DELIMITER)[0];

        Document query = new Document()
                .append(PRIVATE_STUDY_ID, studyId)
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        Document update = new Document()
                .append("$pull", new Document(PERMISSION_RULES_APPLIED, permissionRuleId));
        logger.debug("Remove permission rule id from all {} in study {}: Query {}, Update {}", entry, studyId,
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

        MongoDBCollection collection = dbCollectionMap.get(entry.getEntity());
        QueryResult<UpdateResult> updateResult = collection.update(query, update, new QueryOptions("multi", true));
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogException("Could not remove permission rule id " + permissionRuleId + " from all " + entry);
        }

        // Remove the permission rule from the map in the study
        removeReferenceToPermissionRuleInStudy(studyId, permissionRuleToDelete, entry);
    }

    private boolean isPermissionRuleEntity(Entity entity) {
        if (Study.Entity.CLINICAL_ANALYSES.getEntity() == entity || Study.Entity.COHORTS.getEntity() == entity
                || Study.Entity.FAMILIES.getEntity() == entity || Study.Entity.FILES.getEntity() == entity
                || Study.Entity.INDIVIDUALS.getEntity() == entity || Study.Entity.JOBS.getEntity() == entity
                || Study.Entity.SAMPLES.getEntity() == entity) {
            return true;
        }
        return false;
    }

    private void removeReferenceToPermissionRuleInStudy(long studyId, String permissionRuleToDelete, Study.Entity entry)
            throws CatalogException {
        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(StudyDBAdaptor.QueryParams.PERMISSION_RULES.key() + "." + entry + ".id", permissionRuleToDelete);
        Document update = new Document("$pull",
                new Document(StudyDBAdaptor.QueryParams.PERMISSION_RULES.key() + "." + entry,
                        new Document("id", permissionRuleToDelete)));
        logger.debug("Remove permission rule from the study {}: Query {}, Update {}", studyId,
                query.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        QueryResult<UpdateResult> updateResult = dbCollectionMap.get(Entity.STUDY).update(query, update, new QueryOptions("multi", true));
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogException("Could not remove permission rule " + permissionRuleToDelete + " from study "
                    + String.valueOf(studyId));
        }
    }

    private Bson parseQuery(Query query, Document rawQuery, Entity entry) throws CatalogException {
        switch (entry) {
            case COHORT:
                return dbAdaptorFactory.getCatalogCohortDBAdaptor().parseQuery(query, rawQuery);
            case INDIVIDUAL:
                return dbAdaptorFactory.getCatalogIndividualDBAdaptor().parseQuery(query, rawQuery);
            case JOB:
                return dbAdaptorFactory.getCatalogJobDBAdaptor().parseQuery(query, rawQuery);
            case FILE:
                return dbAdaptorFactory.getCatalogFileDBAdaptor().parseQuery(query, rawQuery);
            case SAMPLE:
                return dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, rawQuery);
            case FAMILY:
                return dbAdaptorFactory.getCatalogFamilyDBAdaptor().parseQuery(query, rawQuery);
            case CLINICAL_ANALYSIS:
                return dbAdaptorFactory.getClinicalAnalysisDBAdaptor().parseQuery(query, rawQuery);
            default:
                throw new CatalogException("Unexpected parameter received. " + entry + " has been received.");
        }
    }

    private void removePermissions(long studyId, List<String> users, Entity entity) {
        List<String> permissions = fullPermissionsMap.get(entity);
        List<String> removePermissions = createPermissionArray(users, permissions);

        MongoDBCollection collection = dbCollectionMap.get(entity);
        Document queryDocument = new Document()
                .append(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.ACL.key(), new Document("$in", removePermissions));
        Document update = new Document("$pullAll", new Document()
                .append(QueryParams.ACL.key(), removePermissions)
                .append(QueryParams.USER_DEFINED_ACLS.key(), removePermissions)
        );

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
