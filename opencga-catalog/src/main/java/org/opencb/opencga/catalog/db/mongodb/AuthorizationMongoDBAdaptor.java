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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationDBAdaptor;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.AclEntry;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
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

    private final Map<Enums.Resource, List<MongoDBCollection>> dbCollectionMap = new HashMap<>();

    private static final String ANONYMOUS = "*";
    static final String MEMBER_WITH_INTERNAL_ACL = "_withInternalAcls";

    public AuthorizationMongoDBAdaptor(DBAdaptorFactory dbFactory, Configuration configuration) {
        super(configuration, LoggerFactory.getLogger(AuthorizationMongoDBAdaptor.class));
        this.dbAdaptorFactory = (MongoDBAdaptorFactory) dbFactory;
        initCollectionConnections();
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
        this.dbCollectionMap.put(Enums.Resource.STUDY,
                Collections.singletonList(dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyCollection()));
        this.dbCollectionMap.put(Enums.Resource.COHORT,
                Collections.singletonList(dbAdaptorFactory.getCatalogCohortDBAdaptor().getCohortCollection()));
        this.dbCollectionMap.put(Enums.Resource.FILE,
                Collections.singletonList(dbAdaptorFactory.getCatalogFileDBAdaptor().getCollection()));
        this.dbCollectionMap.put(Enums.Resource.JOB,
                Collections.singletonList(dbAdaptorFactory.getCatalogJobDBAdaptor().getJobCollection()));
        this.dbCollectionMap.put(Enums.Resource.CLINICAL_ANALYSIS,
                Collections.singletonList(dbAdaptorFactory.getClinicalAnalysisDBAdaptor().getClinicalCollection()));

        // Versioned models will always have first the main collection and second the archive collection
        this.dbCollectionMap.put(Enums.Resource.INDIVIDUAL, Arrays.asList(dbAdaptorFactory.getCatalogIndividualDBAdaptor().getCollection(),
                dbAdaptorFactory.getCatalogIndividualDBAdaptor().getIndividualArchiveCollection()));
        this.dbCollectionMap.put(Enums.Resource.SAMPLE, Arrays.asList(dbAdaptorFactory.getCatalogSampleDBAdaptor().getCollection(),
                dbAdaptorFactory.getCatalogSampleDBAdaptor().getArchiveSampleCollection()));
        this.dbCollectionMap.put(Enums.Resource.DISEASE_PANEL,
                Arrays.asList(dbAdaptorFactory.getCatalogPanelDBAdaptor().getPanelCollection(),
                        dbAdaptorFactory.getCatalogPanelDBAdaptor().getPanelArchiveCollection()));
        this.dbCollectionMap.put(Enums.Resource.FAMILY, Arrays.asList(dbAdaptorFactory.getCatalogFamilyDBAdaptor().getCollection(),
                dbAdaptorFactory.getCatalogFamilyDBAdaptor().getArchiveFamilyCollection()));
    }

    private List<String> getFullPermissions(Enums.Resource resource) {
        List<String> permissionList = new ArrayList<>(resource.getFullPermissionList());
        permissionList.add("NONE");
        return permissionList;
    }

    private void validateEntry(Enums.Resource entry) throws CatalogDBException {
        switch (entry) {
            case STUDY:
            case COHORT:
            case INDIVIDUAL:
            case JOB:
            case FILE:
            case SAMPLE:
            case DISEASE_PANEL:
            case FAMILY:
            case CLINICAL_ANALYSIS:
            case CLINICAL:
                return;
            default:
                throw new CatalogDBException("Unexpected parameter received. " + entry + " has been received.");
        }
    }

    /**
     * Internal method to fetch the permissions of every user. Permissions are splitted and returned in a map of user -> list of
     * permissions.
     *
     * @param resourceId  Resource id being queried.
     * @param membersList Members for which we want to fetch the permissions. If empty, it should return the permissions for all members.
     * @param entry       Entity where the query will be performed.
     * @return A map of [acl, user_defined_acl] -> user -> List of permissions and the string id of the resource queried.
     */
    private EntryPermission internalGet(long resourceId, List<String> membersList, Enums.Resource entry) {
        EntryPermission entryPermission = new EntryPermission();

        List<String> members = (membersList == null ? Collections.emptyList() : membersList);

        MongoDBCollection collection = dbCollectionMap.get(entry).get(0);

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
            logger.debug("Get Acl: {}", bson.toBsonDocument());
        }

        DataResult<Document> aggregate = collection.aggregate(aggregation, null);

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
                        permissions.get(QueryParams.ACL.key()).get(split[0]).add(split[1]);
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
                        permissions.get(QueryParams.USER_DEFINED_ACLS.key()).get(split[0]).add(split[1]);
                    }
                }
            }
        }

        // ------- Check for members with other permissions. In that case, we need to remove NONE from the list.
        for (Map.Entry<String, Map<String, List<String>>> tmpEntry : entryPermission.getPermissions().entrySet()) {
            for (Map.Entry<String, List<String>> tmpMemberPermissionEntry : tmpEntry.getValue().entrySet()) {
                if (tmpMemberPermissionEntry.getValue().size() > 1) {
                    tmpMemberPermissionEntry.getValue().remove("NONE");
                }
            }
        }

        return entryPermission;
    }

    static class EntryPermission {
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
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> get(long resourceId, List<String> members,
                                                                  Map<String, List<String>> userGroups, Enums.Resource entry,
                                                                  Class<T> clazz) throws CatalogException {
        validateEntry(entry);
        long startTime = startQuery();

        // Extract unique whole list of members requested by the user and any groups the users might belong to
        List<String> memberList = null;
        if (members != null) {
            Set<String> uniqueMembers = new HashSet<>(members);
            if (userGroups != null) {
                for (List<String> groups : userGroups.values()) {
                    uniqueMembers.addAll(groups);
                }
            }
            memberList = new ArrayList<>(uniqueMembers);
        }

        EntryPermission entryPermission = internalGet(resourceId, memberList, entry);
        Map<String, List<String>> myMap = entryPermission.getPermissions().get(QueryParams.ACL.key());

        AclEntryList<T> aclList = new AclEntryList<>();
        if (members != null) {
            for (String member : members) {
                EnumSet<T> permissions = null;
                if (myMap.containsKey(member)) {
                    List<T> allPermissions = myMap.get(member).stream().map(p -> T.valueOf(clazz, p)).collect(Collectors.toList());
                    permissions = allPermissions.isEmpty() ? EnumSet.noneOf(clazz) : EnumSet.copyOf(allPermissions);
                }
                List<AclEntry.GroupAclEntry<T>> groups = new ArrayList<>();
                if (userGroups != null && userGroups.containsKey(member)) {
                    for (String group : userGroups.get(member)) {
                        EnumSet<T> groupPermissions = null;
                        if (myMap.containsKey(group)) {
                            List<T> allPermissions = myMap.get(group).stream().map(p -> T.valueOf(clazz, p)).collect(Collectors.toList());
                            groupPermissions = allPermissions.isEmpty() ? EnumSet.noneOf(clazz) : EnumSet.copyOf(allPermissions);
                        }
                        groups.add(new AclEntry.GroupAclEntry<>(group, groupPermissions));
                    }
                }
                aclList.getAcl().add(new AclEntry<>(member, permissions, groups));
            }
        } else {
            for (Map.Entry<String, List<String>> tmpEntry : myMap.entrySet()) {
                List<T> allPermissions = tmpEntry.getValue().stream().map(p -> T.valueOf(clazz, p)).collect(Collectors.toList());
                EnumSet<T> permissions = allPermissions.isEmpty() ? EnumSet.noneOf(clazz) : EnumSet.copyOf(allPermissions);
                aclList.getAcl().add(new AclEntry<>(tmpEntry.getKey(), permissions, Collections.emptyList()));
            }
        }

        return endQuery(startTime, Collections.singletonList(aclList));
    }

    @Override
    public <T extends Enum<T>> OpenCGAResult<AclEntryList<T>> get(List<Long> resourceIds, List<String> members,
                                                                  Map<String, List<String>> userGroups, Enums.Resource entry,
                                                                  Class<T> clazz) throws CatalogException {
        OpenCGAResult<AclEntryList<T>> result = OpenCGAResult.empty();
        for (Long resourceId : resourceIds) {
            OpenCGAResult<AclEntryList<T>> tmpResult = get(resourceId, members, userGroups, entry, clazz);
            result.append(tmpResult);
        }
        return result;
    }

    @Override
    public OpenCGAResult<?> removeFromStudy(long studyId, String member, Enums.Resource resource) throws CatalogException {
        validateEntry(resource);

        Document query = new Document()
                .append(PRIVATE_STUDY_UID, studyId);
        List<String> removePermissions = createPermissionArray(Arrays.asList(member), getFullPermissions(resource));
        Document update = new Document("$pullAll", new Document()
                .append(QueryParams.ACL.key(), removePermissions)
                .append(QueryParams.USER_DEFINED_ACLS.key(), removePermissions)
        );
        logger.debug("Remove all acls for entity {} for member {} in study {}. Query: {}, pullAll: {}", resource, member, studyId,
                query.toBsonDocument(), update.toBsonDocument());
        return new OpenCGAResult<>(update(null, query, update, resource));
    }

    @Override
    public OpenCGAResult setToMembers(long studyId, List<String> members, List<AuthorizationManager.CatalogAclParams> aclParams)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long startTime = startQuery();

            // We obtain which of those members are actually users to add them to the @members group automatically
            addToMembersGroupInStudy(studyId, members, clientSession);

            for (AuthorizationManager.CatalogAclParams aclParam : aclParams) {
                setToMembers(aclParam.getIds(), members, aclParam.getPermissions(), aclParam.getResource(), clientSession);

                // We store that those members have internal permissions
                setMembersHaveInternalPermissionsDefined(studyId, members, aclParam.getResource(),
                        clientSession);
            }

            return endWrite(startTime, aclParams.get(0).getIds().size(), aclParams.get(0).getIds().size(), null);
        });
    }

    @Override
    public OpenCGAResult setToMembers(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long startTime = startQuery();
            for (Long studyId : studyIds) {
                addToMembersGroupInStudy(studyId, members, clientSession);
            }
            setToMembers(studyIds, members, permissions, Enums.Resource.STUDY, clientSession);

            return endWrite(startTime, 1, 1, null);
        });
    }

    private void setToMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, Enums.Resource resource,
                              ClientSession clientSession) throws CatalogDBException {
        validateEntry(resource);

        /* 1. We are going to try to remove all the permissions to those members in first instance */

        // We add the NONE permission by default so it is also taken out
        List<String> permissions = getFullPermissions(resource);
        permissions.add("NONE");
        permissions = createPermissionArray(members, permissions);

        Document queryDocument = new Document()
                .append(PRIVATE_UID, new Document("$in", resourceIds));
        Document update = new Document(QueryParams.ACL.key(), permissions);
        if (isPermissionRuleEntity(resource)) {
            update.put(QueryParams.USER_DEFINED_ACLS.key(), permissions);
        }
        update = new Document("$pullAll", update);
        logger.debug("Pull all acls: Query {}, PullAll {}, entity: {}", queryDocument.toBsonDocument(), update.toBsonDocument(), resource);
        update(clientSession, queryDocument, update, resource);

        /* 2. We now add the expected permissions to those members */

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        permissions = new ArrayList<>(permissionList);
        permissions.add("NONE");
        permissions = createPermissionArray(members, permissions);

        update = new Document(QueryParams.ACL.key(), new Document("$each", permissions));
        if (isPermissionRuleEntity(resource)) {
            update.put(QueryParams.USER_DEFINED_ACLS.key(), new Document("$each", permissions));
        }

        update = new Document("$addToSet", update);
        logger.debug("Add Acls (addToSet): Query {}, Push {}, entity: {}", queryDocument.toBsonDocument(), update.toBsonDocument(),
                resource);
        update(clientSession, queryDocument, update, resource);
    }

    @Override
    public OpenCGAResult addToMembers(long studyId, List<String> members, List<AuthorizationManager.CatalogAclParams> aclParams)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long startTime = startQuery();
            addToMembersGroupInStudy(studyId, members, clientSession);

            for (AuthorizationManager.CatalogAclParams aclParam : aclParams) {
                addToMembers(aclParam.getIds(), members, aclParam.getPermissions(), aclParam.getResource(), clientSession);

                // We store that those members have internal permissions
                setMembersHaveInternalPermissionsDefined(studyId, members, aclParam.getResource(),
                        clientSession);
            }

            return endWrite(startTime, aclParams.get(0).getIds().size(), aclParams.get(0).getIds().size(), null);
        });
    }

    private void addToMembers(List<Long> resourceIds, List<String> members, List<String> permissionList, Enums.Resource resource,
                              ClientSession clientSession) throws CatalogDBException {
        validateEntry(resource);

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        List<String> permissions = new ArrayList<>(permissionList);
        permissions.add("NONE");

        List<String> myPermissions = createPermissionArray(members, permissions);

        Document queryDocument = new Document()
                .append(PRIVATE_UID, new Document("$in", resourceIds));
        Document update;
        if (isPermissionRuleEntity(resource)) {
            update = new Document("$addToSet", new Document()
                    .append(QueryParams.ACL.key(), new Document("$each", myPermissions))
                    .append(QueryParams.USER_DEFINED_ACLS.key(), new Document("$each", myPermissions))
            );
        } else {
            update = new Document("$addToSet", new Document(QueryParams.ACL.key(), new Document("$each", myPermissions)));
        }

        logger.debug("Add Acls (addToSet): Query {}, Push {}", queryDocument.toBsonDocument(), update.toBsonDocument());
        update(clientSession, queryDocument, update, resource);
    }

    @Override
    public OpenCGAResult addToMembers(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction((clientSession) -> {
            long startTime = startQuery();
            for (Long studyId : studyIds) {
                addToMembersGroupInStudy(studyId, members, clientSession);
            }

            addToMembers(studyIds, members, permissions, Enums.Resource.STUDY, clientSession);

            return endWrite(startTime, 1, 1, null);
        });
    }

    private void addToMembersGroupInStudy(long studyId, List<String> members, ClientSession clientSession) throws CatalogDBException {
        // We obtain which of those members are actually users to add them to the @members group automatically
        List<String> userList = members.stream()
                .filter(member -> !member.startsWith("@"))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(userList)) {
            // We first add the member to the @members group in case they didn't belong already
            dbAdaptorFactory.getCatalogStudyDBAdaptor().addUsersToGroup(clientSession, studyId, CatalogAuthorizationManager.MEMBERS_GROUP,
                    userList);
        }
    }

    @Override
    public OpenCGAResult removeFromMembers(List<String> members, List<AuthorizationManager.CatalogAclParams> aclParams)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return runTransaction(clientSession -> {
            long startTime = startQuery();

            for (AuthorizationManager.CatalogAclParams aclParam : aclParams) {
                removeFromMembers(clientSession, aclParam.getIds(), members, aclParam.getPermissions(), aclParam.getResource());
            }

            return endWrite(startTime, aclParams.get(0).getIds().size(), aclParams.get(0).getIds().size(), null);
        });
    }

    private void removeFromMembers(ClientSession clientSession, List<Long> resourceIds, List<String> members, List<String> permissionList,
                                   Enums.Resource resource) throws CatalogDBException {
        validateEntry(resource);

        List<String> permissions = permissionList;

        if (permissions == null || permissions.isEmpty()) {
            // We get all possible permissions those members will have to do a full reset
            permissions = getFullPermissions(resource);
        }

        List<String> removePermissions = createPermissionArray(members, permissions);
        Document queryDocument = new Document()
                .append(PRIVATE_UID, new Document("$in", resourceIds));
        Document update;
        if (isPermissionRuleEntity(resource)) {
            update = new Document("$pullAll", new Document()
                    .append(QueryParams.ACL.key(), removePermissions)
                    .append(QueryParams.USER_DEFINED_ACLS.key(), removePermissions)
            );
        } else {
            update = new Document("$pullAll", new Document(QueryParams.ACL.key(), removePermissions));
        }

        logger.debug("Remove Acls (pullAll): Query {}, Pull {}", queryDocument.toBsonDocument(), update.toBsonDocument());
        update(clientSession, queryDocument, update, resource);
    }

    @Override
    public OpenCGAResult resetMembersFromAllEntries(long studyId, List<String> members)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (members == null || members.isEmpty()) {
            throw new CatalogDBException("Missing 'members' array.");
        }

        return runTransaction(clientSession -> {
            long tmpStartTime = startQuery();
            logger.debug("Resetting permissions of users '{}' for study '{}'", members, studyId);

            dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(clientSession, studyId);
            removePermissions(clientSession, studyId, members, Enums.Resource.COHORT);
            removePermissions(clientSession, studyId, members, Enums.Resource.FILE);
            removePermissions(clientSession, studyId, members, Enums.Resource.INDIVIDUAL);
            removePermissions(clientSession, studyId, members, Enums.Resource.JOB);
            removePermissions(clientSession, studyId, members, Enums.Resource.SAMPLE);
            removePermissions(clientSession, studyId, members, Enums.Resource.DISEASE_PANEL);
            removePermissions(clientSession, studyId, members, Enums.Resource.FAMILY);
            removePermissions(clientSession, studyId, members, Enums.Resource.CLINICAL_ANALYSIS);
            removeFromMembers(clientSession, Arrays.asList(studyId), members, null, Enums.Resource.STUDY);

            return endWrite(tmpStartTime, -1, -1, null);
        });
    }

    // TODO: Make this method transactional
    @Override
    public OpenCGAResult setAcls(List<Long> resourceIds, AclEntryList<?> acls, Enums.Resource resource)
            throws CatalogDBException {
        validateEntry(resource);

        for (long resourceId : resourceIds) {
            // Get current permissions for resource and override with new ones set for members (already existing or not)
            Map<String, Map<String, List<String>>> currentPermissions = internalGet(resourceId, Collections.emptyList(), resource)
                    .getPermissions();
            for (AclEntry<?> acl : acls.getAcl()) {
                // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
                List<String> permissions = acl.getPermissions().stream().map(Enum::name).collect(Collectors.toList());
                permissions.add("NONE");
                currentPermissions.get(QueryParams.ACL.key()).put(acl.getMember(), permissions);
                currentPermissions.get(QueryParams.USER_DEFINED_ACLS.key()).put(acl.getMember(), permissions);
            }
            List<String> permissionArray = createPermissionArray(currentPermissions.get(QueryParams.ACL.key()));
            List<String> manualPermissionArray = createPermissionArray(currentPermissions.get(QueryParams.USER_DEFINED_ACLS.key()));

            Document queryDocument = new Document()
                    .append(PRIVATE_UID, resourceId);
            Document update;
            if (isPermissionRuleEntity(resource)) {
                update = new Document("$set", new Document()
                        .append(QueryParams.ACL.key(), permissionArray)
                        .append(QueryParams.USER_DEFINED_ACLS.key(), manualPermissionArray));
            } else {
                update = new Document("$set", new Document(QueryParams.ACL.key(), permissionArray));
            }

            logger.debug("Set Acls (set): Query {}, Push {}", queryDocument.toBsonDocument(), update.toBsonDocument());
            update(null, queryDocument, update, resource);
        }

        return OpenCGAResult.empty();
    }

    private void setMembersHaveInternalPermissionsDefined(long studyId, List<String> members, Enums.Resource resource,
                                                          ClientSession clientSession) {
        Document queryDocument = new Document()
                .append(PRIVATE_UID, studyId);

        Document addToSet = new Document();
        for (String member : members) {
            addToSet.append(MEMBER_WITH_INTERNAL_ACL + "." + member, resource.name());
        }
        Document update = new Document("$addToSet", addToSet);
        update(clientSession, queryDocument, update, Enums.Resource.STUDY);
    }

    @Override
    public OpenCGAResult<?> applyPermissionRules(long studyId, PermissionRule permissionRule, Enums.Entity entry) throws CatalogException {
        // We will apply the permission rules to all the entries matching the query defined in the permission rules that does not have
        // the permission rules applied yet
        Document rawQuery = new Document()
                .append(PRIVATE_STUDY_UID, studyId)
                .append(PERMISSION_RULES_APPLIED, new Document("$ne", permissionRule.getId()));
        Bson bson = parseQuery(permissionRule.getQuery(), rawQuery, entry.getResource());

        // We add the NONE permission by default so when a user is removed some permissions (not reset), the NONE permission remains
        List<String> permissions = new ArrayList<>(permissionRule.getPermissions());
        permissions.add("NONE");
        List<String> myPermissions = createPermissionArray(permissionRule.getMembers(), permissions);

        Document update = new Document()
                .append("$addToSet", new Document()
                        .append(QueryParams.ACL.key(), new Document("$each", myPermissions))
                        .append(PERMISSION_RULES_APPLIED, permissionRule.getId()));

        logger.debug("Apply permission rules: Query {}, Update {}", bson.toBsonDocument(), update.toBsonDocument());
        return new OpenCGAResult<>(update(null, bson, update, entry.getResource()));
    }

    //TODO: Make transactional !
    @Override
    public OpenCGAResult removePermissionRuleAndRemovePermissions(Study study, String permissionRuleToDeleteId, Enums.Entity entry)
            throws CatalogException {
        // Prepare the permission rule list into a map of permissionRuleId - PermissionRule to make much easier the process
        Map<String, PermissionRule> permissionRuleMap = study.getPermissionRules().get(entry).stream()
                .collect(Collectors.toMap(PermissionRule::getId, p -> p));
        PermissionRule permissionRuleToDelete = permissionRuleMap.get(permissionRuleToDeleteId);

        Set<String> permissionsToRemove =
                createPermissionArray(permissionRuleToDelete.getMembers(), permissionRuleToDelete.getPermissions())
                        .stream().collect(Collectors.toSet());

        // Remove the __TODELETE tag...
        String permissionRuleId = permissionRuleToDeleteId.split(INTERNAL_DELIMITER)[0];

        // 1. Get all the entries that have the permission rule to be removed applied
        Document query = new Document()
                .append(PRIVATE_STUDY_UID, study.getUid())
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ACL.key(), QueryParams.USER_DEFINED_ACLS.key(), PERMISSION_RULES_APPLIED, PRIVATE_UID));
        MongoDBIterator<Document> iterator = dbCollectionMap.get(entry.getResource()).get(0).iterator(query, options);
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
                    .append(PRIVATE_STUDY_UID, study.getUid());

            Document update = new Document("$set", new Document()
                    .append(QueryParams.ACL.key(), effectivePermissions)
                    .append(QueryParams.USER_DEFINED_ACLS.key(), manualPermissions)
                    .append(PERMISSION_RULES_APPLIED, permissionRulesApplied));

            logger.debug("Remove permission rule id and permissions from {}: Query {}, Update {}", entry, tmpQuery.toBsonDocument(),
                    update.toBsonDocument());
            DataResult<?> result = update(null, tmpQuery, update, entry.getResource());
            if (result.getNumUpdated() == 0) {
                throw new CatalogException("Could not update and remove permission rule from entry " + myDocument.get(PRIVATE_UID));
            }
        }

        // 2. Remove the permission rule from the map in the study
        removeReferenceToPermissionRuleInStudy(study.getUid(), permissionRuleToDeleteId, entry);

        return OpenCGAResult.empty();
    }

    @Override
    public OpenCGAResult removePermissionRuleAndRestorePermissions(Study study, String permissionRuleToDeleteId, Enums.Entity entry)
            throws CatalogException {
        // Prepare the permission rule list into a map of permissionRuleId - PermissionRule to make much easier the process
        Map<String, PermissionRule> permissionRuleMap = study.getPermissionRules().get(entry).stream()
                .collect(Collectors.toMap(PermissionRule::getId, p -> p));
        PermissionRule permissionRuleToDelete = permissionRuleMap.get(permissionRuleToDeleteId);

        Set<String> permissionsToRemove =
                createPermissionArray(permissionRuleToDelete.getMembers(), permissionRuleToDelete.getPermissions())
                        .stream().collect(Collectors.toSet());

        // Remove the __TODELETE tag...
        String permissionRuleId = permissionRuleToDeleteId.split(INTERNAL_DELIMITER)[0];

        // 1. Get all the entries that have the permission rule to be removed applied
        Document query = new Document()
                .append(PRIVATE_STUDY_UID, study.getUid())
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ACL.key(), QueryParams.USER_DEFINED_ACLS.key(), PERMISSION_RULES_APPLIED, PRIVATE_UID));
        MongoDBIterator<Document> iterator = dbCollectionMap.get(entry.getResource()).get(0).iterator(query, options);
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
                    .append(PRIVATE_STUDY_UID, study.getUid());

            Document update = new Document("$set", new Document()
                    .append(QueryParams.ACL.key(), effectivePermissions)
                    .append(PERMISSION_RULES_APPLIED, permissionRulesApplied));

            logger.debug("Remove permission rule id and restoring permissions from {}: Query {}, Update {}", entry,
                    tmpQuery.toBsonDocument(), update.toBsonDocument());
            DataResult<?> result = update(null, tmpQuery, update, entry.getResource());
            if (result.getNumUpdated() == 0) {
                throw new CatalogException("Could not update and remove permission rule from entry " + myDocument.get(PRIVATE_UID));
            }
        }

        // 2. Remove the permission rule from the map in the study
        removeReferenceToPermissionRuleInStudy(study.getUid(), permissionRuleToDeleteId, entry);

        return OpenCGAResult.empty();
    }

    //TODO: Make transactional !
    @Override
    public OpenCGAResult removePermissionRule(long studyId, String permissionRuleToDelete, Enums.Entity entry) throws CatalogException {
        // Remove the __TODELETE tag...
        String permissionRuleId = permissionRuleToDelete.split(INTERNAL_DELIMITER)[0];

        Document query = new Document()
                .append(PRIVATE_STUDY_UID, studyId)
                .append(PERMISSION_RULES_APPLIED, permissionRuleId);
        Document update = new Document()
                .append("$pull", new Document(PERMISSION_RULES_APPLIED, permissionRuleId));
        logger.debug("Remove permission rule id from all {} in study {}: Query {}, Update {}", entry, studyId, query.toBsonDocument(),
                update.toBsonDocument());

        DataResult<?> result = update(null, query, update, entry.getResource());
        if (result.getNumUpdated() == 0) {
            throw new CatalogException("Could not remove permission rule id " + permissionRuleId + " from all " + entry);
        }

        // Remove the permission rule from the map in the study
        removeReferenceToPermissionRuleInStudy(studyId, permissionRuleToDelete, entry);

        return OpenCGAResult.empty();
    }

    private boolean isPermissionRuleEntity(Enums.Resource resource) {
        if (Enums.Entity.CLINICAL_ANALYSES.getResource() == resource || Enums.Entity.COHORTS.getResource() == resource
                || Enums.Entity.FAMILIES.getResource() == resource || Enums.Entity.FILES.getResource() == resource
                || Enums.Entity.INDIVIDUALS.getResource() == resource || Enums.Entity.JOBS.getResource() == resource
                || Enums.Entity.SAMPLES.getResource() == resource) {
            return true;
        }
        return false;
    }

    private void removeReferenceToPermissionRuleInStudy(long studyId, String permissionRuleToDelete, Enums.Entity entry)
            throws CatalogException {
        Document query = new Document()
                .append(PRIVATE_UID, studyId)
                .append(StudyDBAdaptor.QueryParams.PERMISSION_RULES.key() + "." + entry + ".id", permissionRuleToDelete);
        Document update = new Document("$pull",
                new Document(StudyDBAdaptor.QueryParams.PERMISSION_RULES.key() + "." + entry,
                        new Document("id", permissionRuleToDelete)));
        logger.debug("Remove permission rule from the study {}: Query {}, Update {}", studyId, query.toBsonDocument(),
                update.toBsonDocument());
        DataResult<?> result = update(null, query, update, Enums.Resource.STUDY);
        if (result.getNumUpdated() == 0) {
            throw new CatalogException("Could not remove permission rule " + permissionRuleToDelete + " from study " + studyId);
        }
    }

    private Bson parseQuery(Query query, Document rawQuery, Enums.Resource entry) throws CatalogException {
        switch (entry) {
            case COHORT:
                return dbAdaptorFactory.getCatalogCohortDBAdaptor().parseQuery(query, rawQuery);
            case INDIVIDUAL:
                return dbAdaptorFactory.getCatalogIndividualDBAdaptor().parseQuery(query, rawQuery);
            case JOB:
                return dbAdaptorFactory.getCatalogJobDBAdaptor().parseQuery(query, rawQuery, QueryOptions.empty());
            case FILE:
                return dbAdaptorFactory.getCatalogFileDBAdaptor().parseQuery(query, rawQuery);
            case SAMPLE:
                return dbAdaptorFactory.getCatalogSampleDBAdaptor().parseQuery(query, rawQuery);
            case FAMILY:
                return dbAdaptorFactory.getCatalogFamilyDBAdaptor().parseQuery(query, rawQuery);
            case CLINICAL_ANALYSIS:
            case CLINICAL:
                return dbAdaptorFactory.getClinicalAnalysisDBAdaptor().parseQuery(query, rawQuery);
            default:
                throw new CatalogException("Unexpected parameter received. " + entry + " has been received.");
        }
    }

    private void removePermissions(ClientSession clientSession, long studyId, List<String> users, Enums.Resource resource) {
        List<String> permissions = getFullPermissions(resource);
        List<String> removePermissions = createPermissionArray(users, permissions);

        Document queryDocument = new Document()
                .append(PRIVATE_STUDY_UID, studyId)
                .append(QueryParams.ACL.key(), new Document("$in", removePermissions));
        Document update = new Document("$pullAll", new Document()
                .append(QueryParams.ACL.key(), removePermissions)
                .append(QueryParams.USER_DEFINED_ACLS.key(), removePermissions)
        );

        update(clientSession, queryDocument, update, resource);
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

    private DataResult<?> update(ClientSession clientSession, Bson query, Bson update, Enums.Resource resource) {
        List<MongoDBCollection> collections = dbCollectionMap.get(resource);
        QueryOptions options = new QueryOptions(MongoDBCollection.MULTI, true);

        DataResult<?> result = collections.get(0).update(clientSession, query, update, options);
        if (collections.size() == 2) {
            collections.get(1).update(clientSession, query, update, options);
        }
        return result;
    }
}
