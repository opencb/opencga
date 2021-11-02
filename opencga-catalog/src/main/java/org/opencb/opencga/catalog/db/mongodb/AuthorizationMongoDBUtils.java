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

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.StudyAclEntry;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBAdaptor.MEMBER_WITH_INTERNAL_ACL;

/**
 * Created by pfurio on 31/07/17.
 */
public class AuthorizationMongoDBUtils {

    static final String OPENCGA = "opencga";
    static final String PRIVATE_OWNER_ID = "_ownerId";
    private static final String PRIVATE_ACL = "_acl";
    private static final String VARIABLE_SETS = "variableSets";
    private static final String ANNOTATION_SETS = AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key();

    private static final String PERMISSION_DELIMITER = "__";

    private static final String ANONYMOUS = ParamConstants.ANONYMOUS_USER_ID;
    private static final String REGISTERED_USERS = ParamConstants.REGISTERED_USERS;

    private static final String MEMBERS = ParamConstants.MEMBERS_GROUP;
    private static final String ADMINS = ParamConstants.ADMINS_GROUP;

    private static final Pattern MEMBERS_PATTERN = Pattern.compile("^" + MEMBERS);
    private static final Pattern REGISTERED_USERS_PATTERN = Pattern.compile("^" + REGISTERED_USERS);
    private static final Pattern ANONYMOUS_PATTERN = Pattern.compile("^\\" + ANONYMOUS);

    public static boolean checkCanViewStudy(Document study, String user) {
        // 0. If the user corresponds with the owner, we don't have to check anything else
        if (study.getString(PRIVATE_OWNER_ID).equals(user)) {
            return true;
        }
        if (OPENCGA.equals(user)) {
            return true;
        }
        // If user does not exist in the members group, the user will not have any permission
        if (isUserInMembers(study, user)) {
            return true;
        }
        return false;
    }

    public static boolean checkStudyPermission(Document study, String user, String studyPermission) {
        // 0. If the user corresponds with the owner, we don't have to check anything else
        if (study.getString(PRIVATE_OWNER_ID).equals(user)) {
            return true;
        }
        if (OPENCGA.equals(user)) {
            return true;
        }
        if (getAdminUsers(study).contains(user)) {
            return true;
        }

        // If user does not exist in the members group, the user will not have any permission
        if (!isUserInMembers(study, user)) {
            return false;
        }

        if (user.equals(ANONYMOUS)) {
            return checkAnonymousHasPermission(study, studyPermission);
        } else {
            // 1. We obtain the groups of the user
            List<String> groups = getGroups(study, user);

            // 2. We check if the study contains the studies expected for the user
            return checkUserHasPermission(study, user, groups, studyPermission, false);
        }
    }

    /**
     * Removes annotation sets from results if the user does not have the proper permissions.
     *
     * @param study           study document.
     * @param entry           Annotable document entry.
     * @param user            user.
     * @param studyPermission studyPermission to check.
     * @param entryPermission entry permission to check.
     * @return the document modified.
     */
    public static Document filterAnnotationSets(Document study, Document entry, String user, String studyPermission,
                                                String entryPermission) {
        if (study == null || entry == null) {
            return entry;
        }

        // If the user corresponds with the owner, we don't have to check anything else
        if (study.getString(PRIVATE_OWNER_ID).equals(user)) {
            return entry;
        }
        if (OPENCGA.equals(user)) {
            return entry;
        }
        if (getAdminUsers(study).contains(user)) {
            return entry;
        }

        List<String> groups = Collections.emptyList();
        if (!user.equals(ANONYMOUS)) {
            groups = getGroups(study, user);
        }
        boolean hasStudyPermission = checkUserHasPermission(study, user, groups, studyPermission, false);

        if (!checkUserHasPermission(entry, user, groups, entryPermission, hasStudyPermission)) {
            entry.put(ANNOTATION_SETS, Collections.emptyList());
        } else {
            // Check if the user has the CONFIDENTIAL PERMISSION
            boolean confidential =
                    checkStudyPermission(study, user, StudyAclEntry.StudyPermissions.CONFIDENTIAL_VARIABLE_SET_ACCESS.toString());
            if (!confidential) {
                // If the user does not have the confidential permission, we will have to remove those annotation sets coming from
                // confidential variable sets
                List<Document> variableSets = (List<Document>) study.get(VARIABLE_SETS);
                Set<Long> confidentialVariableSets = new HashSet<>();
                for (Document variableSet : variableSets) {
                    if (variableSet.getBoolean("confidential")) {
                        confidentialVariableSets.add(variableSet.getLong("id"));
                    }
                }

                if (!confidentialVariableSets.isEmpty()) {
                    // The study contains confidential variable sets so we do have to check if any of the annotations come from
                    // confidential variable sets
                    Iterator<Document> iterator = ((List<Document>) entry.get(ANNOTATION_SETS)).iterator();
                    while (iterator.hasNext()) {
                        Document annotationSet = iterator.next();
                        if (confidentialVariableSets.contains(annotationSet.getLong("variableSetId"))) {
                            iterator.remove();
                        }
                    }
                }
            }

        }
        return entry;
    }

    private static int getPermissionType(Enums.Resource resource) throws CatalogParameterException {
        switch (resource) {
            case FILE:
                return StudyAclEntry.FILE;
            case SAMPLE:
                return StudyAclEntry.SAMPLE;
            case JOB:
                return StudyAclEntry.EXECUTION;
            case INDIVIDUAL:
                return StudyAclEntry.INDIVIDUAL;
            case COHORT:
                return StudyAclEntry.COHORT;
            case DISEASE_PANEL:
                return StudyAclEntry.DISEASE_PANEL;
            case FAMILY:
                return StudyAclEntry.FAMILY;
            case CLINICAL_ANALYSIS:
                return StudyAclEntry.CLINICAL_ANALYSIS;
            default:
                throw new CatalogParameterException("Unexpected resource '" + resource + "'.");
        }
    }

    /**
     * If query contains {@link ParamConstants#ACL_PARAM}, it will parse the value to generate the corresponding mongo query documents.
     *
     * @param study    Queried study document.
     * @param query    Original query.
     * @param resource Affected resource.
     * @param user     User performing the query.
     * @return A list of documents to satisfy the ACL query.
     * @throws CatalogDBException            when there is a DB error.
     * @throws CatalogParameterException     if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    public static List<Document> parseAclQuery(Document study, Query query, Enums.Resource resource, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseAclQuery(study, query, resource, user, null);
    }

    /**
     * If query contains {@link ParamConstants#ACL_PARAM}, it will parse the value to generate the corresponding mongo query documents.
     *
     * @param study         Queried study document.
     * @param query         Original query.
     * @param resource      Affected resource.
     * @param user          User performing the query.
     * @param configuration Configuration object.
     * @return A list of documents to satisfy the ACL query.
     * @throws CatalogDBException            when there is a DB error.
     * @throws CatalogParameterException     if there is any formatting error.
     * @throws CatalogAuthorizationException if the user is not authorised to perform the query.
     */
    public static List<Document> parseAclQuery(Document study, Query query, Enums.Resource resource, String user,
                                               Configuration configuration)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Document> aclDocuments = new LinkedList<>();
        if (!query.containsKey(ParamConstants.ACL_PARAM)) {
            return aclDocuments;
        }

        if (study == null || study.isEmpty()) {
            throw new CatalogDBException("Internal error: Missing study document to generate ACL query");
        }

        String[] userPermission = query.getString(ParamConstants.ACL_PARAM).split(":");
        if (userPermission.length != 2) {
            throw new CatalogParameterException("Unexpected format for '" + ParamConstants.ACL_PARAM + "'. " + ParamConstants.ACL_FORMAT);
        }
        String affectedUser = userPermission[0];
        List<String> permissions = Arrays.asList(userPermission[1].split(","));

        // If user is not checking its own permissions and it is not the owner or admin of the study, we fail
        if (!user.equals(affectedUser) && !study.getString(PRIVATE_OWNER_ID).equals(user) && !getAdminUsers(study).contains(user)) {
            throw new CatalogAuthorizationException("Only study owners or admins are authorised to see other user's permissions.");
        }

        // 0. If the user is the admin or corresponds with the owner, we don't have to check anything else
        if (study.getString(PRIVATE_OWNER_ID).equals(affectedUser)) {
            return aclDocuments;
        }
        if (OPENCGA.equals(affectedUser)) {
            return aclDocuments;
        }
        if (getAdminUsers(study).contains(affectedUser)) {
            return aclDocuments;
        }

        // If user does not exist in the members group, the user will not have any permission
        if (!isUserInMembers(study, affectedUser)) {
            throw new CatalogAuthorizationException("User " + affectedUser + " does not have any permissions in study "
                    + study.getString(StudyDBAdaptor.QueryParams.ID.key()));
        }

        boolean simplifyPermissionCheck = false;
        if (configuration != null && configuration.getOptimizations() != null) {
            simplifyPermissionCheck = configuration.getOptimizations().isSimplifyPermissions();
        }

        boolean isAnonymousPresent = false;
        boolean isRegisteredUsersPresent = false;
        List<String> groups;
        boolean hasStudyPermissions;

        if (!affectedUser.equals(ANONYMOUS)) {
            // 0. Check if anonymous has any permission defined (just for performance)
            isAnonymousPresent = isAnonymousInMembers(study);
            isRegisteredUsersPresent = isRegisteredUsersInMembers(study);

            // 1. We obtain the groups of the user
            groups = getGroups(study, affectedUser);
        } else {
            // 1. Anonymous user will not belong to any group
            groups = Collections.emptyList();
        }

        for (String permission : permissions) {
            String studyPermission = StudyAclEntry.StudyPermissions.getStudyPermission(permission, getPermissionType(resource)).name();

            if (!affectedUser.equals(ANONYMOUS)) {
                // We check if the study contains the studies expected for the user
                hasStudyPermissions = checkUserHasPermission(study, affectedUser, groups, studyPermission, false);
            } else {
                // 2. We check if the study contains the studies expected for the user
                hasStudyPermissions = checkAnonymousHasPermission(study, studyPermission);
            }

            if (hasStudyPermissions && !hasInternalPermissions(study, affectedUser, groups, resource.name())) {
                break;
            }

            Document queryDocument = getAuthorisedEntries(affectedUser, groups, permission, isRegisteredUsersPresent, isAnonymousPresent,
                    simplifyPermissionCheck);
            if (hasStudyPermissions) {
                // The user has permissions defined globally, so we also have to check the entries where the user/groups/members/* have no
                // permissions defined as the user will also be allowed to see them
                queryDocument = new Document("$or", Arrays.asList(
                        getNoPermissionsDefined(affectedUser, groups),
                        queryDocument
                ));
            }
            aclDocuments.add(queryDocument);
        }

        return aclDocuments;
    }

    public static Document getQueryForAuthorisedEntries(Document study, String user, String permission, Enums.Resource resource)
            throws CatalogAuthorizationException, CatalogParameterException {
        return getQueryForAuthorisedEntries(study, user, permission, resource, null);
    }

    public static Document getQueryForAuthorisedEntries(Document study, String user, String permission, Enums.Resource resource,
                                                        Configuration configuration)
            throws CatalogAuthorizationException, CatalogParameterException {
        if (StringUtils.isEmpty(user)) {
            return new Document();
        }

        // 0. If the user is the admin or corresponds with the owner, we don't have to check anything else
        if (study.getString(PRIVATE_OWNER_ID).equals(user)) {
            return new Document();
        }
        if (OPENCGA.equals(user)) {
            return new Document();
        }
        if (getAdminUsers(study).contains(user)) {
            return new Document();
        }

        // If user does not exist in the members group, the user will not have any permission
        if (!isUserInMembers(study, user)) {
            throw new CatalogAuthorizationException("User " + user + " does not have any permissions in study "
                    + study.getString(StudyDBAdaptor.QueryParams.ID.key()));
        }

        boolean simplifyPermissionCheck = false;
        if (configuration != null && configuration.getOptimizations() != null) {
            simplifyPermissionCheck = configuration.getOptimizations().isSimplifyPermissions();
        }

        String studyPermission = StudyAclEntry.StudyPermissions.getStudyPermission(permission, getPermissionType(resource)).name();

        // 0. Check if anonymous has any permission defined (just for performance)
        boolean isAnonymousPresent = isAnonymousInMembers(study);
        boolean isRegisteredUsersPresent = false;
        List<String> groups;
        boolean hasStudyPermissions;
        if (!user.equals(ANONYMOUS)) {
            isRegisteredUsersPresent = isRegisteredUsersInMembers(study);

            // 1. We obtain the groups of the user
            groups = getGroups(study, user);

            // 2. We check if the study contains the studies expected for the user
            hasStudyPermissions = checkUserHasPermission(study, user, groups, studyPermission, false);
        } else {
            // 1. Anonymous user will not belong to any group
            groups = Collections.emptyList();

            // 2. We check if the study contains the studies expected for the user
            hasStudyPermissions = checkAnonymousHasPermission(study, studyPermission);
        }

        if (hasStudyPermissions && !hasInternalPermissions(study, user, groups, resource.name())) {
            return new Document();
        }

        Document queryDocument = getAuthorisedEntries(user, groups, permission, isRegisteredUsersPresent, isAnonymousPresent,
                simplifyPermissionCheck);
        if (hasStudyPermissions && !simplifyPermissionCheck) {
            // The user has permissions defined globally, so we also have to check the entries where the user/groups/members/* have no
            // permissions defined as the user will also be allowed to see them
            queryDocument = new Document("$or", Arrays.asList(
                    getNoPermissionsDefined(user, groups),
                    queryDocument
            ));
        }

        return queryDocument;
    }

    public static boolean isAnonymousInMembers(Document study) {
        return isUserInMembers(study, ANONYMOUS);
    }

    public static boolean isRegisteredUsersInMembers(Document study) {
        return isUserInMembers(study, REGISTERED_USERS);
    }

    public static boolean isUserInMembers(Document study, String user) {
        List<Document> groupDocumentList = study.get(StudyDBAdaptor.QueryParams.GROUPS.key(), ArrayList.class);
        boolean isAnonymousUser = ANONYMOUS.equals(user);
        if (groupDocumentList != null && !groupDocumentList.isEmpty()) {
            for (Document group : groupDocumentList) {
                if ((MEMBERS).equals(group.getString("id"))) {
                    List<String> userIds = group.get("userIds", ArrayList.class);
                    for (String thisUser : userIds) {
                        if (thisUser.equals(user) || ANONYMOUS.equals(thisUser)
                                || (!isAnonymousUser && REGISTERED_USERS.equals(thisUser))) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        }
        return false;
    }

    public static boolean hasInternalPermissions(Document study, String user, List<String> groups, String entity) {
        Object object = study.get(MEMBER_WITH_INTERNAL_ACL);
        if (object == null) {
            // Permissions defined at the study level are always valid for all users
            return false;
        }

        List<String> members = new ArrayList<>();
        members.add(user);
        if (ListUtils.isNotEmpty(groups)) {
            members.addAll(groups);
        }

        for (String member : members) {
            Object entityObject = ((Document) object).get(member);
            if (entityObject == null) {
                // Permissions defined at the study level are always valid for user
                continue;
            }

            if (((List<String>) entityObject).contains(entity)) {
                return true;
            }
        }

        // Neither the user or the groups have that internal permission
        return false;
    }

    public static boolean checkAnonymousHasPermission(Document study, String studyPermission) {
        List<String> aclList = study.get(PRIVATE_ACL, ArrayList.class);
        Map<String, Set<String>> permissionMap = parsePermissions(aclList, ANONYMOUS, Collections.emptyList());

        // We now check if the anonymous user has the permission defined at the study level
        boolean hasStudyPermissions = false;
        if (permissionMap.get("user") != null) {
            hasStudyPermissions = permissionMap.get("user").contains(studyPermission);
        } else if (permissionMap.get("members") != null) {
            hasStudyPermissions = permissionMap.get("members").contains(studyPermission);
        } else if (permissionMap.get(ANONYMOUS) != null) {
            hasStudyPermissions = permissionMap.get(ANONYMOUS).contains(studyPermission);
        }
        return hasStudyPermissions;
    }

    public static boolean checkUserHasPermission(Document document, String user, List<String> groups, String permission,
                                                 boolean defaultValue) {
        List<String> aclList = document.get(PRIVATE_ACL, ArrayList.class);
        Map<String, Set<String>> permissionMap = parsePermissions(aclList, user, groups);

        // 2.2. We now check if the user will have those effective permissions defined at the study level
        boolean hasPermission = defaultValue;
        if (permissionMap.get("user") != null) {
            hasPermission = permissionMap.get("user").contains(permission);
        } else if (permissionMap.get("group") != null) {
            hasPermission = permissionMap.get("group").contains(permission);
        } else if (permissionMap.get("members") != null) {
            hasPermission = permissionMap.get("members").contains(permission);
        } else if (permissionMap.get(REGISTERED_USERS) != null) {
            hasPermission = permissionMap.get(REGISTERED_USERS).contains(permission);
        } else if (permissionMap.get(ANONYMOUS) != null) {
            hasPermission = permissionMap.get(ANONYMOUS).contains(permission);
        }
        return hasPermission;
    }

    public static List<String> getAdminUsers(Document study) {
        List<Document> groupDocumentList = study.get(StudyDBAdaptor.QueryParams.GROUPS.key(), ArrayList.class);
        if (groupDocumentList != null && !groupDocumentList.isEmpty()) {
            for (Document group : groupDocumentList) {
                if ((ADMINS).equals(group.getString("id"))) {
                    return (List<String>) group.get("userIds", ArrayList.class);
                }
            }
        }
        return Collections.emptyList();
    }

    public static List<String> getGroups(Document study, String user) {
        List<Document> groupDocumentList = study.get(StudyDBAdaptor.QueryParams.GROUPS.key(), ArrayList.class);
        List<String> groups = new ArrayList<>();
        if (groupDocumentList != null && !groupDocumentList.isEmpty()) {
            for (Document group : groupDocumentList) {
                String groupName = group.getString("id");
                if (!groupName.equals(MEMBERS) && !groupName.equals(ADMINS)) {
                    List<String> userIds = group.get("userIds", ArrayList.class);
                    for (String userId : userIds) {
                        if (user.equals(userId)) {
                            groups.add(groupName);
                            break;
                        }
                    }
                }
            }
        }
        return groups;
    }

    public static Map<String, Set<String>> parsePermissions(List<String> permissionList, String user, List<String> groupList) {
        Map<String, Set<String>> permissions = new HashMap<>();

        if (permissionList != null) {
            // If _acl was not previously defined, it can be null the first time
            for (String memberPermission : permissionList) {
                String[] split = memberPermission.split(PERMISSION_DELIMITER, 2);
                String member = null;
                if (user.equals(split[0])) {
                    member = "user";
                } else if (groupList.contains(split[0])) {
                    member = "group";
                } else if (MEMBERS.equals(split[0])) {
                    member = "members";
                } else if (ANONYMOUS.equals(split[0])) {
                    member = ANONYMOUS;
                }
                if (member != null) {
                    if (!permissions.containsKey(member)) {
                        permissions.put(member, new HashSet<>());
                    }
                    if (!("NONE").equals(split[1])) {
                        permissions.get(member).add(split[1]);
                    }
                }
            }
        }

        return permissions;
    }

    /**
     * Creates a document with the corresponding query needed to retrieve results only from any authorised document.
     *
     * @param user                     User asking for the entries.
     * @param groups                   Group names where the user belongs to.
     * @param permission               Permission to be checked.
     * @param isRegisteredUsersPresent Boolean indicating whether a flag indicating "all registered users" has been registered in
     *                                 the @members group.
     * @param isAnonymousPresent       Boolean indicating whether the anonymous user has been registered in the @members group.
     * @param simplifyPermissionCheck  Flag indicating whether permission check can be simplified because permissions were never denied at
     *                                 any other entity level but study.
     * @return The document containing the query to be made in mongo database.
     */
    public static Document getAuthorisedEntries(String user, List<String> groups, String permission, boolean isRegisteredUsersPresent,
                                                boolean isAnonymousPresent, boolean simplifyPermissionCheck) {
        if (simplifyPermissionCheck) {
            return getSimplifiedPermissionCheck(user, groups, permission, isRegisteredUsersPresent, isAnonymousPresent);
        } else {
            return getComplexPermissionCheck(user, groups, permission, isRegisteredUsersPresent, isAnonymousPresent);
        }
    }

    /**
     * Creates a document with the corresponding query needed to retrieve results only from any authorised document.
     *
     * @param user                     User asking for the entries.
     * @param groups                   Group names where the user belongs to.
     * @param permission               Permission to be checked.
     * @param isRegisteredUsersPresent Boolean indicating whether a flag indicating "all registered users" has been registered in
     *                                 the @members group.
     * @param isAnonymousPresent       Boolean indicating whether the anonymous user has been registered in the @members group.
     * @return The document containing the query to be made in mongo database.
     */
    public static Document getSimplifiedPermissionCheck(String user, List<String> groups, String permission,
                                                        boolean isRegisteredUsersPresent, boolean isAnonymousPresent) {
        List<String> permissionList = new LinkedList<>();

        // Add current user
        permissionList.add(user + PERMISSION_DELIMITER + permission);

        // The rest of the queries would only be needed for registered users (not anonymous)
        if (!user.equals(ANONYMOUS)) {
            // Add groups
            if (groups != null && !groups.isEmpty()) {
                permissionList.addAll(groups
                        .stream()
                        .map(group -> group + PERMISSION_DELIMITER + permission)
                        .collect(Collectors.toList())
                );
                permissionList.add(MEMBERS + PERMISSION_DELIMITER + permission);
            }

            // Add any registered user
            if (isRegisteredUsersPresent) {
                permissionList.add(REGISTERED_USERS + PERMISSION_DELIMITER + permission);
            }

            // Add anonymous user
            if (isAnonymousPresent) {
                permissionList.add(ANONYMOUS + PERMISSION_DELIMITER + permission);
            }
        }

        return new Document(PRIVATE_ACL, new Document("$in", permissionList));
    }

    /**
     * Creates a document with the corresponding query needed to retrieve results only from any authorised document.
     *
     * @param user                     User asking for the entries.
     * @param groups                   Group names where the user belongs to.
     * @param permission               Permission to be checked.
     * @param isRegisteredUsersPresent Boolean indicating whether a flag indicating "all registered users" has been registered in
     *                                 the @members group.
     * @param isAnonymousPresent       Boolean indicating whether the anonymous user has been registered in the @members group.
     * @return The document containing the query to be made in mongo database.
     */
    public static Document getComplexPermissionCheck(String user, List<String> groups, String permission, boolean isRegisteredUsersPresent,
                                                     boolean isAnonymousPresent) {
        List<Document> queryList = new ArrayList<>();
        // 1. Check if the user has the permission
        queryList.add(new Document(PRIVATE_ACL, user + PERMISSION_DELIMITER + permission));

        // The rest of the queries would only be needed for registered users (not anonymous)
        if (!user.equals(ANONYMOUS)) {
            // This pattern list will contain patterns that should not match
            List<Pattern> patternList = new ArrayList<>();
            patternList.add(Pattern.compile("^" + user));

            // 2. Check if the groups have the permission (& not the user)
            if (groups != null && !groups.isEmpty()) {
                List<String> groupPermissionList = groups.stream().map(group -> group + PERMISSION_DELIMITER + permission)
                        .collect(Collectors.toList());
                queryList.add(new Document("$and",
                        Arrays.asList(
                                new Document(PRIVATE_ACL, new Document("$in", groupPermissionList)),
                                new Document(PRIVATE_ACL, new Document("$nin", patternList)))));

                // Add groups to pattern
                patternList = new ArrayList<>(patternList);
                patternList.addAll(groups.stream().map(group -> Pattern.compile("^" + group)).collect(Collectors.toList()));
            }

            // 3. Check if the @members group have the permission (& not the user & not the other groups)
            queryList.add(new Document("$and", Arrays.asList(
                    new Document(PRIVATE_ACL, "@members" + PERMISSION_DELIMITER + permission),
                    new Document(PRIVATE_ACL, new Document("$nin", patternList)))));

            patternList = new ArrayList<>(patternList);
            patternList.add(MEMBERS_PATTERN);

            if (isRegisteredUsersPresent) {
                // If flag for any registered user is not present in the study, this query will not be needed
                // 4. Check if the "any registed user" flag have the permission (& not the user & not the groups & not @members)
                queryList.add(new Document("$and", Arrays.asList(
                        new Document(PRIVATE_ACL, REGISTERED_USERS + PERMISSION_DELIMITER + permission),
                        new Document(PRIVATE_ACL, new Document("$nin", patternList)))));
                patternList = new ArrayList<>(patternList);
                patternList.add(REGISTERED_USERS_PATTERN);
            }

            if (isAnonymousPresent) {
                // If anonymous is not present in the study, this query will not be needed
                // 4. Check if the anonymous user have the permission (& not the user & not the groups & not @members)
                queryList.add(new Document("$and", Arrays.asList(
                        new Document(PRIVATE_ACL, ANONYMOUS + PERMISSION_DELIMITER + permission),
                        new Document(PRIVATE_ACL, new Document("$nin", patternList)))));
            }
        }

        return new Document("$or", queryList);
    }

    /**
     * Creates a document with the corresponding query needed to retrieve results only from documents where no permissions are assigned.
     *
     * @param user   User asking for the entries.
     * @param groups Group names where the user belongs to.
     * @return The document containing the query to be made in mongo database.
     */
    public static Document getNoPermissionsDefined(String user, List<String> groups) {
        List<Pattern> patternList = new ArrayList<>();

        if (!user.equals(ANONYMOUS)) {
            patternList.add(Pattern.compile("^" + user));
            if (groups != null && !groups.isEmpty()) {
                patternList.addAll(groups.stream().map(group -> Pattern.compile("^" + group)).collect(Collectors.toList()));
            }

            patternList.add(MEMBERS_PATTERN);
            patternList.add(REGISTERED_USERS_PATTERN);
        }
        patternList.add(ANONYMOUS_PATTERN);

        return new Document(PRIVATE_ACL, new Document("$nin", patternList));
    }

}
