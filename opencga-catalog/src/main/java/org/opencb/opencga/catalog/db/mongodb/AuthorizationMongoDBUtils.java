package org.opencb.opencga.catalog.db.mongodb;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 31/07/17.
 */
public class AuthorizationMongoDBUtils {

    static final String ADMIN = "admin";
    static final String PRIVATE_OWNER_ID = "_ownerId";
    private static final String PRIVATE_ACL = "_acl";
    private static final String VARIABLE_SETS = "variableSets";
    private static final String ANNOTATION_SETS = AnnotationMongoDBAdaptor.AnnotationSetParams.ANNOTATION_SETS.key();

    private static final String PERMISSION_DELIMITER = "__";

    private static final String ANONYMOUS = "*";

    private static final String MEMBERS = "@members";
    private static final String ADMINS = "@admins";

    private static final Pattern MEMBERS_PATTERN = Pattern.compile("^" + MEMBERS);
    private static final Pattern ANONYMOUS_PATTERN = Pattern.compile("^\\" + ANONYMOUS);

    public static boolean checkCanViewStudy(Document study, String user) {
        // 0. If the user corresponds with the owner, we don't have to check anything else
        if (study.getString(PRIVATE_OWNER_ID).equals(user)) {
            return true;
        }
        if (ADMIN.equals(user)) {
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
        if (ADMIN.equals(user) && checkAdminPermissions(studyPermission)) {
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
     * @param study study document.
     * @param entry Annotable document entry.
     * @param user user.
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
        if (ADMIN.equals(user) && checkAdminPermissions(studyPermission)) {
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
            entry.remove(ANNOTATION_SETS);
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

    public static Document getQueryForAuthorisedEntries(Document study, String user, String studyPermission, String entryPermission)
            throws CatalogAuthorizationException {
        // 0. If the user is the admin or corresponds with the owner, we don't have to check anything else
        if (study.getString(PRIVATE_OWNER_ID).equals(user)) {
            return new Document();
        }
        if (ADMIN.equals(user) && checkAdminPermissions(studyPermission)) {
            return new Document();
        }
        if (getAdminUsers(study).contains(user)) {
            return new Document();
        }

        // If user does not exist in the members group, the user will not have any permission
        if (!isUserInMembers(study, user)) {
            throw new CatalogAuthorizationException("User " + user + " does not have any permissions in study "
                    + study.getString(StudyDBAdaptor.QueryParams.ALIAS.key()));
        }

        boolean isAnonymousPresent = false;
        List<String> groups;
        boolean hasStudyPermissions;
        if (!user.equals(ANONYMOUS)) {
            // 0. Check if anonymous has any permission defined (just for performance)
            isAnonymousPresent = isUserInMembers(study, ANONYMOUS);

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
        Document queryDocument = getAuthorisedEntries(user, groups, entryPermission, isAnonymousPresent);
        if (hasStudyPermissions) {
            // The user has permissions defined globally, so we also have to check the entries where the user/groups/members/* have no
            // permissions defined as the user will also be allowed to see them
            queryDocument = new Document("$or", Arrays.asList(
                    getNoPermissionsDefined(user, groups),
                    queryDocument
            ));
        }

        return queryDocument;
    }

    public static boolean isUserInMembers(Document study, String user) {
        List<Document> groupDocumentList = study.get(StudyDBAdaptor.QueryParams.GROUPS.key(), ArrayList.class);
        if (groupDocumentList != null && !groupDocumentList.isEmpty()) {
            for (Document group : groupDocumentList) {
                if ((MEMBERS).equals(group.getString("name"))) {
                    List<String> userIds = group.get("userIds", ArrayList.class);
                    for (String userId : userIds) {
                        if (userId.equals(user)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    public static boolean checkAnonymousHasPermission(Document study, String studyPermission) {
        List<String> aclList = study.get(PRIVATE_ACL, ArrayList.class);
        Map<String, Set<String>> permissionMap = parsePermissions(aclList, "*", Collections.emptyList());

        // We now check if the anonymous user has the permission defined at the study level
        boolean hasStudyPermissions = false;
        if (permissionMap.get("user") != null) {
            hasStudyPermissions = permissionMap.get("user").contains(studyPermission);
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
        } else if (permissionMap.get(ANONYMOUS) != null) {
            hasPermission = permissionMap.get(ANONYMOUS).contains(permission);
        }
        return hasPermission;
    }

    public static List<String> getAdminUsers(Document study) {
        List<Document> groupDocumentList = study.get(StudyDBAdaptor.QueryParams.GROUPS.key(), ArrayList.class);
        if (groupDocumentList != null && !groupDocumentList.isEmpty()) {
            for (Document group : groupDocumentList) {
                if ((ADMINS).equals(group.getString("name"))) {
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
                String groupName = group.getString("name");
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
                String[] split = StringUtils.split(memberPermission, PERMISSION_DELIMITER, 2);
                String member = null;
                if (user.equals(split[0])) {
                    member = "user";
                } else if (groupList.contains(split[0])) {
                    member = "group";
                } else if (MEMBERS.equals(split[0])) {
                    member = "members";
                } else if ("*".equals(split[0])) {
                    member = "*";
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
     * @param user User asking for the entries.
     * @param groups Group names where the user belongs to.
     * @param permission Permission to be checked.
     * @param isAnonymousPresent Boolean indicating whether the anonymous user has been registered in the @members group.
     * @return The document containing the query to be made in mongo database.
     */
    public static Document getAuthorisedEntries(String user, List<String> groups, String permission, boolean isAnonymousPresent) {
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

            if (isAnonymousPresent) {
                // If anonymous is not present in the study, this query will not be needed
                // 4. Check if the anonymous user have the permission (& not the user & not the groups & not @members)
                patternList = new ArrayList<>(patternList);
                patternList.add(MEMBERS_PATTERN);
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
     * @param user User asking for the entries.
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
        }
        patternList.add(ANONYMOUS_PATTERN);

        return new Document(PRIVATE_ACL, new Document("$nin", patternList));
    }

    private static boolean checkAdminPermissions(String studyPermission) {
        Set<String> adminPermissions = CatalogAuthorizationManager.getSpecialPermissions(ADMIN).getPermissions()
                .stream()
                .map(String::valueOf)
                .collect(Collectors.toSet());
        if (adminPermissions.contains(studyPermission)) {
            return true;
        }
        return false;
    }


}
