package org.opencb.opencga.catalog.authorization.old;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.utils.ParamUtils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationManager implements AuthorizationManager {
    private static final QueryOptions FILE_INCLUDE_QUERY_OPTIONS = new QueryOptions("include",
            Arrays.asList("projects.studies.files.id", "projects.studies.files.path", "projects.studies.files.acls"));
    private final CatalogUserDBAdaptor userDBAdaptor;
    private final CatalogProjectDBAdaptor projectDBAdaptor;
    private final CatalogStudyDBAdaptor studyDBAdaptor;
    private final CatalogFileDBAdaptor fileDBAdaptor;
    private final CatalogJobDBAdaptor jobDBAdaptor;
    private final CatalogSampleDBAdaptor sampleDBAdaptor;
    private final CatalogIndividualDBAdaptor individualDBAdaptor;
    private final CatalogCohortDBAdaptor cohortDBAdaptor;
    private final AuditManager auditManager;

    public CatalogAuthorizationManager(CatalogDBAdaptorFactory catalogDBAdaptorFactory, AuditManager auditManager) {
        this.auditManager = auditManager;
        userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        projectDBAdaptor = catalogDBAdaptorFactory.getCatalogProjectDbAdaptor();
        studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
        cohortDBAdaptor = catalogDBAdaptorFactory.getCatalogCohortDBAdaptor();
    }

    @Override
    public User.Role getUserRole(String userId) throws CatalogException {
        return userDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).first().getRole();
    }

    @Override
    public void checkProjectPermission(long projectId, String userId, CatalogPermission permission) throws CatalogException {

        if (projectDBAdaptor.getProjectOwnerId(projectId).equals(userId)) {
            return;
        }

        if (permission.equals(CatalogPermission.READ)) {
            final Query query = new Query(CatalogStudyDBAdaptor.QueryParams.PROJECT_ID.key(), projectId);
            final QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, "projects.studies.id");
            for (Study study : studyDBAdaptor.get(query, queryOptions).getResult()) {
                try {
                    checkStudyPermission(study.getId(), userId, StudyPermission.READ_STUDY);
                    return; //Return if can read some study
                } catch (CatalogException e) {
                    e.printStackTrace();
                }
            }
        }

        throw CatalogAuthorizationException.deny(userId, permission.toString(), "Project", projectId, null);
    }

    /*
     * Check permission methods
     */
    @Override
    public void checkStudyPermission(long studyId, String userId, StudyPermission permission) throws CatalogException {
        checkStudyPermission(studyId, userId, permission, permission.toString());
    }

    @Override
    public void checkStudyPermission(long studyId, String userId, StudyPermission permission, String message) throws CatalogException {
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        Group group = getGroupBelonging(studyId, userId);
        String groupId = null;
        if (group != null) {
            groupId = "@" + group.getId();
        }
        Role role = getRoleBelonging(studyId, userId, groupId);
        if (role == null) {
            throw CatalogAuthorizationException.deny(userId, message, "Study", studyId, null);
        }

        final boolean auth;
        switch (permission) {
            case DELETE_JOBS:
                auth = role.getPermissions().isDeleteJobs();
                break;
            case LAUNCH_JOBS:
                auth = role.getPermissions().isLaunchJobs();
                break;
            case MANAGE_SAMPLES:
                auth = role.getPermissions().isManagerSamples();
                break;
            case MANAGE_STUDY:
                auth = role.getPermissions().isStudyManager();
                break;
            case READ_STUDY:
                auth = true; //Authorize if belongs to any role
                break;
            default:
                auth = false;
                break;
        }
        if (!auth) {
            throw CatalogAuthorizationException.deny(userId, message, "Study", studyId, null);
        }
    }

    @Override
    public void checkFilePermission(long fileId, String userId, CatalogPermission permission) throws CatalogException {
        checkFilePermission(fileId, userId, permission, null);
    }

    private void checkFilePermission(long fileId, String userId, CatalogPermission permission,
                                     StudyAuthenticationContext studyAuthenticationContext) throws CatalogException {
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        if (studyAuthenticationContext == null) {
            studyAuthenticationContext = new StudyAuthenticationContext(studyId);
        }
        AclEntry fileAcl = resolveFileAcl(fileId, userId, studyId, studyAuthenticationContext);

        if (!isAuth(permission, fileAcl)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "File", fileId, null);
        }
    }

    @Override
    public void checkSamplePermission(long sampleId, String userId, CatalogPermission permission) throws CatalogException {
        long studyId = sampleDBAdaptor.getStudyIdBySampleId(sampleId);
        if (isStudyOwner(studyId, userId)) {
            return;
        }

        AclEntry sampleAcl = resolveSampleAcl(studyId, sampleId, userId);

        if (!isAuth(permission, sampleAcl)) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "Sample", sampleId, null);
        }
    }

    @Override
    public void checkIndividualPermission(long individualId, String userId, CatalogPermission permission) throws CatalogException {
        long studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        if (isStudyOwner(studyId, userId)) {  //User admin or owner
            return;
        }

        Group group = getGroupBelonging(studyId, userId);
        String groupId = null;
        if (group != null) {
            groupId = "@" + group.getId();
        }

        final boolean auth;
        Role role = getRoleBelonging(studyId, userId, groupId);
        if (role == null) {    //User not in study
            auth = false;
        } else if (role.getPermissions().isManagerSamples()) {
            auth = true;
        } else {
            switch (permission) {
                case READ:
                    List<Sample> samples = sampleDBAdaptor.get(new Query(CatalogSampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(),
                            individualId), new QueryOptions()).getResult();
                    filterSamples(userId, studyId, samples);
                    auth = !samples.isEmpty();
                    break;
                default:
                    auth = false;
                    break;
            }
        }
        if (!auth) {
            throw CatalogAuthorizationException.deny(userId, permission.toString(), "Individual", individualId, null);
        }
    }

    /**
     * This method don't check if the user {@link #isAdmin(String)} or {@link #isStudyOwner(int, String)}.
     */
    private AclEntry resolveFileAcl(long fileId, String userId, long studyId,
                                    StudyAuthenticationContext studyAuthenticationContext) throws CatalogException {
        return resolveFileAcl(fileDBAdaptor.getFile(fileId, FILE_INCLUDE_QUERY_OPTIONS).first(), userId, studyId,
                studyAuthenticationContext);
    }

    /**
     * Resolves the permissions between a sample and a user.
     * This method don't check if the user {@link #isAdmin(String)} or {@link #isStudyOwner(int, String)}
     *
     * @param file Must contain id, acl and path. Get file with with {@link #FILE_INCLUDE_QUERY_OPTIONS}
     */
    private AclEntry resolveFileAcl(File file, String userId, long studyId,
                                    StudyAuthenticationContext studyAuthenticationContext) throws CatalogException {
        Group group = getGroupBelonging(studyId, userId);

        String groupId = null;
        if (group != null) {
            groupId = "@" + group.getId();
        }
        // Look for a role where the userId or the group (if any) is present
        Role role = getRoleBelonging(studyId, userId, groupId);
        AclEntry studyAcl = getStudyACL(userId, role);
        AclEntry fileAcl = null;

        // We obtain all the paths from our current position to the root folder
        List<String> paths = FileManager.getParentPaths(file.getPath());
        // We obtain a map with the ACL entries
        Map<String, Map<String, AclEntry>> pathAclMap = getFileAclEntries(studyAuthenticationContext, userId, studyId, groupId, paths);

        // TODO: Talk about this with Nacho.
        for (int i = paths.size() - 1; i >= 0; i--) {
            String path = paths.get(i);
            if (pathAclMap.containsKey(path)) {
                Map<String, AclEntry> aclMap = pathAclMap.get(path);
                //Get first the user AclEntry
                if (aclMap.get(userId) != null) {
                    fileAcl = aclMap.get(userId);
                } else if (aclMap.get(groupId) != null) {
                    fileAcl = aclMap.get(groupId);
                } else if (aclMap.get(AclEntry.USER_OTHERS_ID) != null) {
                    fileAcl = aclMap.get(AclEntry.USER_OTHERS_ID);
                }
                if (fileAcl != null) {
                    break;
                }
            }
        }

        if (fileAcl == null) {
            fileAcl = studyAcl;
        }
        return fileAcl;
    }

    private Map<String, Map<String, AclEntry>> getFileAclEntries(StudyAuthenticationContext studyAuthenticationContext, String userId,
                                                                 List<File> files) throws CatalogException {
        Group group = getGroupBelonging(studyAuthenticationContext.studyId, userId);
        Set<String> paths = new HashSet<>();
        for (File file : files) {
            paths.addAll(FileManager.getParentPaths(file.getPath()));
        }
//        System.out.println("Querying for " + paths.size() + " paths to the authContext");
        return getFileAclEntries(studyAuthenticationContext, userId, studyAuthenticationContext.studyId, "@" + group.getId(),
                new ArrayList<>(paths));
    }

    /**
     * Returns the AclEntries for the user, group (if any) and {@link AclEntry#USER_OTHERS_ID}.
     *
     * @param studyAuthenticationContext Context with already fetched elements. Will avoid fetch extra data
     * @param userId                     User id
     * @param studyId                    Study identifier
     * @param groupId                    User belonging group. May be null.
     * @param paths                      List of paths to check
     * @return Map (Path -> Map (UserId -> AclEntry) )
     * @throws CatalogDBException
     */
    private Map<String, Map<String, AclEntry>> getFileAclEntries(StudyAuthenticationContext studyAuthenticationContext, String userId,
                                                                 long studyId, String groupId, List<String> paths)
            throws CatalogDBException {
        return null;
//        for (Iterator<String> iterator = paths.iterator(); iterator.hasNext();) {
//            String path = iterator.next();
//            if (studyAuthenticationContext.pathUserAclMap.containsKey(path)) {
//                Map<String, AclEntry> userAclMap = studyAuthenticationContext.pathUserAclMap.get(path);
//                if (userAclMap.containsKey(userId) && (groupId == null || userAclMap.containsKey(groupId))
//                        && userAclMap.containsKey(AclEntry.USER_OTHERS_ID)) {
//                    iterator.remove();
//                }
//            }
//        }
//        if (!paths.isEmpty()) {
//            // We make a query to obtain the ACLs of all the paths for userId, groupId and *
//            List<String> userIds = (groupId == null)
//                    ? Arrays.asList(userId, AclEntry.USER_OTHERS_ID)
//                    : Arrays.asList(userId, groupId, AclEntry.USER_OTHERS_ID);
//            Map<String, Map<String, AclEntry>> map = fileDBAdaptor.getFilesAcl(studyId, paths, userIds).first();
//            for (String path : paths) {
//                Map<String, AclEntry> stringAclEntryMap;
//                if (map.containsKey(path)) {
//                    stringAclEntryMap = map.get(path);
//                } else {
//                    stringAclEntryMap = new HashMap<>();
//                }
//                stringAclEntryMap.putIfAbsent(userId, null);
//                if (groupId != null) {
//                    stringAclEntryMap.putIfAbsent(groupId, null);
//                }
//                stringAclEntryMap.putIfAbsent(AclEntry.USER_OTHERS_ID, null);
//
//                if (studyAuthenticationContext.pathUserAclMap.containsKey(path)) {
//                    studyAuthenticationContext.pathUserAclMap.get(path).putAll(stringAclEntryMap);
//                } else {
//                    studyAuthenticationContext.pathUserAclMap.put(path, stringAclEntryMap);
//                }
//            }
//        }
//
//        return studyAuthenticationContext.pathUserAclMap;
    }

    /**
     * Resolves the permissions between a sample and a user.
     * Returns the most specific matching ACL following the next sequence:
     * user > group > others > study
     * This method don't check if the user {@link #isAdmin(String)} or {@link #isStudyOwner(int, String)}
     *
     *
     * @param studyId
     * @throws CatalogException
     */
    private AclEntry resolveSampleAcl(long studyId, Sample sample, String userId) throws CatalogException {
        if (sample.getAcls() == null) {
            return resolveSampleAcl(studyId, sample.getId(), userId);
        } else {
            Group group = getGroupBelonging(studyId, userId);
            String groupId = null;
            if (group != null) {
                groupId = "@" + group.getId();
            }

            Role role = getRoleBelonging(studyId, userId, groupId);

            // TODO: Uncomment
//            Map<String, AclEntry> userAclMap = sample.getAcls().stream().collect(Collectors.toMap(AclEntry::getUserId, e -> e));
//            return resolveSampleAcl(userId, groupId, role, userAclMap);
            return null;
        }
    }

    /**
     * Resolves the permissions between a sample and a user.
     * Returns the most specific matching ACL following the next sequence:
     * user > group > others > study
     * This method don't check if the user {@link #isAdmin(String)} or {@link #isStudyOwner(int, String)}
     *
     *
     * @param studyId
     * @throws CatalogException
     */
    private AclEntry resolveSampleAcl(long studyId, long sampleId, String userId) throws CatalogException {
        Group group = getGroupBelonging(studyId, userId);
        String groupId = null;
        if (group != null) {
            groupId = "@" + group.getId();
        }

        // TODO: Uncomment
//        List<String> userIds = (groupId == null)
//                ? Arrays.asList(userId, AclEntry.USER_OTHERS_ID)
//                : Arrays.asList(userId, groupId, AclEntry.USER_OTHERS_ID);
//        Map<String, AclEntry> userAclMap = sampleDBAdaptor.getSampleAcl(sampleId, userIds).first();
//
//        Role role = getRoleBelonging(studyId, userId, groupId);
//
//        return resolveSampleAcl(userId, groupId, role, userAclMap);
        return null;
    }

    private AclEntry resolveSampleAcl(String userId, String groupId, Role role, Map<String, AclEntry> userAclMap) {
        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (groupId != null && userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(AclEntry.USER_OTHERS_ID)) {
            return userAclMap.get(AclEntry.USER_OTHERS_ID);
        } else {
            return getStudyACL(userId, role);
        }
    }

    private AclEntry getStudyACL(String userId, Role role) {
        if (role == null) {
            return new AclEntry(userId, false, false, false, false);
        } else {
            return new AclEntry(userId, role.getPermissions().isRead(), role.getPermissions().isWrite(), false, role.getPermissions()
                    .isDelete());
        }
    }

    /*
     * ACL Management methods
     */

    @Deprecated
    @Override
    public QueryResult setFileACL(String fileIds, String userIds, AclEntry acl, String sessionId) throws CatalogException {
        return null;
//        long startTime = System.currentTimeMillis();
//        ParamUtils.checkObj(acl, "acl");
//        ParamUtils.checkParameter(sessionId, "sessionId");
//
//        String userSessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        List<Long> studyIdsBySampleIds = fileDBAdaptor.getStudyIdsByFileIds(fileIds);
//        for (Long studyId : studyIdsBySampleIds) {
//            checkStudyPermission(studyId, userSessionId, StudyPermission.MANAGE_STUDY);
//        }
//
//        List<AclEntry> aclEntries = new ArrayList<>();
//
//        String[] fileIdArray = fileIds.split(",");
//        String[] userIdArray = userIds.split(",");
//
//        for (String userId : userIdArray) {
//            acl.setUserId(userId);
//            for (String fileIdValue : fileIdArray) {
//                int fileId = Integer.valueOf(fileIdValue);
//                aclEntries.add(fileDBAdaptor.setFileAcl(fileId, acl).first());
//                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userSessionId, new ObjectMap("acl", acl), "setAcls", null);
//            }
//        }
//
//        return new QueryResult<>("Set File ACL", (int) (System.currentTimeMillis() - startTime), aclEntries.size(), aclEntries.size(),
//                "", "", aclEntries);
    }

    @Override
    public QueryResult unsetFileACL(String fileIds, String userIds, String sessionId) throws CatalogException {
        return null;
//        long startTime = System.currentTimeMillis();
//        ParamUtils.checkParameter(sessionId, "sessionId");
//        ParamUtils.checkParameter(userIds, "userId");
//
//        String userSessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        List<Long> studyIdsBySampleIds = fileDBAdaptor.getStudyIdsByFileIds(fileIds);
//        for (Long studyId : studyIdsBySampleIds) {
//            checkStudyPermission(studyId, userSessionId, StudyPermission.MANAGE_STUDY);
//        }
//
//        List<AclEntry> aclEntries = new ArrayList<>();
//
//        String[] fileIdArray = fileIds.split(",");
//        String[] userIdArray = userIds.split(",");
//
//        for (String userId : userIdArray) {
//            for (String fileIdValue : fileIdArray) {
//                int fileId = Integer.valueOf(fileIdValue);
//                aclEntries.add(fileDBAdaptor.unsetFileAcl(fileId, userId).first());
//                auditManager.recordAction(AuditRecord.Resource.file, AuditRecord.UPDATE, fileId, userId, new ObjectMap("acl",
//                        aclEntries.get(aclEntries.size() - 1)), null, "unsetAcl", null);
//
//            }
//        }
//
//        return new QueryResult<>("Unset File ACL", (int) (System.currentTimeMillis() - startTime), aclEntries.size(), aclEntries.size(),
//                "", "", aclEntries);
    }

    @Deprecated
    @Override
    public QueryResult<AclEntry> setSampleACL(String sampleIds, String userIds, AclEntry acl, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userSessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
        List<Long> studyIdsBySampleIds = sampleDBAdaptor.getStudyIdsBySampleIds(sampleIds);
        for (Long studyId : studyIdsBySampleIds) {
            checkStudyPermission(studyId, userSessionId, StudyPermission.MANAGE_STUDY);
        }

        List<AclEntry> aclEntries = new ArrayList<>();

        String[] sampleIdArray = sampleIds.split(",");
        String[] userIdArray = userIds.split(",");

        // FIXME: Check audit.
        for (String userId : userIdArray) {
            acl.setUserId(userId);
            for (String sampleIdValue : sampleIdArray) {
                int sampleId = Integer.valueOf(sampleIdValue);
                aclEntries.add(sampleDBAdaptor.setSampleAcl(sampleId, acl).first());
//              auditManager.recordUpdate(AuditRecord.Resource.sample, sampleId, userSessionId, new ObjectMap("acl", acl), "setAcls", null);
            }
        }

        return new QueryResult<>("Set Sample ACL", (int) (System.currentTimeMillis() - startTime), aclEntries.size(), aclEntries.size(),
                "", "", aclEntries);
    }

    @Override
    public QueryResult unsetSampleACL(String sampleIds, String userIds, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(userIds, "userId");

        String userSessionId = userDBAdaptor.getUserIdBySessionId(sessionId);
        List<Long> studyIdsBySampleIds = sampleDBAdaptor.getStudyIdsBySampleIds(sampleIds);
        for (Long studyId : studyIdsBySampleIds) {
            checkStudyPermission(studyId, userSessionId, StudyPermission.MANAGE_STUDY);
        }

        List<AclEntry> aclEntries = new ArrayList<>();

        String[] sampleIdArray = sampleIds.split(",");
        String[] userIdArray = userIds.split(",");

        for (String userId : userIdArray) {
            for (String sampleIdValue : sampleIdArray) {
                int sampleId = Integer.valueOf(sampleIdValue);
                aclEntries.add(sampleDBAdaptor.unsetSampleAcl(sampleId, userId).first());
//                auditManager.recordAction(AuditRecord.Resource.sample, AuditRecord.UPDATE, sampleId, userId, new ObjectMap("acl",
//                        aclEntries.get(aclEntries.size() - 1)), null, "unsetAcl", null);

            }
        }

        return new QueryResult<>("Unset Sample ACL", (int) (System.currentTimeMillis() - startTime), aclEntries.size(), aclEntries.size(),
                "", "", aclEntries);
    }

    @Override
    public void filterProjects(String userId, List<Project> projects) throws CatalogException {
        if (projects == null || projects.isEmpty()) {
            return;
        }
        if (isAdmin(userId)) {
            return;
        }
        Iterator<Project> projectIt = projects.iterator();
        while (projectIt.hasNext()) {
            Project p = projectIt.next();
            try {
                checkProjectPermission(p.getId(), userId, CatalogPermission.READ);
            } catch (CatalogAuthorizationException e) {
                projectIt.remove();
                continue;
            }
            filterStudies(userId, p.getStudies());
        }
    }

    /*
     * Filter methods.
     * Given a list of elements, remove those that the user can't read
     */

    @Override
    public void filterStudies(String userId, List<Study> studies) throws CatalogException {
        if (studies == null || studies.isEmpty()) {
            return;
        }
        if (isAdmin(userId)) {
            return;
        }
        Iterator<Study> studyIt = studies.iterator();
        while (studyIt.hasNext()) {
            Study study = studyIt.next();
            try {
                checkStudyPermission(study.getId(), userId, StudyPermission.READ_STUDY);
            } catch (CatalogAuthorizationException e) {
                studyIt.remove();
                continue;
            }
            StudyAuthenticationContext studyAuthenticationContext = new StudyAuthenticationContext(study.getId());
            List<File> files = study.getFiles();
            filterFiles(userId, study.getId(), files, studyAuthenticationContext);
            filterSamples(userId, study.getId(), study.getSamples());
            filterJobs(userId, study.getJobs(), studyAuthenticationContext);
            filterCohorts(userId, study.getId(), study.getCohorts());
            filterIndividuals(userId, study.getId(), study.getIndividuals());
        }
    }

    @Override
    public void filterFiles(String userId, long studyId, List<File> files) throws CatalogException {
        filterFiles(userId, studyId, files, new StudyAuthenticationContext(studyId));
    }

    private void filterFiles(String userId, long studyId, List<File> files, StudyAuthenticationContext
            studyAuthenticationContext) throws CatalogException {
        if (files == null || files.isEmpty()) {
            return;
        }
        if (isAdmin(userId) || isStudyOwner(studyId, userId)) {
            return;
        }

        getFileAclEntries(studyAuthenticationContext, userId, files);

        Iterator<File> fileIt = files.iterator();
        while (fileIt.hasNext()) {
            File file = fileIt.next();
            if (!resolveFileAcl(file, userId, studyId, studyAuthenticationContext).isRead()) {
                fileIt.remove();
            }
        }
    }

    @Override
    public void filterSamples(String userId, long studyId, List<Sample> samples) throws CatalogException {
        if (samples == null || samples.isEmpty()) {
            return;
        }
        if (isAdmin(userId) || isStudyOwner(studyId, userId)) {
            return;
        }

        Iterator<Sample> sampleIterator = samples.iterator();
        while (sampleIterator.hasNext()) {
            Sample sample = sampleIterator.next();
            AclEntry sampleACL = resolveSampleAcl(studyId, sample, userId);
            if (!sampleACL.isRead()) {
                sampleIterator.remove();
            }
        }
    }

    @Override
    public void filterJobs(String userId, List<Job> jobs) throws CatalogException {
        filterJobs(userId, jobs, (StudyAuthenticationContext) null);
    }

    @Override
    public void filterJobs(String userId, List<Job> jobs, Long studyId) throws CatalogException {
        filterJobs(userId, jobs, studyId == null ? null : new StudyAuthenticationContext(studyId));
    }

    public void filterJobs(String userId, List<Job> jobs, StudyAuthenticationContext studyAuthenticationContext) throws CatalogException {
        if (jobs == null || jobs.isEmpty()) {
            return;
        }
        if (isAdmin(userId)) {
            return;
        }
        if (studyAuthenticationContext != null) {
            if (isStudyOwner(studyAuthenticationContext.studyId, userId)) {
                return;
            }
        }
        Map<Long, StudyAuthenticationContext> studyAuthContextMap = new HashMap<>();
        if (studyAuthenticationContext != null) {
            studyAuthContextMap.put(studyAuthenticationContext.studyId, studyAuthenticationContext);
        }

        for (Iterator<Job> iterator = jobs.iterator(); iterator.hasNext();) {
            Job job = iterator.next();
            long studyId;
            StudyAuthenticationContext specificStudyAuthenticationContext;
            if (studyAuthenticationContext == null) {
                studyId = jobDBAdaptor.getStudyIdByJobId(job.getId());
                specificStudyAuthenticationContext = studyAuthContextMap.getOrDefault(studyId, new StudyAuthenticationContext(studyId));
                if (isStudyOwner(studyId, userId)) {
                    continue;
                }
            } else {
                specificStudyAuthenticationContext = studyAuthenticationContext;
                studyId = studyAuthenticationContext.studyId;
            }

            if (job.getOutput() == null || job.getInput() == null) {
                job = fetchJob(job.getId());
            }
            List<File> result = fetchJobFiles(job);
            if (result.isEmpty()) {
                continue;
            }
            getFileAclEntries(specificStudyAuthenticationContext, userId, result);
            Map<Long, File> fileIdMap = result.stream().collect(Collectors.toMap(File::getId, f -> f));

            boolean removed = false;
            for (Long fileId : job.getOutput()) {
                if (!resolveFileAcl(fileIdMap.get(fileId), userId, studyId, specificStudyAuthenticationContext).isRead()) {
                    iterator.remove();
                    removed = true;
                    break;
                }
            }
            if (!removed) {
                for (Long fileId : job.getInput()) {
                    if (!resolveFileAcl(fileIdMap.get(fileId), userId, studyId, specificStudyAuthenticationContext).isRead()) {
                        iterator.remove();
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void filterCohorts(String userId, long studyId, List<Cohort> cohorts) throws CatalogException {
        if (cohorts == null || cohorts.isEmpty()) {
            return;
        }
        if (isAdmin(userId) || isStudyOwner(studyId, userId)) {
            return;
        }

        Map<Long, AclEntry> sampleAclMap = new HashMap<>();

        for (Iterator<Cohort> iterator = cohorts.iterator(); iterator.hasNext();) {
            Cohort cohort = iterator.next();
            for (Long sampleId : cohort.getSamples()) {
                AclEntry sampleACL;
                if (sampleAclMap.containsKey(sampleId)) {
                    sampleACL = sampleAclMap.get(sampleId);
                } else {
                    sampleACL = resolveSampleAcl(studyId, sampleId, userId);
                    sampleAclMap.put(sampleId, sampleACL);
                }
                if (!sampleACL.isRead()) {
                    iterator.remove();  //Remove cohort.
                    break;              //Stop checking cohort
                }
            }
        }
    }

    @Override
    public void filterIndividuals(String userId, long studyId, List<Individual> individuals) throws CatalogException {
        if (individuals == null || individuals.isEmpty()) {
            return;
        }
        if (isAdmin(userId) || isStudyOwner(studyId, userId)) {
            return;
        }

        for (Iterator<Individual> iterator = individuals.iterator(); iterator.hasNext();) {
            Individual individual = iterator.next();
            try {
                checkIndividualPermission(individual.getId(), userId, CatalogPermission.READ);
            } catch (CatalogAuthorizationException e) {
                iterator.remove();
            }
        }
    }

    @Override
    public void checkReadJob(String userId, long jobId) throws CatalogException {
        privateCheckReadJob(userId, fetchJob(jobId));
    }

    @Override
    public void checkReadJob(String userId, Job job) throws CatalogException {
        //If the given job does not contain the required fields, query Catalog
        //Only non READY or ERROR jobs can have null output files
        if ((job.getOutput() == null
                && (job.getStatus().getStatus() == null || !job.getStatus().getStatus().equals(Job.JobStatus.READY)
                || !job.getStatus().getStatus().equals(Job.JobStatus.ERROR))) || job.getInput() == null) {
            job = fetchJob(job.getId());
        }
        privateCheckReadJob(userId, job);
    }

    private void privateCheckReadJob(String userId, Job job) throws CatalogException {
        if (isAdmin(userId)) {
            return;
        }
        long studyId = jobDBAdaptor.getStudyIdByJobId(job.getId());

        if (isStudyOwner(studyId, userId)) {
            return;
        }

        StudyAuthenticationContext context = new StudyAuthenticationContext(studyId);

        //Make all the required queries once
        List<File> files = fetchJobFiles(job);

        getFileAclEntries(context, userId, files);
        try {
            if (job.getOutput() != null) {
                for (Long fileId : job.getOutput()) {
                    checkFilePermission(fileId, userId, CatalogPermission.READ, context);
                }
            }
            for (Long fileId : job.getInput()) {
                checkFilePermission(fileId, userId, CatalogPermission.READ, context);
            }
        } catch (CatalogAuthorizationException e) {
            throw CatalogAuthorizationException.cantRead(userId, "Job", job.getId(), job.getName());
        }
    }

    @Override
    public void checkReadCohort(String userId, Cohort cohort) throws CatalogException {
        try {
            if (cohort.getSamples() == null) {
                cohort = cohortDBAdaptor.getCohort(cohort.getId(), new QueryOptions()).first();
            }
            for (Long sampleId : cohort.getSamples()) {
                checkSamplePermission(sampleId, userId, CatalogPermission.READ);
            }
        } catch (CatalogAuthorizationException e) {
            throw CatalogAuthorizationException.cantRead(userId, "Cohort", cohort.getId(), cohort.getName());
        }
    }

    @Override
    public Group getGroupBelonging(long studyId, String userId) throws CatalogException {
        QueryResult<Group> queryResult = studyDBAdaptor.getGroup(studyId, userId, null, null);
        return queryResult.getNumResults() == 0 ? null : queryResult.first();
    }

    @Override
    public Role getRoleBelonging(long studyId, String userId, String groupId) throws CatalogException {
        QueryResult<Role> queryResult = studyDBAdaptor.getRole(studyId, userId, groupId, null, null);
        return queryResult.getNumResults() == 0 ? null : queryResult.first();
    }


    /*
     * Group management methods
     */

    @Deprecated
    @Override
    public QueryResult<Group> addMember(long studyId, String groupId, String userIdToAdd, String sessionId) throws CatalogException {
return null;
//        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        checkStudyPermission(studyId, userId, StudyPermission.MANAGE_STUDY);
//
//        Group groupFromUserToAdd = getGroupBelonging(studyId, userIdToAdd);
//        if (groupFromUserToAdd != null) {
//            throw new CatalogException("User \"" + userIdToAdd + "\" already belongs to group " + groupFromUserToAdd.getId());
//        }
//
//        QueryResult<Group> queryResult = studyDBAdaptor.addMemberToGroup(studyId, groupId, userIdToAdd);
//        ObjectMap after = new ObjectMap("groups", new ObjectMap("userIds", Collections.singletonList(userIdToAdd)).append("id", groupId));
//        auditManager.recordAction(AuditRecord.Resource.study, AuditRecord.UPDATE, studyId, userId, null, after, "addMember", null);
//        return queryResult;
    }

    @Deprecated
    @Override
    public QueryResult<Group> removeMember(long studyId, String groupId, String userIdToRemove, String sessionId) throws CatalogException {
return null;
//        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        checkStudyPermission(studyId, userId, StudyPermission.MANAGE_STUDY);
//
//        Group groupFromUserToRemove = getGroupBelonging(studyId, userIdToRemove);
//        if (groupFromUserToRemove == null || !groupFromUserToRemove.getId().equals(groupId)) {
//            throw new CatalogException("User \"" + userIdToRemove + "\" does not belongs to group " + groupId);
//        }
//
//        QueryResult<Group> queryResult = studyDBAdaptor.removeMemberFromGroup(studyId, groupId, userIdToRemove);
//        ObjectMap before = new ObjectMap("groups", new ObjectMap("userIds", Collections.singletonList(userIdToRemove)).append("id",
//                groupId));
//        auditManager.recordAction(AuditRecord.Resource.study, AuditRecord.UPDATE, studyId, userId, before, null, "addMember", null);
//        return queryResult;
    }

    //TODO: Cache this?
    private boolean isStudyOwner(long studyId, String userId) throws CatalogDBException {
//        return userDBAdaptor.getProjectOwnerId(studyDBAdaptor.getProjectIdByStudyId(studyId)).equals(userId);
        return projectDBAdaptor.getProjectOwnerId(studyDBAdaptor.getProjectIdByStudyId(studyId)).equals(userId);
    }


    /*
     * Helper methods
     */

    //TODO: Cache this?
    private boolean isAdmin(String userId) throws CatalogException {
        return getUserRole(userId).equals(User.Role.ADMIN);
    }

    private boolean isAuth(CatalogPermission permission, AclEntry acl) {
        final boolean auth;
        switch (permission) {
            case READ:
                auth = acl.isRead();
                break;
            case WRITE:
                auth = acl.isWrite();
                break;
            case DELETE:
                auth = acl.isDelete();
                break;
            default:
                auth = false;
                break;
        }
        return auth;
    }

    public List<Long> getFileIds(Job job) {
        List<Long> fileIds = new ArrayList<>(job.getOutput().size() + job.getInput().size());
        fileIds.addAll(job.getOutput());
        fileIds.addAll(job.getInput());
        return fileIds;
    }

    /**
     * Read job information needed to check if can be read or not.
     *
     * @return Job : { id, input, output, outDirId }
     */
    private Job fetchJob(long jobId) throws CatalogDBException {
        return jobDBAdaptor.getJob(jobId, new QueryOptions("include",
                        Arrays.asList("projects.studies.jobs.id",
                                "projects.studies.jobs.status",
                                "projects.studies.jobs.input",
                                "projects.studies.jobs.output",
                                "projects.studies.jobs.outDirId")
                )
        ).first();
    }

    private List<File> fetchJobFiles(Job job) throws CatalogDBException {
        List<Long> fileIds = getFileIds(job);
        if (fileIds.isEmpty()) {
            return Collections.emptyList();
        }
        Query fileQuery = new Query(CatalogFileDBAdaptor.QueryParams.ID.key(), fileIds);
        return fileDBAdaptor.get(fileQuery, FILE_INCLUDE_QUERY_OPTIONS).getResult();
    }

    /**
     * This is like a mini cache where we will store the ACL entries for each path and user. This is useful for single queries trying to
     * fetch multiple files for example. This way, we only retrieve the ACLs once while maintaining the authorization step for each one.
     */
    static class StudyAuthenticationContext {
        private final long studyId;
        /*
         * Map<Path, Map<UserId, AclEntry>>
         */
        private final Map<String, Map<String, AclEntry>> pathUserAclMap;

        StudyAuthenticationContext(long studyId) {
            this.studyId = studyId;
            pathUserAclMap = new HashMap<>();
        }
    }
}
