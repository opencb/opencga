package org.opencb.opencga.catalog.authorization;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.db.api.*;

import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationManager implements AuthorizationManager {
    final CatalogUserDBAdaptor userDBAdaptor;
    final CatalogStudyDBAdaptor studyDBAdaptor;
    final CatalogFileDBAdaptor fileDBAdaptor;
    final CatalogJobDBAdaptor jobDBAdaptor;
    final CatalogSampleDBAdaptor sampleDBAdaptor;

    public CatalogAuthorizationManager(CatalogDBAdaptorFactory catalogDBAdaptorFactory) {
        this.userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
    }

    @Override
    public User.Role getUserRole(String userId) throws CatalogException {
        return userDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).first().getRole();
    }

    @Override
    @Deprecated
    public AclEntry getProjectACL(String userId, int projectId) throws CatalogException {
        AclEntry projectAcl;
        if (isAdmin(userId)) {
            return new AclEntry(userId, true, true, true, true);
        }
        boolean sameOwner = userDBAdaptor.getProjectOwnerId(projectId).equals(userId);

        if (sameOwner) {
            projectAcl = new AclEntry(userId, true, true, true, true);
        } else {
            QueryResult<AclEntry> result = userDBAdaptor.getProjectAcl(projectId, userId);
            if (!result.getResult().isEmpty()) {
                projectAcl = result.getResult().get(0);
            } else {
                QueryResult<AclEntry> resultAll = userDBAdaptor.getProjectAcl(projectId, AclEntry.USER_OTHERS_ID);
                if (!resultAll.getResult().isEmpty()) {
                    projectAcl = resultAll.getResult().get(0);
                } else {
                    projectAcl = new AclEntry(userId, false, false, false, false);
                }
            }
        }
        return projectAcl;
    }

    @Override
    @Deprecated
    public QueryResult setProjectACL(int projectId, AclEntry acl, String sessionId) throws CatalogException {
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        checkProjectPermission(projectId, userId, CatalogPermission.WRITE);

        return userDBAdaptor.setProjectAcl(projectId, acl);
    }

    @Override
    public void checkProjectPermission(int projectId, String userId, CatalogPermission permission) throws CatalogException {
        if (!isAdmin(userId) && !userDBAdaptor.getProjectOwnerId(projectId).equals(userId)) {
            throw CatalogAuthorizationException.denny(userId, permission.toString(), "Project", projectId, null);
        }
    }

    @Override
    public void checkStudyPermission(int studyId, String userId, StudyPermission permission) throws CatalogException {
        checkStudyPermission(studyId, userId, permission, permission.toString());
    }

    @Override
    public void checkStudyPermission(int studyId, String userId, StudyPermission permission, String message) throws CatalogException {
        if (isOwner(studyId, userId)) {
            return;
        }
        if (isAdmin(userId)) {
            return;
        }

        Group group = getGroupBelonging(studyId, userId);
        if (group == null) {
            throw CatalogAuthorizationException.denny(userId, message, "Study", studyId, null);
        }

        final boolean auth;
        switch (permission) {
            case DELETE_JOBS:
                auth = group.getPermissions().isDeleteJobs();
                break;
            case LAUNCH_JOBS:
                auth = group.getPermissions().isLaunchJobs();
                break;
            case MANAGE_SAMPLES:
                auth = group.getPermissions().isManagerSamples();
                break;
            case MANAGE_STUDY:
                auth = group.getPermissions().isStudyManager();
                break;
            case READ_STUDY:
                auth = true; //Authorize if belongs to any group
                break;
            default:
                auth = false;
        }
        if (!auth) {
            throw CatalogAuthorizationException.denny(userId, message, "Study", studyId, null);
        }
    }

    @Override
    public void checkFilePermission(int fileId, String userId, CatalogPermission permission) throws CatalogException {
        if (isAdmin(userId)) {
            return;
        }
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        if (isOwner(studyId, userId)) {
            return;
        }

        AclEntry fileAcl = resolveFileAcl(fileId, userId, studyId, getGroupBelonging(studyId, userId));


        final boolean auth;
        switch (permission) {
            case READ:
                auth = fileAcl.isRead();
                break;
            case WRITE:
                auth = fileAcl.isWrite();
                break;
            case DELETE:
                auth = fileAcl.isDelete();
                break;
            default:
                auth = false;
                break;
        }
        if (!auth) {
            throw CatalogAuthorizationException.denny(userId, permission.toString(), "File", fileId, null);
        }

    }

    @Override
    public void checkSamplePermission(int sampleId, String userId, CatalogPermission permission) throws CatalogException {
        if (isAdmin(userId)) {
            return;
        }
        int studyId = sampleDBAdaptor.getStudyIdBySampleId(sampleId);
        if (isOwner(studyId, userId)) {
            return;
        }

        AclEntry sampleAcl = resolveSampleAcl(sampleId, userId, getGroupBelonging(studyId, userId));

        final boolean auth;
        switch (permission) {
            case READ:
                auth = sampleAcl.isRead();
                break;
            case WRITE:
                auth = sampleAcl.isWrite();
                break;
            case DELETE:
                auth = sampleAcl.isDelete();
                break;
            default:
                auth = false;
                break;
        }
        if (!auth) {
            throw CatalogAuthorizationException.denny(userId, permission.toString(), "Sample", sampleId, null);
        }

    }

    private AclEntry resolveFileAcl(int fileId, String userId, int studyId, Group group) throws CatalogException {
        if (group == null) {
            return new AclEntry(userId, false, false, false, false);
        }

        String groupId = "@" + group.getId();
        AclEntry studyAcl = getStudyACL(userId, group);
        AclEntry fileAcl = null;

        File file = fileDBAdaptor.getFile(fileId, fileIncludeQueryOptions).first();
        List<String> paths = FileManager.getParentPaths(file.getPath());
        Map<String, Map<String, AclEntry>> pathAclMap = fileDBAdaptor.getFilesAcl(studyId, FileManager.getParentPaths(file.getPath()),
                Arrays.asList(userId, groupId, AclEntry.USER_OTHERS_ID)).first();

        for (int i = paths.size() - 1; i >= 0; i--) {
            String path = paths.get(i);
            if (pathAclMap.containsKey(path)) {
                Map<String, AclEntry> aclMap = pathAclMap.get(path);
                //Get first the user AclEntry
                if (aclMap.containsKey(userId)) {
                    fileAcl = aclMap.get(userId);
                } else if (aclMap.containsKey(groupId)) {
                    fileAcl = aclMap.get(groupId);
                } else if (aclMap.containsKey(AclEntry.USER_OTHERS_ID)) {
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

    /**
     * Resolves the permissions between a sample and a user.
     * Returns the most specific matching ACL following the next sequence:
     * user > group > others > study
     *
     * @param group     User belonging group.
     * @throws CatalogException
     */
    private AclEntry resolveSampleAcl(int sampleId, String userId, Group group) throws CatalogException {
        if (group == null) {
            return new AclEntry(userId, false, false, false, false);
        }

        String groupId = "@" + group.getId();
        Map<String, AclEntry> userAclMap = sampleDBAdaptor.getSampleAcl(sampleId, Arrays.asList(userId, groupId, AclEntry.USER_OTHERS_ID)).first();

        if (userAclMap.containsKey(userId)) {
            return userAclMap.get(userId);
        } else if (userAclMap.containsKey(groupId)) {
            return userAclMap.get(groupId);
        } else if (userAclMap.containsKey(AclEntry.USER_OTHERS_ID)) {
            return userAclMap.get(AclEntry.USER_OTHERS_ID);
        } else {
            return getStudyACL(userId, group);
        }
    }

    private AclEntry getStudyACL(String userId, Group group) {
        if (group == null) {
            return new AclEntry(userId, false, false, false, false);
        } else {
            return new AclEntry(userId, group.getPermissions().isRead(), group.getPermissions().isWrite(), false, group.getPermissions().isDelete());
        }
    }

    @Override
    @Deprecated
    public AclEntry getStudyACL(String userId, int studyId) throws CatalogException {
        return getStudyACL(userId, getGroupBelonging(studyId, userId));
    }

    @Override
    @Deprecated
    public QueryResult setStudyACL(int studyId, AclEntry acl, String sessionId) throws CatalogException {
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        AclEntry studyACL = getStudyACL(userId, studyId);
        if (!studyACL.isWrite()) {
            throw CatalogAuthorizationException.cantModify(userId, "Study", studyId, null);
        }

        return studyDBAdaptor.setStudyAcl(studyId, acl);
    }

    @Override
    @Deprecated
    public AclEntry getFileACL(String userId, int fileId) throws CatalogException {
        if (isAdmin(userId)) {
            return new AclEntry(userId, true, true, true, true);
        }
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        return getFileACL(userId, fileId, getStudyACL(userId, studyId));
    }

    @Override
    public QueryResult setFileACL(int fileId, AclEntry acl, String sessionId) throws CatalogException {
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        checkStudyPermission(fileDBAdaptor.getStudyIdByFileId(fileId), userId, StudyPermission.MANAGE_STUDY);

        return fileDBAdaptor.setFileAcl(fileId, acl);
    }

    @Override
    public QueryResult unsetFileACL(int fileId, String userId, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(userId, "userId");

        checkStudyPermission(fileDBAdaptor.getStudyIdByFileId(fileId), userDBAdaptor.getUserIdBySessionId(sessionId), StudyPermission.MANAGE_STUDY);

        return fileDBAdaptor.unsetFileAcl(fileId, userId);
    }

    @Override
    @Deprecated
    public AclEntry getSampleACL(String userId, int sampleId) throws CatalogException {
        return getSampleACL(userId, sampleId, sampleDBAdaptor.getStudyIdBySampleId(sampleId));
    }

    @Deprecated
    private AclEntry getSampleACL(String userId, int sampleId, int studyId) throws CatalogException {
        return getSampleACL(userId, sampleId, getStudyACL(userId, studyId));
    }

    @Deprecated
    private AclEntry getSampleACL(String userId, int sampleId, AclEntry studyACL) throws CatalogException {
        QueryResult<AclEntry> queryResult = sampleDBAdaptor.getSampleAcl(sampleId, userId);
        AclEntry sampleAcl;
        if (queryResult.getNumResults() == 0) {
            sampleAcl = studyACL;
        } else {
            sampleAcl = queryResult.first();
        }
        return sampleAcl;
    }

    @Override
    public QueryResult setSampleACL(int sampleId, AclEntry acl, String sessionId) throws CatalogException {
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        checkStudyPermission(sampleDBAdaptor.getStudyIdBySampleId(sampleId), userId, StudyPermission.MANAGE_STUDY);

        return sampleDBAdaptor.setSampleAcl(sampleId, acl);
    }

    @Override
    public QueryResult unsetSampleACL(int sampleId, String userId, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(userId, "userId");

        checkStudyPermission(sampleDBAdaptor.getStudyIdBySampleId(sampleId), userDBAdaptor.getUserIdBySessionId(sessionId), StudyPermission.MANAGE_STUDY);

        return sampleDBAdaptor.unsetSampleAcl(sampleId, userId);
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
                break;
            }
            filterStudies(userId, p.getStudies());
        }
    }

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
                break;
            }
            Group group = getGroupBelonging(study.getId(), userId);
            List<File> files = study.getFiles();
            filterFiles(userId, study.getId(), files, group);
            filterSamples(userId, study.getId(), study.getSamples(), group);
            filterJobs(userId, study.getJobs());
            filterCohorts(userId, study.getId(), study.getCohorts());
        }
    }

    @Override
    public void filterFiles(String userId, int studyId, List<File> files) throws CatalogException {
        filterFiles(userId, studyId, files, getGroupBelonging(studyId, userId));
    }

    private void filterFiles(String userId, int studyId, List<File> files, Group group) throws CatalogException {
        if (files == null || files.isEmpty()) {
            return;
        }

        Iterator<File> fileIt = files.iterator();
        while (fileIt.hasNext()) {
            File file = fileIt.next();
            if (!resolveFileAcl(file.getId(), userId, studyId, group).isRead()) {
                fileIt.remove();
            }
        }
    }

    @Override
    public void filterSamples(String userId, int studyId, List<Sample> samples) throws CatalogException {
        filterSamples(userId, studyId, samples, getGroupBelonging(studyId, userId));
    }

    public void filterSamples(String userId, int studyId, List<Sample> samples, Group group) throws CatalogException {
        if (samples == null || samples.isEmpty()) {
            return;
        }
        if (isAdmin(userId) || isOwner(studyId, userId)) {
            return;
        }

        Iterator<Sample> sampleIterator = samples.iterator();
        while (sampleIterator.hasNext()) {
            Sample sample = sampleIterator.next();
            AclEntry sampleACL = resolveSampleAcl(sample.getId(), userId, group);
            if (!sampleACL.isRead()) {
                sampleIterator.remove();
            }
        }
    }

    @Override
    public void filterJobs(String userId, List<Job> jobs) throws CatalogException {
        if (jobs == null || jobs.isEmpty()) {
            return;
        }
        if (isAdmin(userId)) {
            return;
        }
        job_loop: for (Iterator<Job> iterator = jobs.iterator(); iterator.hasNext(); ) {
            Job job = iterator.next();
            int studyId = jobDBAdaptor.getStudyIdByJobId(job.getId());
            Group group = getGroupBelonging(studyId, userId);
            if (job.getOutput() == null || job.getInput() == null) {
                job = readJob(job.getId());
            }
            for (Integer fileId : job.getOutput()) {
                if (!resolveFileAcl(fileId, userId, studyId, group).isRead()) {
                    iterator.remove();
                    break job_loop;
                }
            }
            for (Integer fileId : job.getInput()) {
                if (!resolveFileAcl(fileId, userId, studyId, group).isRead()) {
                    iterator.remove();
                    break job_loop;
                }
            }
        }
    }

    @Override
    public void filterCohorts(String userId, int studyId, List<Cohort> cohorts) throws CatalogException {
        if (cohorts == null || cohorts.isEmpty()) {
            return;
        }
        if (isAdmin(userId) || isOwner(studyId, userId)) {
            return;
        }
        Group group = getGroupBelonging(studyId, userId);

        Map<Integer, AclEntry> sampleAclMap = new HashMap<>();

        for (Iterator<Cohort> iterator = cohorts.iterator(); iterator.hasNext(); ) {
            Cohort cohort = iterator.next();
            for (Integer sampleId : cohort.getSamples()) {
                AclEntry sampleACL;
                if (sampleAclMap.containsKey(sampleId)) {
                    sampleACL = sampleAclMap.get(sampleId);
                } else {
                    sampleACL = resolveSampleAcl(sampleId, userId, group);
                    sampleAclMap.put(sampleId, sampleACL);
                }
                if (!sampleACL.isRead()) {
                    iterator.remove();  //Remove cohort.
                    break;              //Stop checking cohort
                }
            }
        }
    }

    /**
     * Read job information needed to check if can be read or not.
     *
     * @return Job : { id, input, output, outDirId }
     */
    private Job readJob(int jobId) throws CatalogDBException {
        return jobDBAdaptor.getJob(jobId, new QueryOptions("include",
                Arrays.asList("projects.studies.jobs.id",
                        "projects.studies.jobs.input",
                        "projects.studies.jobs.output",
                        "projects.studies.jobs.outDirId")
                )
        ).first();
    }

    @Override
    public void checkReadJob(String userId, int jobId) throws CatalogException {
        checkReadJob(userId, readJob(jobId));
    }

    @Override
    public void checkReadJob(String userId, Job job) throws CatalogException {
        try {
            for (Integer fileId : job.getOutput()) {
                checkFilePermission(fileId, userId, CatalogPermission.READ);
            }
            for (Integer fileId : job.getInput()) {
                checkFilePermission(fileId, userId, CatalogPermission.READ);
            }
        } catch (CatalogAuthorizationException e) {
            throw CatalogAuthorizationException.cantRead(userId, "Job", job.getId(), job.getName());
        }
    }

    @Override
    public void checkReadCohort(String userId, Cohort cohort) throws CatalogException {
        try {
            for (Integer sampleId : cohort.getSamples()) {
                checkSamplePermission(sampleId, userId, CatalogPermission.READ);
            }
        } catch (CatalogAuthorizationException e) {
            throw CatalogAuthorizationException.cantRead(userId, "Cohort", cohort.getId(), cohort.getName());
        }
    }

    @Override
    public Group getGroupBelonging(int studyId, String userId) throws CatalogException {
        QueryResult<Group> queryResult = studyDBAdaptor.getGroup(studyId, userId, null, null);
        return queryResult.getNumResults() == 0 ? null : queryResult.first();
    }

    @Override
    public QueryResult<Group> addMember(int studyId, String groupId, String userIdToAdd, String sessionId) throws CatalogException {

        checkStudyPermission(studyId, userDBAdaptor.getUserIdBySessionId(sessionId), StudyPermission.MANAGE_STUDY);

        Group groupFromUserToAdd = getGroupBelonging(studyId, userIdToAdd);
        if (groupFromUserToAdd != null) {
            throw new CatalogException("User \"" + userIdToAdd + "\" already belongs to group " + groupFromUserToAdd.getId());
        }

        return studyDBAdaptor.addMemberToGroup(studyId, groupId, userIdToAdd);
    }

    @Override
    public QueryResult<Group> removeMember(int studyId, String groupId, String userIdToRemove, String sessionId) throws CatalogException {

        checkStudyPermission(studyId, userDBAdaptor.getUserIdBySessionId(sessionId), StudyPermission.MANAGE_STUDY);

        Group groupFromUserToRemove = getGroupBelonging(studyId, userIdToRemove);
        if (groupFromUserToRemove == null || !groupFromUserToRemove.getId().equals(groupId)) {
            throw new CatalogException("User \"" + userIdToRemove + "\" does not belongs to group " + groupId);
        }

        return studyDBAdaptor.removeMemberFromGroup(studyId, groupId, userIdToRemove);
    }

    @Deprecated
    private AclEntry mergeAcl(String userId, AclEntry acl1, AclEntry acl2) {
        return new AclEntry(
                userId,
                acl1.isRead() && acl2.isRead(),
                acl1.isWrite() && acl2.isWrite(),
                acl1.isExecute() && acl2.isExecute(),
                acl1.isDelete() && acl2.isDelete()
        );
    }


    @Deprecated
    private AclEntry getStudyACL(String userId, int studyId, AclEntry projectAcl) throws CatalogException {
        AclEntry studyAcl;
        if (isAdmin(userId)) {
            return new AclEntry(userId, true, true, true, true);
        }
        boolean sameOwner = studyDBAdaptor.getStudyOwnerId(studyId).equals(userId);

        if (sameOwner) {
            studyAcl = new AclEntry(userId, true, true, true, true);
        } else {
            QueryResult<AclEntry> result = studyDBAdaptor.getStudyAcl(studyId, userId);
            if (!result.getResult().isEmpty()) {
                studyAcl = result.getResult().get(0);
            } else {
                QueryResult<AclEntry> resultAll = studyDBAdaptor.getStudyAcl(studyId, AclEntry.USER_OTHERS_ID);
                if (!resultAll.getResult().isEmpty()) {
                    studyAcl = resultAll.getResult().get(0);
                } else {
                    //studyAcl = new Acl(userId, false, false, false, false);
                    studyAcl = projectAcl;
                }
            }
        }
        return mergeAcl(userId, projectAcl, studyAcl);
    }

    /**
     * Use StudyACL for all files.
     */
    @Deprecated
    public AclEntry getFileACL(String userId, int fileId, AclEntry studyAcl) throws CatalogException {
        return __getFileAcl(userId, fileDBAdaptor.getStudyIdByFileId(fileId), fileId, studyAcl);
    }

    private boolean isOwner(int studyId, String userId) throws CatalogDBException {
        return userDBAdaptor.getProjectOwnerId(studyDBAdaptor.getProjectIdByStudyId(studyId)).equals(userId);
    }

    private boolean isAdmin(String userId) throws CatalogException {
        return getUserRole(userId).equals(User.Role.ADMIN);
    }

    //TODO: Check folder ACLs
    private final QueryOptions fileIncludeQueryOptions = new QueryOptions("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.path", "projects.studies.files.acls"));

    @Deprecated
    private AclEntry __getFileAcl(String userId, int studyId, int fileId, AclEntry studyAcl) throws CatalogException {
        AclEntry fileAcl = null;
        boolean sameOwner = fileDBAdaptor.getFileOwnerId(fileId).equals(userId);

        if (sameOwner) {
            fileAcl = new AclEntry(userId, true, true, true, true);
        } else {
            File file = fileDBAdaptor.getFile(fileId, fileIncludeQueryOptions).first();
            List<String> paths = FileManager.getParentPaths(file.getPath());
            Map<String, Map<String, AclEntry>> pathAclMap = fileDBAdaptor.getFilesAcl(studyId, FileManager.getParentPaths(file.getPath()), Arrays.asList(userId, AclEntry.USER_OTHERS_ID)).first();

            for (int i = paths.size() - 1; i >= 0; i--) {
                String path = paths.get(i);
                if (pathAclMap.containsKey(path)) {
                    //Get first the user AclEntry
                    fileAcl = pathAclMap.get(path).get(userId);
                    //If missing, get Others AclEntry
                    if (fileAcl == null) {
                        fileAcl = pathAclMap.get(path).get(AclEntry.USER_OTHERS_ID);
                    }
                    if (fileAcl != null) {
                        break;
                    }
                }
            }
//            for (String path : paths) {
//                if (pathAclMap.containsKey(path)) {
//                    mergeAcl(userId, )
//                }
//            }
//
//            if (!result.getResult().isEmpty()) {
//                fileAcl = result.getResult().get(0);
//            } else {
//                QueryResult<Acl> resultAll = fileDBAdaptor.getFileAcl(fileId, Acl.USER_OTHERS_ID);
//                if (!resultAll.getResult().isEmpty()) {
//                    fileAcl = resultAll.getResult().get(0);
//                } else {
//                    //fileAcl = new Acl(userId, false, false, false, false);
//                    fileAcl = studyAcl;
//                }
//            }
        }
        return fileAcl == null ? studyAcl : fileAcl;
//        return mergeAcl(userId, fileAcl, studyAcl);
    }

}
