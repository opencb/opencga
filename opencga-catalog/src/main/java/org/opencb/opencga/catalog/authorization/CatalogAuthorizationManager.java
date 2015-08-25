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
    private final CatalogUserDBAdaptor userDBAdaptor;
    private final CatalogStudyDBAdaptor studyDBAdaptor;
    private final CatalogFileDBAdaptor fileDBAdaptor;
    private final CatalogJobDBAdaptor jobDBAdaptor;
    private final CatalogSampleDBAdaptor sampleDBAdaptor;
    private final CatalogIndividualDBAdaptor individualDBAdaptor;

    public CatalogAuthorizationManager(CatalogDBAdaptorFactory catalogDBAdaptorFactory) {
        userDBAdaptor = catalogDBAdaptorFactory.getCatalogUserDBAdaptor();
        studyDBAdaptor = catalogDBAdaptorFactory.getCatalogStudyDBAdaptor();
        fileDBAdaptor = catalogDBAdaptorFactory.getCatalogFileDBAdaptor();
        jobDBAdaptor = catalogDBAdaptorFactory.getCatalogJobDBAdaptor();
        sampleDBAdaptor = catalogDBAdaptorFactory.getCatalogSampleDBAdaptor();
        individualDBAdaptor = catalogDBAdaptorFactory.getCatalogIndividualDBAdaptor();
    }

    @Override
    public User.Role getUserRole(String userId) throws CatalogException {
        return userDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).first().getRole();
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
        int studyId = sampleDBAdaptor.getStudyIdBySampleId(sampleId);
        if (isAdmin(userId) || isOwner(studyId, userId)) {
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

    @Override
    public void checkIndividualPermission(int individualId, String userId, CatalogPermission permission) throws CatalogException {
        int studyId = individualDBAdaptor.getStudyIdByIndividualId(individualId);
        if (isAdmin(userId) || isOwner(studyId, userId)) {  //User admin or owner
            return;
        }

        final boolean auth;
        Group group = getGroupBelonging(studyId, userId);
        if (group == null) {    //User not in study
            auth = false;
        } else if (group.getPermissions().isManagerSamples()) {
            auth = true;
        } else {
            switch (permission) {
                case READ:
                    List<Sample> samples = sampleDBAdaptor.getAllSamples(new QueryOptions(CatalogSampleDBAdaptor.SampleFilterOption.individualId.toString(), individualId)).getResult();
                    filterSamples(userId, studyId, samples, group);
                    auth = !samples.isEmpty();
                    break;
                default:
                    auth = false;
                    break;
            }
        }
        if (!auth) {
            throw CatalogAuthorizationException.denny(userId, permission.toString(), "Individual", individualId, null);
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
            filterIndividuals(userId, study.getId(), study.getIndividuals());
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

    @Override
    public void filterIndividuals(String userId, int studyId, List<Individual> individuals) throws CatalogException {
        if (individuals == null || individuals.isEmpty()) {
            return;
        }
        if (isAdmin(userId) || isOwner(studyId, userId)) {
            return;
        }

        for (Iterator<Individual> iterator = individuals.iterator(); iterator.hasNext(); ) {
            Individual individual = iterator.next();
            try {
                checkIndividualPermission(individual.getId(), userId, CatalogPermission.READ);
            } catch (CatalogAuthorizationException e) {
                iterator.remove();
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


    /**
     * Use StudyACL for all files.
     */

    private boolean isOwner(int studyId, String userId) throws CatalogDBException {
        return userDBAdaptor.getProjectOwnerId(studyDBAdaptor.getProjectIdByStudyId(studyId)).equals(userId);
    }

    private boolean isAdmin(String userId) throws CatalogException {
        return getUserRole(userId).equals(User.Role.ADMIN);
    }

    //TODO: Check folder ACLs
    private final QueryOptions fileIncludeQueryOptions = new QueryOptions("include", Arrays.asList("projects.studies.files.id", "projects.studies.files.path", "projects.studies.files.acls"));

}
