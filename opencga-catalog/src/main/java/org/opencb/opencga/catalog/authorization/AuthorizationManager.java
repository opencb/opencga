package org.opencb.opencga.catalog.authorization;


//import java.security.acl.Acl;

import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface AuthorizationManager {

    void checkStudyPermission(int studyId, String userId, StudyPermission permission) throws CatalogException;

    void checkStudyPermission(int studyId, String userId, StudyPermission permission, String message) throws CatalogException;

    void checkFilePermission(int fileId, String userId, CatalogPermission permission) throws CatalogException;

    void checkSamplePermission(int sampleId, String userId, CatalogPermission permission) throws CatalogException;

    void checkReadJob(String userId, int jobId) throws CatalogException;

    void checkReadJob(String userId, Job job) throws CatalogException;

    void checkReadCohort(String userId, Cohort cohort) throws CatalogException;

    User.Role getUserRole(String userId) throws CatalogException;

    @Deprecated
    Acl getProjectACL(String userId, int projectId) throws CatalogException;

    @Deprecated
    QueryResult setProjectACL(int projectId, Acl acl, String sessionId) throws CatalogException;

    @Deprecated
    Acl getStudyACL(String userId, int studyId) throws CatalogException;

    QueryResult setStudyACL(int studyId, Acl acl, String sessionId) throws CatalogException;

    @Deprecated
    Acl getFileACL(String userId, int fileId) throws CatalogException;

    QueryResult setFileACL(int fileId, Acl acl, String sessionId) throws CatalogException;

    @Deprecated
    Acl getSampleACL(String userId, int sampleId) throws CatalogException;

    QueryResult setSampleACL(int sampleId, Acl acl, String sessionId) throws CatalogException;

    /**
     * Removes from the list the projects that the user can not read.
     * From the remaining projects, filters the studies and files.
     *
     * @param userId   UserId
     * @param projects Projects list
     * @throws org.opencb.opencga.catalog.exceptions.CatalogException
     */
    void filterProjects(String userId, List<Project> projects) throws CatalogException;

    /**
     * Removes from the list the studies that the user can not read.
     * From the remaining studies, filters the files.
     *
     * @param userId     UserId
     * @param projectAcl Project ACL
     * @param studies    Studies list
     * @throws org.opencb.opencga.catalog.exceptions.CatalogException
     */
    void filterStudies(String userId, Acl projectAcl, List<Study> studies) throws CatalogException;

    /**
     * Removes from the list the files that the user can not read.
     *
     * @param userId   UserId
     * @param studyAcl Study ACL
     * @param files    Files list
     * @throws org.opencb.opencga.catalog.exceptions.CatalogException
     */
    void filterFiles(String userId, Acl studyAcl, List<File> files) throws CatalogException;

    /**
     * Removes from the list the samples that the user can not read.
     *
     * @param userId   UserId
     * @param samples  Samples list
     * @throws org.opencb.opencga.catalog.exceptions.CatalogException
     */
    void filterSamples(String userId, int studyId, List<Sample> samples) throws CatalogException;

    void filterJobs(String userId, List<Job> jobs) throws CatalogException;

    void filterCohorts(String userId, int studyId, List<Cohort> cohorts) throws CatalogException;

    /*--------------------------*/
    // Group management methods
    /*--------------------------*/

    String ADMINS_GROUP = "admins";
    String DATA_MANAGERS_GROUP = "dataManagers";
    String MEMBERS_GROUP = "members";

    /**
     * Get tree default groups.
     *  admins : Full permissions
     *  dataManagers: Full data permissions. No study permissions
     *  members: Just launch jobs permission.
     *
     * @param adminUsers Users to add to the admin group by default.
     */
    static List<Group> getDefaultGroups(Collection<String> adminUsers) {
        return Arrays.asList(
                new Group(ADMINS_GROUP, new ArrayList<>(adminUsers), new StudyPermissions(true, true, true, true, true, true, true)),
                new Group(DATA_MANAGERS_GROUP, Collections.emptyList(), new StudyPermissions(true, true, true, true, true, true, false)),
                new Group(MEMBERS_GROUP, Collections.emptyList(), new StudyPermissions(false, false, false, true, false, false, false)));
    }

    Group getGroupBelonging(int studyId, String userId) throws CatalogException;

//    Group createGroup(int studyId, String groupId, GroupPermissions groupPermissions, String sessionId) throws CatalogException;

//    void deleteGroup(int studyId, String groupId, String sessionId) throws CatalogException;

    QueryResult<Group> addMember(int studyId, String groupId, String userId, String sessionId) throws CatalogException;

    QueryResult<Group> removeMember(int studyId, String groupId, String userId, String sessionId) throws CatalogException;
}
