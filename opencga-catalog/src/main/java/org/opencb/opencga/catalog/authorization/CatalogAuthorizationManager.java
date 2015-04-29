package org.opencb.opencga.catalog.authorization;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by hpccoll1 on 28/04/15.
 */
public class CatalogAuthorizationManager implements AuthorizationManager {
    final CatalogUserDBAdaptor userDBAdaptor;
    private final CatalogDBAdaptor catalogDBAdaptor;

    public CatalogAuthorizationManager(CatalogDBAdaptor catalogDBAdaptor) {
        this.catalogDBAdaptor = catalogDBAdaptor;
        this.userDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
    }

    @Override
    public User.Role getUserRole(String userId) throws CatalogException {
        return userDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).first().getRole();
    }

    @Override
    public Acl getProjectACL(String userId, int projectId) throws CatalogException {
        Acl projectAcl;
        if (getUserRole(userId).equals(User.Role.ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        boolean sameOwner = catalogDBAdaptor.getProjectOwnerId(projectId).equals(userId);

        if (sameOwner) {
            projectAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getProjectAcl(projectId, userId);
            if (!result.getResult().isEmpty()) {
                projectAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getProjectAcl(projectId, Acl.USER_OTHERS_ID);
                if (!resultAll.getResult().isEmpty()) {
                    projectAcl = resultAll.getResult().get(0);
                } else {
                    projectAcl = new Acl(userId, false, false, false, false);
                }
            }
        }
        return projectAcl;
    }

    @Override
    public Acl getStudyACL(String userId, int studyId) throws CatalogException {
        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
        return getStudyACL(userId, studyId, getProjectACL(userId, projectId));
    }

    @Override
    public Acl getFileACL(String userId, int fileId) throws CatalogException {
        if (getUserRole(userId).equals(User.Role.ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
        return getStudyACL(userId, studyId);
    }

    @Override
    public Acl getSampleACL(String userId, int sampleId) throws CatalogException {
        return getStudyACL(userId, catalogDBAdaptor.getStudyIdBySampleId(sampleId));
    }



        /*
     *  Permission methods. Internal use only.
     *  Builds the specific ACL for each pair sessionId,object
     *  ****************
     */

    /**
     * Removes from the list the projects that the user can not read.
     * From the remaining projects, filters the studies and files.
     *
     * @param userId   UserId
     * @param projects Projects list
     * @throws org.opencb.opencga.catalog.CatalogException
     */
    @Override
    public void filterProjects(String userId, List<Project> projects) throws CatalogException {
        Iterator<Project> projectIt = projects.iterator();
        while (projectIt.hasNext()) {
            Project p = projectIt.next();
            Acl projectAcl = getProjectACL(userId, p.getId());
            if (!projectAcl.isRead()) {
                projectIt.remove();
            } else {
                List<Study> studies = p.getStudies();
                filterStudies(userId, projectAcl, studies);
            }
        }
    }

    /**
     * Removes from the list the studies that the user can not read.
     * From the remaining studies, filters the files.
     *
     * @param userId     UserId
     * @param projectAcl Project ACL
     * @param studies    Studies list
     * @throws org.opencb.opencga.catalog.CatalogException
     */
    @Override
    public void filterStudies(String userId, Acl projectAcl, List<Study> studies) throws CatalogException {
        Iterator<Study> studyIt = studies.iterator();
        while (studyIt.hasNext()) {
            Study s = studyIt.next();
            Acl studyAcl = getStudyACL(userId, s.getId(), projectAcl);
            if (!studyAcl.isRead()) {
                studyIt.remove();
            } else {
                List<File> files = s.getFiles();
                filterFiles(userId, studyAcl, files);
            }
        }
    }

    /**
     * Removes from the list the files that the user can not read.
     *
     * @param userId   UserId
     * @param studyAcl Study ACL
     * @param files    Files list
     * @throws org.opencb.opencga.catalog.CatalogException
     */
    @Override
    public void filterFiles(String userId, Acl studyAcl, List<File> files) throws CatalogException {
        if (files == null || files.isEmpty()) {
            return;
        }
        Iterator<File> fileIt = files.iterator();
        while (fileIt.hasNext()) {
            File f = fileIt.next();
            Acl fileAcl = getFileACL(userId, f.getId(), studyAcl);
            if (!fileAcl.isRead()) {
                fileIt.remove();
            }
        }
    }

    private Acl mergeAcl(String userId, Acl acl1, Acl acl2) {
        return new Acl(
                userId,
                acl1.isRead() && acl2.isRead(),
                acl1.isWrite() && acl2.isWrite(),
                acl1.isExecute() && acl2.isExecute(),
                acl1.isDelete() && acl2.isDelete()
        );
    }


    public Acl getStudyACL(String userId, int studyId, Acl projectAcl) throws CatalogException {
        Acl studyAcl;
        if (getUserRole(userId).equals(User.Role.ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        boolean sameOwner = catalogDBAdaptor.getStudyOwnerId(studyId).equals(userId);

        if (sameOwner) {
            studyAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getStudyAcl(studyId, userId);
            if (!result.getResult().isEmpty()) {
                studyAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getStudyAcl(studyId, Acl.USER_OTHERS_ID);
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
    public Acl getFileACL(String userId, int fileId, Acl studyAcl) throws CatalogException {
        return studyAcl;
    }

    //TODO: Check folder ACLs
    private Acl __getFileAcl(String userId, int fileId, Acl studyAcl) throws CatalogException {
        Acl fileAcl;
        boolean sameOwner = catalogDBAdaptor.getFileOwnerId(fileId).equals(userId);

        if (sameOwner) {
            fileAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = catalogDBAdaptor.getFileAcl(fileId, userId);
            if (!result.getResult().isEmpty()) {
                fileAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = catalogDBAdaptor.getFileAcl(fileId, Acl.USER_OTHERS_ID);
                if (!resultAll.getResult().isEmpty()) {
                    fileAcl = resultAll.getResult().get(0);
                } else {
                    //fileAcl = new Acl(userId, false, false, false, false);
                    fileAcl = studyAcl;
                }
            }
        }
        return mergeAcl(userId, fileAcl, studyAcl);
    }

}
