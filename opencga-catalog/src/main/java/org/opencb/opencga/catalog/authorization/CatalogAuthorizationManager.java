package org.opencb.opencga.catalog.authorization;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.db.api.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class CatalogAuthorizationManager implements AuthorizationManager {
    final CatalogUserDBAdaptor userDBAdaptor;
    final CatalogStudyDBAdaptor studyDBAdaptor;
    final CatalogFileDBAdaptor fileDBAdaptor;
    final CatalogSampleDBAdaptor sampleDBAdaptor;

    public CatalogAuthorizationManager(CatalogDBAdaptor catalogDBAdaptor) {
        this.userDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptor.getCatalogSampleDBAdaptor();
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
        boolean sameOwner = userDBAdaptor.getProjectOwnerId(projectId).equals(userId);

        if (sameOwner) {
            projectAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = userDBAdaptor.getProjectAcl(projectId, userId);
            if (!result.getResult().isEmpty()) {
                projectAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = userDBAdaptor.getProjectAcl(projectId, Acl.USER_OTHERS_ID);
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
    public QueryResult setProjectACL(int projectId, Acl acl, String sessionId) throws CatalogException {
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Acl projectAcl = getProjectACL(userId, projectId);
        if (!projectAcl.isWrite()) {
            throw CatalogAuthorizationException.cantModify(userId, "Project", projectId, null);
        }

        return userDBAdaptor.setProjectAcl(projectId, acl);
    }

    @Override
    public Acl getStudyACL(String userId, int studyId) throws CatalogException {
        int projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        return getStudyACL(userId, studyId, getProjectACL(userId, projectId));
    }

    @Override
    public QueryResult setStudyACL(int studyId, Acl acl, String sessionId) throws CatalogException {
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Acl studyACL = getStudyACL(userId, studyId);
        if (!studyACL.isWrite()) {
            throw CatalogAuthorizationException.cantModify(userId, "Study", studyId, null);
        }

        return studyDBAdaptor.setStudyAcl(studyId, acl);
    }

    @Override
    public Acl getFileACL(String userId, int fileId) throws CatalogException {
        if (getUserRole(userId).equals(User.Role.ADMIN)) {
            return new Acl(userId, true, true, true, true);
        }
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        return getStudyACL(userId, studyId);
    }

    @Override
    public QueryResult setFileACL(int fileId, Acl acl, String sessionId) throws CatalogException {
        ParamUtils.checkObj(acl, "acl");
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Acl fileAcl = getFileACL(userId, fileId);
        if (!fileAcl.isWrite()) {
            throw CatalogAuthorizationException.cantModify(userId, "File", fileId, null);
        }

        return fileDBAdaptor.setFileAcl(fileId, acl);
    }

    @Override
    public Acl getSampleACL(String userId, int sampleId) throws CatalogException {
        return getStudyACL(userId, sampleDBAdaptor.getStudyIdBySampleId(sampleId));
    }

    @Override
    public QueryResult setSampleACL(int sampleId, Acl acl, String sessionId) {
        throw new UnsupportedOperationException("Unimplemented");
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
     * @throws org.opencb.opencga.catalog.exceptions.CatalogException
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
     * @throws org.opencb.opencga.catalog.exceptions.CatalogException
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
     * @throws org.opencb.opencga.catalog.exceptions.CatalogException
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
        boolean sameOwner = studyDBAdaptor.getStudyOwnerId(studyId).equals(userId);

        if (sameOwner) {
            studyAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = studyDBAdaptor.getStudyAcl(studyId, userId);
            if (!result.getResult().isEmpty()) {
                studyAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = studyDBAdaptor.getStudyAcl(studyId, Acl.USER_OTHERS_ID);
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
        boolean sameOwner = fileDBAdaptor.getFileOwnerId(fileId).equals(userId);

        if (sameOwner) {
            fileAcl = new Acl(userId, true, true, true, true);
        } else {
            QueryResult<Acl> result = fileDBAdaptor.getFileAcl(fileId, userId);
            if (!result.getResult().isEmpty()) {
                fileAcl = result.getResult().get(0);
            } else {
                QueryResult<Acl> resultAll = fileDBAdaptor.getFileAcl(fileId, Acl.USER_OTHERS_ID);
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
