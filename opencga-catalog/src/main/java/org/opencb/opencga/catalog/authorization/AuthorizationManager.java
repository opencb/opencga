package org.opencb.opencga.catalog.authorization;


//import java.security.acl.Acl;

import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.catalog.db.CatalogDBException;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public interface AuthorizationManager {

    public User.Role getUserRole(String userId) throws CatalogDBException;

    public Acl getProjectACL(String userId, int projectId);

    public Acl getStudyACL(String userId, int studyId);

    public Acl getFileACL(String userId, int fileId);

    public Acl getSampleACL(String userId, int fileId);

}
