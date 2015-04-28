package org.opencb.opencga.catalog.authorization;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.api.IUserManager;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;

import java.util.Arrays;

/**
 * Created by hpccoll1 on 28/04/15.
 */
public class CatalogAuthorizationService implements AuthorizationService {
    final CatalogUserDBAdaptor userDBAdaptor;

    public CatalogAuthorizationService(CatalogUserDBAdaptor userDBAdaptor) {
        this.userDBAdaptor = userDBAdaptor;
    }

    @Override
    public User.Role getUserRole(String userId) throws CatalogDBException {
        return userDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).first().getRole();
    }

    @Override
    public Acl getProjectACL(String userId, int projectId) {
        return null;
    }

    @Override
    public Acl getStudyACL(String userId, int studyId) {
        return null;
    }

    @Override
    public Acl getFileACL(String userId, int fileId) {
        return null;
    }

    @Override
    public Acl getSampleACL(String userId, int fileId) {
        return null;
    }
}
