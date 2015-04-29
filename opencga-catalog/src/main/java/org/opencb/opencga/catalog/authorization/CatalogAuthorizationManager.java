package org.opencb.opencga.catalog.authorization;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.User;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.db.api.CatalogDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogUserDBAdaptor;

import java.util.Arrays;

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
    public User.Role getUserRole(String userId) throws CatalogDBException {
        return userDBAdaptor.getUser(userId, new QueryOptions("include", Arrays.asList("role")), null).first().getRole();
    }

    @Override
    public Acl getProjectACL(String userId, int projectId) {
        return new Acl(userId, true, true, true, true);
    }

    @Override
    public Acl getStudyACL(String userId, int studyId) {
        return new Acl(userId, true, true, true, true);
    }

    @Override
    public Acl getFileACL(String userId, int fileId) {
        return new Acl(userId, true, true, true, true);
    }

    @Override
    public Acl getSampleACL(String userId, int fileId) {
        return new Acl(userId, true, true, true, true);
    }
}
