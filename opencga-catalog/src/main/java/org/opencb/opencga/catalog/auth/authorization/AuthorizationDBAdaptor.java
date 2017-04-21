package org.opencb.opencga.catalog.auth.authorization;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by pfurio on 20/04/17.
 */
public interface AuthorizationDBAdaptor {

    /**
     * Retrieve the list of Acls for the list of members in the resource given.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param members members for whom the Acls will be obtained.
     * @param entity Entity for which the ACLs will be retrieved.
     * @param <E> AclEntry type.
     * @return the list of Acls defined for the members.
     * @throws CatalogException  CatalogException.
     */
    <E extends AbstractAclEntry> QueryResult<E> get(long resourceId, List<String> members, String entity) throws CatalogException;

    /**
     * Retrieve the list of Acls for the list of members in the resources given.
     *
     * @param resourceIds ids of the study, file, sample... where the Acl will be looked for.
     * @param members members for whom the Acls will be obtained.
     * @param entity Entity for which the ACLs will be retrieved.
     * @param <E> AclEntry type.
     * @return the list of Acls defined for the members.
     * @throws CatalogException  CatalogException.
     */
    <E extends AbstractAclEntry> List<QueryResult<E>> get(List<Long> resourceIds, List<String> members, String entity)
            throws CatalogException;

    /**
     * Remove all the Acls defined for the member in the resource for the study.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member member from whom the Acls will be removed.
     * @param entity Entity for which the ACLs will be retrieved.
     * @throws CatalogException  CatalogException.
     */
    void removeFromStudy(long studyId, String member, String entity) throws CatalogException;

    void setToMembers(List<Long> resourceIds, List<String> members, List<String> permissions, String entity) throws CatalogDBException;

    void addToMembers(List<Long> resourceIds, List<String> members, List<String> permissions, String entity) throws CatalogDBException;

    void removeFromMembers(List<Long> resourceIds, List<String> members, @Nullable List<String> permissions, String entity)
            throws CatalogDBException;

}
