package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

import java.util.List;

/**
 * Created by pfurio on 04/07/16.
 */
public interface AclDBAdaptor<T, U> extends DBAdaptor<T> {

    /**
     * Creates a new Acl.
     *
     * @param id id.
     * @param acl Acl.
     * @return the new created Acl.
     * @throws CatalogDBException if there is any internal error.
     */
    QueryResult<U> createAcl(long id, U acl) throws CatalogDBException;

    /**
     * Obtains the acl given the following parameters. If only the id is given, a list containing all the acls will be returned.
     *
     * @param id id.
     * @param members List of members to look for permissions. Can only be existing users or groups.
     * @return A queryResult object containing a list of acls that satisfies the query.
     * @throws CatalogDBException when the id does not exist or the members introduced do not exist in the database.
     */
    QueryResult<U> getAcl(long id, List<String> members) throws CatalogDBException;

    /**
     * Removes the Acl of the member.
     *
     * @param id id.
     * @param member member whose permissions will be taken out.
     * @throws CatalogDBException when there is an internal error.
     */
    void removeAcl(long id, String member) throws CatalogDBException;

    /**
     * Adds the permissions to the member getting rid of the former permissions the member might have had.
     *
     * @param id id.
     * @param member member.
     * @param permissions new list of permissions to be applied.
     * @return a Acl with the new set of permissions.
     * @throws CatalogDBException when there is an internal error.
     */
    QueryResult<U> setAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException;

    /**
     * Adds new permissions to the former list of permissions the member had.
     *
     * @param id  id.
     * @param member member.
     * @param permissions new permissions that will be added.
     * @return a Acl after the permissions update.
     * @throws CatalogDBException when there is an internal error.
     */
    QueryResult<U> addAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException;

    /**
     * Remove the permissions passed from the ACLs the member had.
     *
     * @param id  id.
     * @param member member.
     * @param permissions List of permissions that will be taken out from the list of permissions of the member.
     * @return an Acl after the removal of permissions.
     * @throws CatalogDBException when there is an internal error.
     */
    QueryResult<U> removeAclsFromMember(long id, String member, List<String> permissions) throws CatalogDBException;

}
