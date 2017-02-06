/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;

import javax.annotation.Nullable;
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
    @Deprecated
    QueryResult<U> createAcl(long id, U acl) throws CatalogDBException;

    /**
     * Creates new ACLs for the documents matching the query.
     *
     * @param query Query object.
     * @param aclEntryList List of ACLs to be created.
     * @throws CatalogDBException if there is any internal error.
     */
    void createAcl(Query query, List<U> aclEntryList) throws CatalogDBException;

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
    @Deprecated
    QueryResult<U> addAclsToMember(long id, String member, List<String> permissions) throws CatalogDBException;

    void addAclsToMember(Query query, List<String> members, List<String> permissions) throws CatalogDBException;

    /**
     * Remove the permissions passed from the ACLs the member had.
     *
     * @param id  id.
     * @param member member.
     * @param permissions List of permissions that will be taken out from the list of permissions of the member.
     * @return an Acl after the removal of permissions.
     * @throws CatalogDBException when there is an internal error.
     */
    @Deprecated
    QueryResult<U> removeAclsFromMember(long id, String member, List<String> permissions) throws CatalogDBException;

    void removeAclsFromMember(Query query, List<String> members, @Nullable List<String> permissions) throws CatalogDBException;

}
