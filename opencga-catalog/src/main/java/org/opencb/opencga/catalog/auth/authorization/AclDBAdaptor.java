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

package org.opencb.opencga.catalog.auth.authorization;

import org.bson.conversions.Bson;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.acls.permissions.AbstractAclEntry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Created by pfurio on 29/07/16.
 */
public interface AclDBAdaptor<T extends AbstractAclEntry> {

    /**
     * Register the Acl given in the resource.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be registered.
     * @param acl Acl object to be registered.
     * @return the same Acl once it is properly registered.
     * @throws CatalogDBException if any problem occurs during the insertion.
     */
    @Deprecated
    T createAcl(long resourceId, T acl) throws CatalogDBException;

    // FIXME: Bson should be changed for something like (long id). We are doing this at this point because we only have one
    // mongo implementation and we will need to create a new collection containing the permissions in order to clearly separate catalog
    // stuff from the authorization stuff.
    void setAcl(Bson bsonQuery, List<T> acl) throws CatalogDBException;

    /**
     * Retrieve the list of Acls for the list of members in the resource given.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param members members for whom the Acls will be obtained.
     * @return the list of Acls defined for the members.
     */
    List<T> getAcl(long resourceId, List<String> members);

    /**
     * Remove the existing Acl for the member.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param member member for whom the Acl will be removed.
     * @throws CatalogDBException if any problem occurs during the removal.
     */
    void removeAcl(long resourceId, String member) throws CatalogDBException;

    /**
     * Remove all the Acls defined for the member in the resource for the study.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member member from whom the Acls will be removed.
     * @throws CatalogDBException if any problem occurs during the removal.
     */
    void removeAclsFromStudy(long studyId, String member) throws CatalogDBException;

    /**
     * Set a new set of permissions for the member.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param member member for whom the permissions will be set.
     * @param permissions list of permissions.
     * @return the Acl with the changes applied.
     * @throws CatalogDBException if any problem occurs during the change of permissions.
     */
    @Deprecated
    T setAclsToMember(long resourceId, String member, List<String> permissions) throws CatalogDBException;

    /**
     * Add new permissions for the member.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param member member for whom the permissions will be added.
     * @param permissions list of permissions.
     * @return the Acl with the changes applied.
     * @throws CatalogDBException if any problem occurs during the change of permissions.
     */
    @Deprecated
    T addAclsToMember(long resourceId, String member, List<String> permissions) throws CatalogDBException;

    void addAclsToMembers(List<Long> resourceIds, List<String> members, List<String> permissions) throws CatalogDBException;

    /**
     * Remove some member permissions.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param member member for whom the permissions will be removed.
     * @param permissions list of permissions to be removed.
     * @return the Acl with the changes applied.
     * @throws CatalogDBException if any problem occurs during the change of permissions.
     */
    @Deprecated
    T removeAclsFromMember(long resourceId, String member, List<String> permissions) throws CatalogDBException;

    void removeAclsFromMembers(List<Long> resourceIds, List<String> members, @Nullable List<String> permissions) throws CatalogDBException;
}
