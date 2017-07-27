/*
 * Copyright 2015-2017 OpenCB
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

import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.permissions.*;

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

    void resetMembersFromAllEntries(long studyId, List<String> members) throws CatalogDBException;

    <E extends AbstractAclEntry> void setAcls(List<Long> resourceIds, List<E> acls, String entity) throws CatalogDBException;
}
