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
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.models.PermissionRule;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.acls.permissions.AbstractAclEntry;

import java.util.List;

/**
 * Created by pfurio on 20/04/17.
 */
public interface AuthorizationDBAdaptor {

    /**
     * Retrieve the list of Acls for the list of members in the resource given.
     *
     * @param <E> AclEntry type.
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param members members for whom the Acls will be obtained.
     * @param entry Entity for which the ACLs will be retrieved.
     * @return the list of Acls defined for the members.
     * @throws CatalogException  CatalogException.
     */
    <E extends AbstractAclEntry> QueryResult<E> get(long resourceId, List<String> members, Entity entry) throws CatalogException;

    /**
     * Retrieve the list of Acls for the list of members in the resources given.
     *
     * @param <E> AclEntry type.
     * @param resourceIds ids of the study, file, sample... where the Acl will be looked for.
     * @param members members for whom the Acls will be obtained.
     * @param entry Entity for which the ACLs will be retrieved.
     * @return the list of Acls defined for the members.
     * @throws CatalogException  CatalogException.
     */
    <E extends AbstractAclEntry> List<QueryResult<E>> get(List<Long> resourceIds, List<String> members, Entity entry)
            throws CatalogException;

    /**
     * Remove all the Acls defined for the member in the resource for the study.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member member from whom the Acls will be removed.
     * @param entry Entity for which the ACLs will be retrieved.
     * @return WriteResult object.
     * @throws CatalogException  CatalogException.
     */
    WriteResult removeFromStudy(long studyId, String member, Entity entry) throws CatalogException;

    default WriteResult setToMembers(long studyId, List<Long> resourceIds, List<String> members, List<String> permissions, Entity entity)
            throws CatalogDBException {
        return setToMembers(studyId, resourceIds, null, members, permissions, entity, null);
    }

    WriteResult setToMembers(long studyId, List<Long> resourceIds, List<Long> resourceIds2, List<String> members, List<String> permissions,
                      Entity entity, Entity entity2) throws CatalogDBException;

    // Special method only to set acls in study
    WriteResult setToMembers(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogDBException;

    default WriteResult addToMembers(long studyId, List<Long> resourceIds, List<String> members, List<String> permissions, Entity entity)
            throws CatalogDBException {
        return addToMembers(studyId, resourceIds, null, members, permissions, entity, null);
    }

    WriteResult addToMembers(long studyId, List<Long> resourceIds, List<Long> resourceIds2, List<String> members, List<String> permissions,
                      Entity entity, Entity entity2) throws CatalogDBException;

    // Special method only to add acls in study
    WriteResult addToMembers(List<Long> studyIds, List<String> members, List<String> permissions) throws CatalogDBException;

    default WriteResult removeFromMembers(List<Long> resourceIds, List<String> members, List<String> permissions, Entity entity)
            throws CatalogDBException {
        return removeFromMembers(resourceIds, null, members, permissions, entity, null);
    }

    WriteResult removeFromMembers(List<Long> resourceIds, List<Long> resourceIds2, List<String> members, List<String> permissions,
                                  Entity entity, Entity entity2) throws CatalogDBException;

    WriteResult resetMembersFromAllEntries(long studyId, List<String> members) throws CatalogDBException;

    <E extends AbstractAclEntry> WriteResult setAcls(List<Long> resourceIds, List<E> acls, Entity entity) throws CatalogDBException;

    WriteResult applyPermissionRules(long studyId, PermissionRule permissionRule, Study.Entity entry) throws CatalogException;

    WriteResult removePermissionRuleAndRemovePermissions(Study study, String permissionRuleId, Study.Entity entry) throws CatalogException;

    WriteResult removePermissionRuleAndRestorePermissions(Study study, String permissionRuleToDeleteId, Study.Entity entity)
            throws CatalogException;

    WriteResult removePermissionRule(long studyId, String permissionRuleToDelete, Study.Entity entry) throws CatalogException;
}
