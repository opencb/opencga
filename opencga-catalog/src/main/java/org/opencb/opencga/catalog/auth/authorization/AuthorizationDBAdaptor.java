/*
 * Copyright 2015-2020 OpenCB
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

import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.study.PermissionRule;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;
import java.util.Map;

/**
 * Created by pfurio on 20/04/17.
 */
public interface AuthorizationDBAdaptor {

    /**
     * Retrieve the list of Acls for the list of members in the resource given.
     *
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param members    members for whom the Acls will be obtained.
     * @param entry      Entity for which the ACLs will be retrieved.
     * @return the list of Acls defined for the members.
     * @throws CatalogException CatalogException.
     */
    OpenCGAResult<Map<String, List<String>>> get(long resourceId, List<String> members, Enums.Resource entry) throws CatalogException;

    /**
     * Retrieve the list of Acls for the list of members in the resource given.
     *
     * @param studyUid   Study Uid.
     * @param resourceId id of the study, file, sample... where the Acl will be looked for.
     * @param members    members for whom the Acls will be obtained.
     * @param entry      Entity for which the ACLs will be retrieved.
     * @return the list of Acls defined for the members.
     * @throws CatalogException CatalogException.
     */
    OpenCGAResult<Map<String, List<String>>> get(long studyUid, String resourceId, List<String> members, Enums.Resource entry)
            throws CatalogException;


    /**
     * Retrieve the list of Acls for the list of members in the resources given.
     *
     * @param resourceIds ids of the study, file, sample... where the Acl will be looked for.
     * @param members     members for whom the Acls will be obtained.
     * @param entry       Entity for which the ACLs will be retrieved.
     * @return the list of Acls defined for the members.
     * @throws CatalogException CatalogException.
     */
    OpenCGAResult<Map<String, List<String>>> get(List<Long> resourceIds, List<String> members, Enums.Resource entry)
            throws CatalogException;

    /**
     * Remove all the Acls defined for the member in the resource for the study.
     *
     * @param studyId study id where the Acls will be removed from.
     * @param member  member from whom the Acls will be removed.
     * @param entry   Entity for which the ACLs will be retrieved.
     * @return OpenCGAResult object.
     * @throws CatalogException CatalogException.
     */
    OpenCGAResult removeFromStudy(long studyId, String member, Enums.Resource entry) throws CatalogException;

    OpenCGAResult setToMembers(long studyId, List<String> members, List<AuthorizationManager.CatalogAclParams> aclParams)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    // Special method only to set acls in study
    OpenCGAResult setToMembers(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult addToMembers(long studyId, List<String> members, List<AuthorizationManager.CatalogAclParams> aclParams)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    // Special method only to add acls in study
    OpenCGAResult addToMembers(List<Long> studyIds, List<String> members, List<String> permissions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult removeFromMembers(List<String> members, List<AuthorizationManager.CatalogAclParams> aclParams)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult resetMembersFromAllEntries(long studyId, List<String> members)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    @Deprecated
        // Use setAcls passing studyUid and String resourceIds
    OpenCGAResult<Map<String, List<String>>> setAcls(List<Long> resourceIds, Map<String, List<String>> acls, Enums.Resource resource)
            throws CatalogDBException;

    OpenCGAResult<Map<String, List<String>>> setAcls(long studyUid, List<String> resourceIds, Map<String, List<String>> acls,
                                                     Enums.Resource resource) throws CatalogDBException;

    OpenCGAResult applyPermissionRules(long studyId, PermissionRule permissionRule, Enums.Entity entry) throws CatalogException;

    OpenCGAResult removePermissionRuleAndRemovePermissions(Study study, String permissionRuleId, Enums.Entity entry)
            throws CatalogException;

    OpenCGAResult removePermissionRuleAndRestorePermissions(Study study, String permissionRuleToDeleteId, Enums.Entity entity)
            throws CatalogException;

    OpenCGAResult removePermissionRule(long studyId, String permissionRuleToDelete, Enums.Entity entry) throws CatalogException;
}
