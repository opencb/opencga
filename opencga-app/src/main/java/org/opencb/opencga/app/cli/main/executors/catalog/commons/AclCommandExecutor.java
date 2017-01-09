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

package org.opencb.opencga.app.cli.main.executors.catalog.commons;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.client.rest.catalog.CatalogClient;
import org.opencb.opencga.client.rest.catalog.StudyClient;

import java.io.IOException;

/**
 * Created by pfurio on 27/07/16.
 */
public class AclCommandExecutor<T,U> {

    public QueryResponse<U> acls(AclCommandOptions.AclsCommandOptions aclCommandOptions, CatalogClient<T,U> client)
            throws CatalogException,IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", aclCommandOptions.study);
        return client.getAcls(aclCommandOptions.id, params);
    }

    public QueryResponse<U> aclsCreate(AclCommandOptions.AclsCreateCommandOptions aclCommandOptions, CatalogClient<T,U> client)
            throws CatalogException,IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", aclCommandOptions.study);
        params.putIfNotNull("permissions", aclCommandOptions.permissions);
        return client.createAcl(aclCommandOptions.id, aclCommandOptions.members, params);
    }

    public QueryResponse<U> aclMemberDelete(AclCommandOptions.AclsMemberDeleteCommandOptions aclCommandOptions,
                                            CatalogClient<T,U> client) throws CatalogException,IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", aclCommandOptions.study);
        return client.deleteAcl(aclCommandOptions.id, aclCommandOptions.memberId, params);
    }

    public QueryResponse<U> aclMemberInfo(AclCommandOptions.AclsMemberInfoCommandOptions aclCommandOptions,
                                          CatalogClient<T,U> client) throws CatalogException,IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", aclCommandOptions.study);
        return client.getAcl(aclCommandOptions.id, aclCommandOptions.memberId, params);
    }

    public QueryResponse<U> aclMemberUpdate(AclCommandOptions.AclsMemberUpdateCommandOptions aclCommandOptions,
                                            CatalogClient<T,U> client) throws CatalogException,IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", aclCommandOptions.study);
        params.putIfNotNull(StudyClient.AclParams.ADD_PERMISSIONS.key(), aclCommandOptions.addPermissions);
        params.putIfNotNull(StudyClient.AclParams.REMOVE_PERMISSIONS.key(), aclCommandOptions.removePermissions);
        params.putIfNotNull(StudyClient.AclParams.SET_PERMISSIONS.key(), aclCommandOptions.setPermissions);
        return client.updateAcl(aclCommandOptions.id, aclCommandOptions.memberId, params);
    }

}
