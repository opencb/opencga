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

package org.opencb.opencga.app.cli.main.executors.catalog.commons;

import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.rest.catalog.CatalogClient;

import java.io.IOException;

/**
 * Created by pfurio on 27/07/16.
 */
public class AclCommandExecutor<T> {

    // We put .replace("/",":") because there are some pathParams such as in files where "/" cannot be sent in the url. Instead, we will
    // change it for :

    public DataResponse<ObjectMap> acls(AclCommandOptions.AclsCommandOptions aclCommandOptions, CatalogClient<T> client)
            throws CatalogException,IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", aclCommandOptions.study);
        params.putIfNotEmpty("member", aclCommandOptions.memberId);
        return client.getAcls(aclCommandOptions.id.replace("/", ":"), params);
    }

}
