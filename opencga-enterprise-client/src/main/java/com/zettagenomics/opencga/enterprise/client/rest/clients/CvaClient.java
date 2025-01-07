/*
* Copyright 2015-2024 OpenCB
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

package com.zettagenomics.opencga.enterprise.client.rest.clients;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.client.ParentClient;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.core.exceptions.ClientException;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.RestResponse;


public class CvaClient extends ParentClient {

    public CvaClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Get sample information.
     * @param caseId Comma separated list sample IDs or UUIDs up to a maximum of 100.
     * @param params Map containing any of the following optional parameters.
     *       include: Fields included in the response, whole JSON path must be provided.
     *       exclude: Fields excluded in the response, whole JSON path must be provided.
     *       includeIndividual: Include Individual object as an attribute.
     *       flattenAnnotations: Flatten the annotations?.
     *       study: Study [[organization@]project:]study where study and project can be either the ID or UUID.
     *       version: Comma separated list of sample versions. 'all' to get all the sample versions. Not supported if multiple sample ids
     *            are provided.
     *       deleted: Boolean to retrieve deleted entries.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> info(String caseId, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("cva", caseId, null, null, "info", params, GET, Sample.class);
    }
}
