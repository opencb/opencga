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

package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.cohort.CohortAclUpdateParams;
import org.opencb.opencga.core.models.cohort.CohortCreateParams;
import org.opencb.opencga.core.models.cohort.CohortUpdateParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Cohort webservices.
 *    Client version: 2.0.0
 *    PATH: cohorts
 */
public class CohortClient extends AbstractParentClient {

    public CohortClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Update some cohort attributes.
     * @param cohorts Comma separated list of cohort ids.
     * @param data params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Cohort> update(String cohorts, CohortUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("cohorts", cohorts, null, null, "update", params, POST, Cohort.class);
    }

    /**
     * Update annotations from an annotationSet.
     * @param cohort Cohort id.
     * @param annotationSet AnnotationSet id to be updated.
     * @param data Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove'
     *     containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset'
     *     containing the comma separated variables that will be set to the default value when the action is RESET.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Cohort> updateAnnotations(String cohort, String annotationSet, ObjectMap data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("cohorts", cohort, "annotationSets", annotationSet, "annotations/update", params, POST, Cohort.class);
    }

    /**
     * Return the acl of the cohort. If member is provided, it will only return the acl for the member.
     * @param cohorts Comma separated list of cohort names or ids up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String cohorts, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("cohorts", cohorts, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to add ACLs.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, CohortAclUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("cohorts", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Fetch catalog cohort stats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FacetField> aggregationStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("cohorts", null, null, null, "aggregationStats", params, GET, FacetField.class);
    }

    /**
     * Get cohort information.
     * @param cohorts Comma separated list of cohort names or ids up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Cohort> info(String cohorts, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("cohorts", cohorts, null, null, "info", params, GET, Cohort.class);
    }

    /**
     * Create a cohort.
     * @param data JSON containing cohort information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Cohort> create(CohortCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("cohorts", null, null, null, "create", params, POST, Cohort.class);
    }

    /**
     * Search cohorts.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Cohort> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("cohorts", null, null, null, "search", params, GET, Cohort.class);
    }

    /**
     * Delete cohorts.
     * @param cohorts Comma separated list of cohort ids.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Cohort> delete(String cohorts, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("cohorts", cohorts, null, null, "delete", params, DELETE, Cohort.class);
    }
}
