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
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclUpdateParams;
import org.opencb.opencga.core.models.individual.IndividualCreateParams;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Individual webservices.
 *    Client version: 2.0.0
 *    PATH: individuals
 */
public class IndividualClient extends AbstractParentClient {

    public IndividualClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Delete existing individuals.
     * @param individuals Comma separated list of individual ids.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Individual> delete(String individuals, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("individuals", individuals, null, null, "delete", params, DELETE, Individual.class);
    }

    /**
     * Update some individual attributes.
     * @param individuals Comma separated list of individual ids.
     * @param data params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Individual> update(String individuals, IndividualUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("individuals", individuals, null, null, "update", params, POST, Individual.class);
    }

    /**
     * Return the acl of the individual. If member is provided, it will only return the acl for the member.
     * @param individuals Comma separated list of individual names or ids up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String individuals, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("individuals", individuals, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to update the permissions. If propagate flag is set to true, it will propagate the
     *     permissions defined to the samples that are associated to the matching individuals.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, IndividualAclUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("individuals", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Fetch catalog individual stats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FacetField> aggregationStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("individuals", null, null, null, "aggregationStats", params, GET, FacetField.class);
    }

    /**
     * Get individual information.
     * @param individuals Comma separated list of individual names or ids up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Individual> info(String individuals, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("individuals", individuals, null, null, "info", params, GET, Individual.class);
    }

    /**
     * Search for individuals.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Individual> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("individuals", null, null, null, "search", params, GET, Individual.class);
    }

    /**
     * Update annotations from an annotationSet.
     * @param individual Individual ID or name.
     * @param annotationSet AnnotationSet id to be updated.
     * @param data Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove'
     *     containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset'
     *     containing the comma separated variables that will be set to the default value when the action is RESET.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Individual> updateAnnotations(String individual, String annotationSet, ObjectMap data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("individuals", individual, "annotationSets", annotationSet, "annotations/update", params, POST, Individual.class);
    }

    /**
     * Create individual.
     * @param data JSON containing individual information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Individual> create(IndividualCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("individuals", null, null, null, "create", params, POST, Individual.class);
    }
}
