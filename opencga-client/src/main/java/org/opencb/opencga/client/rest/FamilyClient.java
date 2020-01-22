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
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyAclUpdateParams;
import org.opencb.opencga.core.models.family.FamilyCreateParams;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Family webservices.
 *    Client version: 2.0.0
 *    PATH: families
 */
public class FamilyClient extends AbstractParentClient {

    public FamilyClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Update some family attributes.
     * @param families Comma separated list of family ids.
     * @param data params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Family> update(String families, FamilyUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("families", families, null, null, "update", params, POST, Family.class);
    }

    /**
     * Update annotations from an annotationSet.
     * @param family Family id.
     * @param annotationSet AnnotationSet id to be updated.
     * @param data Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove'
     *     containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset'
     *     containing the comma separated variables that will be set to the default value when the action is RESET.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Family> updateAnnotations(String family, String annotationSet, ObjectMap data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("families", family, "annotationSets", annotationSet, "annotations/update", params, POST, Family.class);
    }

    /**
     * Create family and the individual objects if they do not exist.
     * @param data JSON containing family information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Family> create(FamilyCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("families", null, null, null, "create", params, POST, Family.class);
    }

    /**
     * Get family information.
     * @param families Comma separated list of family IDs or names up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Family> info(String families, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("families", families, null, null, "info", params, GET, Family.class);
    }

    /**
     * Returns the acl of the families. If member is provided, it will only return the acl for the member.
     * @param families Comma separated list of family IDs or names up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String families, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("families", families, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to add ACLs.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, FamilyAclUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("families", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Fetch catalog family stats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FacetField> aggregationStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("families", null, null, null, "aggregationStats", params, GET, FacetField.class);
    }

    /**
     * Delete existing families.
     * @param families Comma separated list of family ids.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Family> delete(String families, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("families", families, null, null, "delete", params, DELETE, Family.class);
    }

    /**
     * Search families.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Family> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("families", null, null, null, "search", params, GET, Family.class);
    }
}
