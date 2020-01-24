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
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAclUpdateParams;
import org.opencb.opencga.core.models.sample.SampleCreateParams;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Sample webservices.
 *    Client version: 2.0.0
 *    PATH: samples
 */
public class SampleClient extends AbstractParentClient {

    public SampleClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Delete samples.
     * @param samples Comma separated list sample IDs or UUIDs up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> delete(String samples, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("samples", samples, null, null, "delete", params, DELETE, Sample.class);
    }

    /**
     * Sample search method.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("samples", null, null, null, "search", params, GET, Sample.class);
    }

    /**
     * Update some sample attributes.
     * @param samples Comma separated list sample IDs or UUIDs up to a maximum of 100.
     * @param data params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> update(String samples, SampleUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("samples", samples, null, null, "update", params, POST, Sample.class);
    }

    /**
     * Returns the acl of the samples. If member is provided, it will only return the acl for the member.
     * @param samples Comma separated list sample IDs or UUIDs up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String samples, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("samples", samples, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to update the permissions. If propagate flag is set to true, it will propagate the
     *     permissions defined to the individuals that are associated to the matching samples.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, SampleAclUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("samples", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Fetch catalog sample stats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FacetField> aggregationStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("samples", null, null, null, "aggregationStats", params, GET, FacetField.class);
    }

    /**
     * Update annotations from an annotationSet.
     * @param sample Sample id.
     * @param annotationSet AnnotationSet id to be updated.
     * @param data Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove'
     *     containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset'
     *     containing the comma separated variables that will be set to the default value when the action is RESET.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> updateAnnotations(String sample, String annotationSet, ObjectMap data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("samples", sample, "annotationSets", annotationSet, "annotations/update", params, POST, Sample.class);
    }

    /**
     * Get sample information.
     * @param samples Comma separated list sample IDs or UUIDs up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> info(String samples, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("samples", samples, null, null, "info", params, GET, Sample.class);
    }

    /**
     * Create sample.
     * @param data JSON containing sample information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> create(SampleCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("samples", null, null, null, "create", params, POST, Sample.class);
    }

    /**
     * Load samples from a ped file [EXPERIMENTAL].
     * @param file file.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Sample> load(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file", file);
        return execute("samples", null, null, null, "load", params, GET, Sample.class);
    }
}
