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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.panel.PanelAclUpdateParams;
import org.opencb.opencga.core.models.panel.PanelCreateParams;
import org.opencb.opencga.core.models.panel.PanelUpdateParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Panel webservices.
 *    Client version: 2.0.0
 *    PATH: panels
 */
public class PanelClient extends AbstractParentClient {

    public PanelClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Panel info.
     * @param panels Comma separated list of panel ids up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Panel> info(String panels, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("panels", panels, null, null, "info", params, GET, Panel.class);
    }

    /**
     * Returns the acl of the panels. If member is provided, it will only return the acl for the member.
     * @param panels Comma separated list of panel ids up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String panels, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("panels", panels, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to update the permissions.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, PanelAclUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("panels", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Update panel attributes.
     * @param panels Comma separated list of panel ids.
     * @param data Panel parameters.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Panel> update(String panels, PanelUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("panels", panels, null, null, "update", params, POST, Panel.class);
    }

    /**
     * Delete existing panels.
     * @param panels Comma separated list of panel ids.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Panel> delete(String panels, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("panels", panels, null, null, "delete", params, DELETE, Panel.class);
    }

    /**
     * Create a panel.
     * @param data Panel parameters.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Panel> create(PanelCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("panels", null, null, null, "create", params, POST, Panel.class);
    }

    /**
     * Panel search.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Panel> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("panels", null, null, null, "search", params, GET, Panel.class);
    }
}
