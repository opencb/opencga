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

package org.opencb.opencga.client.rest.catalog;


import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.catalog.models.acls.permissions.ToolAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;


/**
 * Created by sgallego on 6/30/16.
 */
public class ToolClient extends CatalogClient<Tool, ToolAclEntry> {

    private static final String TOOLS_URL = "tools";

    public ToolClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = TOOLS_URL;
        this.clazz = Tool.class;
        this.aclClass = ToolAclEntry.class;
    }
}
