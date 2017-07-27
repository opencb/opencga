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

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.acls.permissions.DiseasePanelAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by pfurio on 10/06/16.
 */
public class PanelClient extends CatalogClient<DiseasePanel, DiseasePanelAclEntry> {
    private static final String PANEL_URL = "panels";

    public PanelClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = PANEL_URL;
        this.clazz = DiseasePanel.class;
        this.aclClass = DiseasePanelAclEntry.class;
    }

    public QueryResponse<DiseasePanel> create(String studyId, String name, String disease, ObjectMap params)
            throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "name", name, "disease", disease);
        return execute(PANEL_URL, "create", params, GET, DiseasePanel.class);
    }
}
