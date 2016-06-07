/*
 * Copyright 2015 OpenCB
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
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Variable;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class VariableClient extends AbstractParentClient<Variable> {

    private static final String VARIABLES_URL = "variables";

    protected VariableClient(String sessionId, ClientConfiguration configuration) {
        super(sessionId, configuration);

        this.category = VARIABLES_URL;
        this.clazz = Variable.class;
    }

    public QueryResponse<Variable> create(String studyId, String variableSetName, ObjectMap params) throws CatalogException, IOException {
        addParamsToObjectMap(params, "studyId", studyId, "name", variableSetName);
        return execute(VARIABLES_URL, "create", params, Variable.class);
    }

    public QueryResponse<Variable> addVariable(String variableSetid, ObjectMap params) throws CatalogException, IOException {
        addParamsToObjectMap(params, "studyId", variableSetid);
        return execute(VARIABLES_URL, "field/add", params, Variable.class);
    }

}
