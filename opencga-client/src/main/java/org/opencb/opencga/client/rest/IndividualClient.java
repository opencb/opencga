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
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.acls.IndividualAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class IndividualClient extends AbstractParentClient<Individual, IndividualAclEntry> {

    private static final String INDIVIDUALS_URL = "individuals";

    protected IndividualClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = INDIVIDUALS_URL;
        this.clazz = Individual.class;
        this.aclClass = IndividualAclEntry.class;
    }

    public QueryResponse<Individual> create(String studyId, String individualName, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "name", individualName);
        return execute(INDIVIDUALS_URL, "create", params, GET, Individual.class);
    }

    public QueryResponse<Individual> annotate(String individualId, String annotateSetName, ObjectMap params)
            throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "annotateSetName", annotateSetName);
        return execute(INDIVIDUALS_URL, individualId, "annotate", params, GET, Individual.class);
    }

    public QueryResponse<Individual> groupBy(String studyId, String by, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "by", by);
        return execute(INDIVIDUALS_URL, "groupBy", params, GET, Individual.class);
    }

    public QueryResponse<Individual> annotationSetsAllInfo(String individualId, ObjectMap params) throws CatalogException, IOException {
        return execute(INDIVIDUALS_URL, individualId, "annotationSets", null, "info", params, GET, Individual.class);
    }

    public QueryResponse<Individual> annotationSetsSearch(String individualId, String annotationSetName, String variableSetId,
                                                          ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "annotateSetName", annotationSetName, "variableSetId", variableSetId);
        return execute(INDIVIDUALS_URL, individualId, "annotationSets", null, "search", params, GET, Individual.class);
    }

    public QueryResponse<Individual> annotationSetsInfo(String individualId, String annotationSetName, String variableSetId,
                                                        ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "variableSetId", variableSetId);
        return execute(INDIVIDUALS_URL, individualId, "annotationSets", annotationSetName, "info", params, GET, Individual.class);
    }

    public QueryResponse<Individual> annotationSetsDelete(String individualId, String annotationSetName, String variableSetId,
                                                          ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "annotateSetName", annotationSetName, "variableSetId", variableSetId);
        return execute(INDIVIDUALS_URL, individualId, "annotationSets", annotationSetName, "delete", params, GET, Individual.class);
    }

}
