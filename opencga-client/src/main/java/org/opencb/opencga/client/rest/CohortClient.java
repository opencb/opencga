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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Cohort;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.CohortAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class CohortClient extends AbstractParentClient<Cohort, CohortAclEntry> {

    private static final String COHORT_URL = "cohorts";

    protected CohortClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = COHORT_URL;
        this.clazz = Cohort.class;
        this.aclClass = CohortAclEntry.class;
    }

    public QueryResponse<Cohort> create(String studyId, String cohortName, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "name", cohortName);
        return execute(COHORT_URL, "create", params, GET, Cohort.class);
    }

    public QueryResponse<Object> getStats(String cohortId, Query query, QueryOptions options) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap(query);
        params.putAll(options);
        return execute(COHORT_URL, cohortId, "stats", params, GET, Object.class);
    }

    public QueryResponse<Sample> getSamples(String cohortId, QueryOptions options) throws CatalogException, IOException {
        return execute(COHORT_URL, cohortId, "samples", options, GET, Sample.class);
    }

    public QueryResponse<Cohort> annotate(String cohortId, String annotateSetName, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "annotateSetName", annotateSetName);
        return execute(COHORT_URL, cohortId, "annotate", params, GET, Cohort.class);
    }

    public QueryResponse<Cohort> groupBy(String studyId, String by, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "by", by);
        return execute(COHORT_URL, "groupBy", params, GET, Cohort.class);
    }

    public QueryResponse<Cohort> annotationSetsAllInfo(String cohortId, ObjectMap params) throws CatalogException, IOException {
        return execute(COHORT_URL, cohortId, "annotationSets", null, "info",  params, GET, Cohort.class);
    }

    public QueryResponse<Cohort> annotationSetsSearch(String cohortId, String annotationSetName, String variableSetId, ObjectMap params)
            throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "annotateSetName", annotationSetName, "variableSetId", variableSetId);
        return execute(COHORT_URL, cohortId, "annotationSets", null, "search", params, GET, Cohort.class);
    }

    public QueryResponse<Cohort> annotationSetsInfo(String cohortId, String annotationSetName, String variableSetId, ObjectMap params)
            throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "variableSetId", variableSetId);
        return execute(COHORT_URL, cohortId, "annotationSets", annotationSetName, "info", params, GET, Cohort.class);
    }

    public QueryResponse<Cohort> annotationSetsDelete(String cohortId, String annotationSetName, String variableSetId, ObjectMap params)
            throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "annotateSetName", annotationSetName, "variableSetId", variableSetId);
        return execute(COHORT_URL, cohortId, "annotationSets", annotationSetName, "delete", params, GET, Cohort.class);
    }

}
