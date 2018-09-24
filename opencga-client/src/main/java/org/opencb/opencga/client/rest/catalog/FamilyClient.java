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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.core.models.Family;
import org.opencb.opencga.core.models.acls.permissions.FamilyAclEntry;

import java.io.IOException;

/**
 * Created by pfurio on 15/05/17.
 */
public class FamilyClient extends AnnotationClient<Family, FamilyAclEntry> {

    private static final String FAMILY_URL = "families";

    public FamilyClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = FAMILY_URL;
        this.clazz = Family.class;
        this.aclClass = FamilyAclEntry.class;
    }

    public QueryResponse<Family> create(String studyId, ObjectMap bodyParams) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY, studyId);
        params.putIfNotNull("body", bodyParams);
        return execute(FAMILY_URL, "create", params, POST, Family.class);
    }

    public QueryResponse<ObjectMap> groupBy(String studyId, String fields, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "study", studyId, "fields", fields);
        return execute(FAMILY_URL, "groupBy", params, GET, ObjectMap.class);
    }

    public QueryResponse<FacetedQueryResult> stats(String study, Query query, QueryOptions queryOptions) throws IOException {
        ObjectMap params = new ObjectMap(query);
        params.putAll(queryOptions);
        params.put("study", study);
        return execute(FAMILY_URL, "stats", params, GET, FacetedQueryResult.class);
    }

}
