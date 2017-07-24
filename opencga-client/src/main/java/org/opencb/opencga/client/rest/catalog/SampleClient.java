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
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by imedina on 24/05/16.
 */
public class SampleClient extends AnnotationClient<Sample, SampleAclEntry> {

    private static final String SAMPLES_URL = "samples";

    public SampleClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = SAMPLES_URL;
        this.clazz = Sample.class;
        this.aclClass = SampleAclEntry.class;
    }

    public QueryResponse<Sample> create(String studyId, ObjectMap bodyParams) throws CatalogException, IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(SampleDBAdaptor.QueryParams.STUDY.key(), studyId);
        params.putIfNotNull("body", bodyParams);
        return execute(SAMPLES_URL, "create", params, POST, Sample.class);
    }

    public QueryResponse<Sample> loadFromPed(String studyId, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "study", studyId);
        return execute(SAMPLES_URL, "load", params, GET, Sample.class);
    }

    public QueryResponse<ObjectMap> groupBy(String studyId, String fields, ObjectMap params) throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "study", studyId, "fields", fields);
        return execute(SAMPLES_URL, "groupBy", params, GET, ObjectMap.class);
    }

}
