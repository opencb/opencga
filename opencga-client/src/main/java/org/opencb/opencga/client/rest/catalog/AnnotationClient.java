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
import org.opencb.opencga.catalog.models.AnnotationSet;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by pfurio on 28/07/16.
 */
public abstract class AnnotationClient<T, U> extends CatalogClient<T, U> {

    protected AnnotationClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    public QueryResponse<AnnotationSet> createAnnotationSet(String id, String variableSetId, String annotationSetName,
                                                            ObjectMap annotations) throws IOException {
        ObjectMap bodyParams = new ObjectMap();
        bodyParams.putIfNotEmpty("name", annotationSetName);
        bodyParams.putIfNotNull("annotations", annotations);

        ObjectMap params = new ObjectMap()
                .append("body", bodyParams)
                .append("variableSetId", variableSetId);
        return execute(category, id, "annotationsets", null, "create", params, POST, AnnotationSet.class);
    }

    public QueryResponse<AnnotationSet> getAnnotationSets(String id, ObjectMap params) throws IOException {
        return execute(category, id, "annotationsets", params, GET, AnnotationSet.class);
    }

    public QueryResponse<AnnotationSet> searchAnnotationSets(String id, ObjectMap params) throws IOException {
        return execute(category, id, "annotationsets", null, "search", params, GET, AnnotationSet.class);
    }

    public QueryResponse<AnnotationSet> deleteAnnotationSet(String id, String annotationSetName, ObjectMap params) throws IOException {
        return execute(category, id, "annotationsets", annotationSetName, "delete", params, GET, AnnotationSet.class);
    }

    public QueryResponse<AnnotationSet> updateAnnotationSet(String id, String annotationSetName, ObjectMap annotations) throws IOException {
        ObjectMap params = new ObjectMap("body", annotations);
        return execute(category, id, "annotationsets", annotationSetName, "update", params, POST, AnnotationSet.class);
    }

}
