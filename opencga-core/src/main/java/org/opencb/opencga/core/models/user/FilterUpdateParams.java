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

package org.opencb.opencga.core.models.user;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.common.Enums;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FilterUpdateParams {

    @DataField(description = ParamConstants.FILTER_UPDATE_PARAMS_RESOURCE_DESCRIPTION)
    private Enums.Resource resource;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.FILTER_UPDATE_PARAMS_QUERY_DESCRIPTION)
    private Query query;
    @DataField(description = ParamConstants.FILTER_UPDATE_PARAMS_OPTIONS_DESCRIPTION)
    private QueryOptions options;

    public FilterUpdateParams() {
    }

    public FilterUpdateParams(Enums.Resource resource, String description, Query query, QueryOptions options) {
        this.resource = resource;
        this.description = description;
        this.query = query;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FilterUpdateParams{");
        sb.append("resource=").append(resource);
        sb.append(", description='").append(description).append('\'');
        sb.append(", query=").append(query);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public Enums.Resource getResource() {
        return resource;
    }

    public FilterUpdateParams setResource(Enums.Resource resource) {
        this.resource = resource;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FilterUpdateParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public FilterUpdateParams setQuery(Query query) {
        this.query = query;
        return this;
    }

    public QueryOptions getOptions() {
        return options;
    }

    public FilterUpdateParams setOptions(QueryOptions options) {
        this.options = options;
        return this;
    }
}
