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
import org.opencb.opencga.core.models.file.File;

public class FilterUpdateParams {

    private File.Bioformat bioformat;
    private String description;
    private Query query;
    private QueryOptions options;

    public FilterUpdateParams() {
    }

    public FilterUpdateParams(File.Bioformat bioformat, String description, Query query, QueryOptions options) {
        this.bioformat = bioformat;
        this.description = description;
        this.query = query;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FilterUpdateParams{");
        sb.append("bioformat=").append(bioformat);
        sb.append(", description='").append(description).append('\'');
        sb.append(", query=").append(query);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public File.Bioformat getBioformat() {
        return bioformat;
    }

    public FilterUpdateParams setBioformat(File.Bioformat bioformat) {
        this.bioformat = bioformat;
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
