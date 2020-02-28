package org.opencb.opencga.core.models.user;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.file.File;

public class UserFilter {

    private String name;
    private String description;
    private File.Bioformat bioformat;
    private Query query;
    private QueryOptions options;

    public UserFilter() {
    }

    public UserFilter(String name, String description, File.Bioformat bioformat, Query query, QueryOptions options) {
        this.name = name;
        this.description = description;
        this.bioformat = bioformat;
        this.query = query;
        this.options = options;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Filter{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", bioformat=").append(bioformat);
        sb.append(", query=").append(query);
        sb.append(", options=").append(options);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public UserFilter setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public UserFilter setDescription(String description) {
        this.description = description;
        return this;
    }

    public File.Bioformat getBioformat() {
        return bioformat;
    }

    public UserFilter setBioformat(File.Bioformat bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public UserFilter setQuery(Query query) {
        this.query = query;
        return this;
    }

    public QueryOptions getOptions() {
        return options;
    }

    public UserFilter setOptions(QueryOptions options) {
        this.options = options;
        return this;
    }
}
