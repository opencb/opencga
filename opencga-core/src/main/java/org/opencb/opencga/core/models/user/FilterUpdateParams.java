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
