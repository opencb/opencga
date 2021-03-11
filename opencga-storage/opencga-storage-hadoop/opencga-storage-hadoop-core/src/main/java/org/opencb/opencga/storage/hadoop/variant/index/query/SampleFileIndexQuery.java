package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.opencga.storage.core.config.IndexFieldConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;

import java.util.Collections;
import java.util.List;

public class SampleFileIndexQuery {

    private final String sampleName;
    private final List<IndexFieldFilter> filters;

    public SampleFileIndexQuery(String sampleName) {
        this(sampleName, Collections.emptyList());
    }

    public SampleFileIndexQuery(String sampleName, List<IndexFieldFilter> filters) {
        this.sampleName = sampleName;
        this.filters = filters;
    }

    public String getSampleName() {
        return sampleName;
    }

    public List<IndexFieldFilter> getFilters() {
        return filters;
    }

    public IndexFieldFilter getFilter(IndexFieldConfiguration.Source source, String key) {
        return filters.stream()
                .filter(i -> i.getIndex().getSource().equals(source) && i.getIndex().getKey().equals(key))
                .findFirst()
                .orElse(null);
    }

    public boolean isEmpty() {
        return filters.isEmpty();
    }
}
