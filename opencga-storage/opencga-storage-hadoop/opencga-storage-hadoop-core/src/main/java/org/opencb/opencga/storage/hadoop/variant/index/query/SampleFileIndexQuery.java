package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.sample.FileIndexSchema;

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

    public IndexFieldFilter getVariantTypeFilter() {
        return getFilter(IndexFieldConfiguration.Source.VARIANT, FileIndexSchema.TYPE_KEY);
    }

    public IndexFieldFilter getFilePositionFilter() {
        return getFilter(IndexFieldConfiguration.Source.META, FileIndexSchema.FILE_POSITION_KEY);
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
