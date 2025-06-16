package org.opencb.opencga.storage.core.metadata.models.project;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class VariantSecondaryAnnotationIndexSets {

    private List<SearchIndexMetadata> values;
    private long lastSolrUpdateTimestamp;
    private long solrUpdateTimeoutMillis;

    public VariantSecondaryAnnotationIndexSets() {
        values = new LinkedList<>();
    }

    public VariantSecondaryAnnotationIndexSets(List<SearchIndexMetadata> values) {
        this.values = values;
    }

    public List<SearchIndexMetadata> getValues() {
        return values;
    }

    public VariantSecondaryAnnotationIndexSets setValues(List<SearchIndexMetadata> values) {
        this.values = values;
        return this;
    }

    public SearchIndexMetadata getActiveIndex() {
        for (int i = values.size() - 1; i >= 0; i--) {
            SearchIndexMetadata indexSet = values.get(i);
            if (indexSet.getStatus() == SearchIndexMetadata.Status.ACTIVE) {
                return indexSet;
            }
        }
        return null;
    }

    public SearchIndexMetadata getLastStagingOrActiveIndex() {
        SearchIndexMetadata index = getActiveIndex();
        for (SearchIndexMetadata thisIndexMetadata : values) {
            if (thisIndexMetadata.getStatus() == SearchIndexMetadata.Status.STAGING) {
                if (index == null || index.getCreationDate().before(thisIndexMetadata.getCreationDate())) {
                    index = thisIndexMetadata;
                }
            }
        }
        return index;
    }

    public void addIndexMetadata(SearchIndexMetadata index) {
        this.values.add(index);
    }

    public List<SearchIndexMetadata> getDeprecatedIndexes() {
        List<SearchIndexMetadata> deprecatedIndexes = new LinkedList<>();
        for (SearchIndexMetadata indexSet : values) {
            if (indexSet.getStatus() == SearchIndexMetadata.Status.DEPRECATED) {
                deprecatedIndexes.add(indexSet);
            }
        }
        return deprecatedIndexes;
    }

    public SearchIndexMetadata getIndexMetadata(int version) {
        for (SearchIndexMetadata value : values) {
            if (value.getVersion() == version) {
                return value;
            }
        }
        return null;
    }

    public long getLastSolrUpdateTimestamp() {
        return lastSolrUpdateTimestamp;
    }

    public VariantSecondaryAnnotationIndexSets setLastSolrUpdateTimestamp(long lastSolrUpdateTimestamp) {
        this.lastSolrUpdateTimestamp = lastSolrUpdateTimestamp;
        return this;
    }

    public long getSolrUpdateTimeoutMillis() {
        return solrUpdateTimeoutMillis;
    }

    public VariantSecondaryAnnotationIndexSets setSolrUpdateTimeoutMillis(long solrUpdateTimeoutMillis) {
        this.solrUpdateTimeoutMillis = solrUpdateTimeoutMillis;
        return this;
    }

    public VariantSecondaryAnnotationIndexSets refreshSolrUpdateTimestamp(long timeout, TimeUnit timeUnit) {
        this.lastSolrUpdateTimestamp = System.currentTimeMillis();
        this.solrUpdateTimeoutMillis = timeUnit.toMillis(timeout);
        return this;
    }

    public VariantSecondaryAnnotationIndexSets resetSolrUpdateTimestamp() {
        this.lastSolrUpdateTimestamp = 0;
        this.solrUpdateTimeoutMillis = 0;
        return this;
    }

    public boolean isSolrUpdateExpired() {
        return System.currentTimeMillis() > lastSolrUpdateTimestamp + solrUpdateTimeoutMillis;
    }

    public boolean isSolrUpdateInProgress() {
        return lastSolrUpdateTimestamp > 0 && !isSolrUpdateExpired();
    }

}
