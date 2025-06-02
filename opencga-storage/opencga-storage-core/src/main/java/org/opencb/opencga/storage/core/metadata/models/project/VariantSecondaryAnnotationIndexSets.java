package org.opencb.opencga.storage.core.metadata.models.project;

import java.util.LinkedList;
import java.util.List;

public class VariantSecondaryAnnotationIndexSets {

    private List<SearchIndexMetadata> values;

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
        SearchIndexMetadata stagingIndex = null;
        for (SearchIndexMetadata thisIndexMetadata : values) {
            if (thisIndexMetadata.getStatus() == SearchIndexMetadata.Status.STAGING) {
                if (stagingIndex == null || stagingIndex.getCreationDate().before(thisIndexMetadata.getCreationDate())) {
                    stagingIndex = thisIndexMetadata;
                }
            }
        }
        if (stagingIndex == null) {
            return getActiveIndex();
        } else {
            return stagingIndex;
        }
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
}
