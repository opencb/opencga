package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.storage.core.config.IndexFieldConfiguration;

import java.util.List;

public abstract class IndexSchema {

    protected List<IndexField<?>> fields;
    private int indexSizeBits;

    protected IndexSchema() {

    }

    public IndexSchema(List<IndexField<?>> fields) {
        this.fields = fields;
        updateIndexSizeBits();
    }

    protected void updateIndexSizeBits() {
        int indexSizeBits = 0;
        for (IndexField<?> indexField : fields) {
            indexSizeBits += indexField.getBitLength();
        }
        this.indexSizeBits = indexSizeBits;
    }

    public IndexField<?> getField(IndexFieldConfiguration.Source source, String key) {
        return fields.stream().filter(i -> i.getSource().equals(source) && i.getKey().equals(key)).findFirst().orElse(null);
    }

    public List<IndexField<?>> getFields() {
        return fields;
    }

    public int getBitsLength() {
        return indexSizeBits;
    }
}
