package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;

public abstract class IndexFieldFilter {

    private final IndexField<?> indexField;

    protected IndexFieldFilter(IndexField<?> indexField) {
        this.indexField = indexField;
    }

//    public boolean readAndTest(BitInputStream bis) {
//        int code = bis.readIntPartial(index.getBitLength());
//        return test(code);
//    }

    public boolean readAndTest(BitBuffer bb) {
        int code = bb.getIntPartial(indexField.getBitOffset(), indexField.getBitLength());
        return test(code);
    }

    public abstract boolean test(int code);

    public IndexField<?> getIndex() {
        return indexField;
    }

    public abstract boolean isExactFilter();
}
