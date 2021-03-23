package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalIndexField;

public class SingleValueIndexFieldFilter extends IndexFieldFilter {

    private final int expectedCode;
    private final boolean exactFilter;

    public <T> SingleValueIndexFieldFilter(CategoricalIndexField<T> index, T expectedValue) {
        this(index, index.encode(expectedValue));
    }

    public SingleValueIndexFieldFilter(CategoricalIndexField<?> index, int expectedCode) {
        super(index);
        int indexLength = index.getBitLength();
        int mask = BitBuffer.mask(indexLength);
        this.expectedCode = expectedCode & mask;
        exactFilter = !index.ambiguous(expectedCode);
    }

    @Override
    public boolean test(int code) {
//        return IndexUtils.testIndexAny(code, expectedValue);
//        return (code & expectedCode) != 0;
        return code == expectedCode;
    }

    @Override
    public boolean isExactFilter() {
        return exactFilter;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + getIndex().getId()
                + " (offset=" + getIndex().getBitOffset() + ", length=" + getIndex().getBitLength() + ")"
                + " : [ " + IndexUtils.binaryToString(expectedCode, getIndex().getBitLength()) + " ] }";
    }
}
