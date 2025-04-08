package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalMultiValuedIndexField;

import java.util.List;

public class MaskIndexFieldFilter extends IndexFieldFilter {

    private final int expectedMask;
    private final boolean exactFilter;

//    public <T> MaskIndexFieldFilter(CategoricalIndexField<T> index, T expectedValue) {
//        this(index, index.encode(expectedValue));
//    }

    public <T> MaskIndexFieldFilter(CategoricalMultiValuedIndexField<T> index, List<T> expectedValues) {
        this(index, index.encode(expectedValues));
    }

    public MaskIndexFieldFilter(CategoricalIndexField<?> index, int expectedMask) {
        super(index);
        int indexLength = index.getBitLength();
        int mask = BitBuffer.mask(indexLength);
        this.expectedMask = expectedMask & mask;
        exactFilter = !index.ambiguous(expectedMask);
    }

    @Override
    public boolean test(int code) {
        return IndexUtils.testIndexAny(code, expectedMask);
//        return (code & expectedCode) != 0;
//        return code == expectedCode;
    }

    @Override
    public boolean isExactFilter() {
        return exactFilter;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + getIndex().getId()
                + " exact:" + isExactFilter()
                + " (offset=" + getIndex().getBitOffset() + ", length=" + getIndex().getBitLength() + ")"
                + " : [ " + IndexUtils.binaryToString(expectedMask, getIndex().getBitLength()) + " ] }";
    }
}
