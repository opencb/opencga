package org.opencb.opencga.storage.core.variant.index.core;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;

import java.util.Collections;

public abstract class SingleFieldIndexSchema<T> extends FixedSizeIndexSchema {

    private final IndexField<T> field;

    public SingleFieldIndexSchema(IndexField<T> field) {
        super(Collections.singletonList(field));
        this.field = field;
    }

    public IndexField<T> getField() {
        return field;
    }

    public int readFieldValue(BitBuffer buffer, int i) {
//        return getField().read(read(buffer, i));
        return buffer.getIntPartial(i * getBitsLength(), getBitsLength());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SingleFieldIndexSchema{");
        sb.append("field=").append(field);
        sb.append('}');
        return sb.toString();
    }
}
