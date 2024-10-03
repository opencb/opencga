package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.io.bit.BitInputStream;

import java.util.List;

public abstract class FixedSizeIndexSchema extends IndexSchema {

    private int indexSizeBits;

    protected FixedSizeIndexSchema() {
    }

    public FixedSizeIndexSchema(List<IndexField<?>> fields) {
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

    public int getBitsLength() {
        return indexSizeBits;
    }

    /**
     * Read index element from the bit input stream.
     * @param buffer BitBuffer
     * @param i element position
     * @return BitBuffer containing all fields from the index.
     */
    public BitBuffer readEntry(BitBuffer buffer, int i) {
        return buffer.getBitBuffer(i * indexSizeBits, indexSizeBits);
    }

    /**
     * Read next index element from the bit input stream.
     * @param stream BitBuffer
     * @return BitBuffer containing all fields from the index.
     */
    public BitBuffer readEntry(BitInputStream stream) {
        return stream.readBitBuffer(getBitsLength());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FixedSizeIndexSchema{");
        sb.append("indexSizeBits=").append(indexSizeBits);
        sb.append(", fields=").append(fields);
        sb.append('}');
        return sb.toString();
    }
}
