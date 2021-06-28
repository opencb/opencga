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
     * @param stream Bit input stream
     * @param i element position
     * @return BitBuffer containing all fields from the index.
     */
    public BitBuffer read(BitInputStream stream, int i) {
        return stream.getBitBuffer(i * indexSizeBits, indexSizeBits);
    }

    public int readValue(BitInputStream stream, int i) {
        return stream.getIntPartial(i * indexSizeBits, indexSizeBits);
    }
}
