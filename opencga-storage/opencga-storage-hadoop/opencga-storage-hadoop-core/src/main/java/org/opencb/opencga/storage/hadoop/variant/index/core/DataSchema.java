package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Data schema.
 * This class contains the schema of the data stored in the index. The schema is defined by a set of fields.
 * <p>
 * The generated data is stored in a ByteBuffer, and this class is used to read and write the data.
 * The ByteBuffer contains a set of entries, each entry contains a set of fields.
 * <p>
 * The fields of each entry are stored in the same order as they are added to the schema.
 * <p>
 *  - ByteBuffer
 *    - Entry 1
 *      - Entry length
 *      - Field 1
 *      - ...
 *      - Field n
 *    - ...
 *    - Entry n
 */
public abstract class DataSchema {

    private List<DataField<?>> fields;
    protected final DataField<Integer> entryLengthField;

//    private boolean sparse = false;

    public DataSchema() {
        fields = new ArrayList<>();
        entryLengthField = new IntegerDataField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "ENTRY_LENGTH", null));
        fields.add(entryLengthField);
    }

    protected void addField(DataField<?> field) {
        field.setFieldPosition(fields.size());
        fields.add(field);
    }

//    public boolean isSparse() {
//        return sparse;
//    }

    public DataField<?> getField(IndexFieldConfiguration.Source source, String key) {
        return fields.stream().filter(i -> i.getSource() == source && i.getKey().equals(key)).findFirst().orElse(null);
    }

    public List<DataField<?>> getFields() {
        return fields;
    }

    public void writeEntry(ByteBuffer buffer, ByteBuffer entryBuffer) {
        int entryLength = entryBuffer.limit();
        entryLengthField.write(entryLength, buffer);
        buffer.put(entryBuffer.array(), buffer.arrayOffset(), entryLength);
    }

    public void writeEntry(ByteArrayOutputStream stream, ByteBuffer entryBuffer) {
        int entryLength = entryBuffer.limit();
        entryLengthField.write(entryLength, stream);
        stream.write(entryBuffer.array(), entryBuffer.arrayOffset(), entryLength);
    }

    public ByteBuffer readEntry(ByteBuffer buffer, int entryPosition) {
        try {
            buffer.rewind();
            for (int i = 0; i < entryPosition; i++) {
                if (!buffer.hasRemaining()) {
                    return ByteBuffer.allocate(0);
                }
                int entryLength = entryLengthField.readAndDecode(buffer);
                buffer.position(buffer.position() + entryLength);
            }
            return readNextEntry(buffer);
        } catch (Exception e) {
            throw e;
        }
    }

    public ByteBuffer readNextEntry(ByteBuffer buffer) {
        try {
            if (!buffer.hasRemaining()) {
                return ByteBuffer.allocate(0);
            }
            int elementSize = entryLengthField.readAndDecode(buffer);
            ByteBuffer elementBuffer = ByteBuffer.allocate(elementSize);
            buffer.get(elementBuffer.array(), elementBuffer.arrayOffset(), elementSize);
            elementBuffer.rewind();
            return elementBuffer;
        } catch (Exception e) {
            throw e;
        }
    }

    public ByteBuffer readField(ByteBuffer buffer, int fieldPosition) {
        buffer.rewind();
        for (DataField<?> field : fields) {
            if (field == entryLengthField) {
                // Skip entry length field
                continue;
            } else if (field.getFieldPosition() == fieldPosition) {
                return field.read(buffer);
            } else {
                field.move(buffer);
            }
        }
        throw new IllegalArgumentException("Unknown field position " + fieldPosition);
    }

    public <T> T readField(ByteBuffer buffer, DataField<T> field) {
        buffer.rewind();
        for (DataField<?> thisField : fields) {
            if (thisField == entryLengthField) {
                // Skip entry length field
                continue;
            } else if (thisField == field) {
                return field.readAndDecode(buffer);
            } else {
                thisField.move(buffer);
            }
        }
        throw new IllegalArgumentException("Unknown field " + field);
    }

}
