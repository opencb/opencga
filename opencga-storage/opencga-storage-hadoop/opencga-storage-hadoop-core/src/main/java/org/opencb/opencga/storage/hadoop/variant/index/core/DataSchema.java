package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.io.bit.ExposedByteArrayOutputStream;

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

    private final List<DataFieldBase<?>> fields;
    protected final DataField<Integer> entryLengthField;
    private ByteBuffer defaultEntry;

//    private boolean sparse = false;

    public DataSchema() {
        fields = new ArrayList<>();
        entryLengthField = new VarIntDataField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "ENTRY_LENGTH", null));
        defaultEntry = ByteBuffer.allocate(0);
    }

    protected void addField(DataFieldBase<?> field) {
        fields.add(field);
        ExposedByteArrayOutputStream defaultEntryStream = new ExposedByteArrayOutputStream();
        for (DataFieldBase<?> dataField : fields) {
            writeDefaultValue(dataField, defaultEntryStream);
        }
        defaultEntry = defaultEntryStream.toByteByffer().asReadOnlyBuffer();
    }

    private static <T> void writeDefaultValue(DataFieldBase<T> dataField, ByteArrayOutputStream defaultEntry) {
        T defaultValue = dataField.getDefault();
        dataField.write(defaultValue, defaultEntry);
    }

    public DataFieldBase<?> getField(IndexFieldConfiguration.Source source, String key) {
        return fields.stream().filter(i -> i.getSource() == source && i.getKey().equals(key)).findFirst().orElse(null);
    }

    public List<DataFieldBase<?>> getFields() {
        return fields;
    }

    public void writeEntry(ByteBuffer buffer, ByteBuffer entryBuffer) {
        entryBuffer.rewind();
        if (isDefaultEntry(entryBuffer)) {
            // This is the default entry
            entryLengthField.write(0, buffer);
            return;
        }
        int entryLength = entryBuffer.limit();
        entryLengthField.write(entryLength, buffer);
        buffer.put(entryBuffer.array(), buffer.arrayOffset(), entryLength);
    }


    public void writeEntry(ByteArrayOutputStream stream, ByteBuffer entryBuffer) {
        entryBuffer.rewind();
        if (isDefaultEntry(entryBuffer)) {
            // This is the default entry
            entryLengthField.write(0, stream);
            return;
        }
        int entryLength = entryBuffer.limit();
        entryLengthField.write(entryLength, stream);
        stream.write(entryBuffer.array(), entryBuffer.arrayOffset(), entryLength);
    }

    private boolean isDefaultEntry(ByteBuffer entryBuffer) {
        return defaultEntry.limit() == entryBuffer.limit()
                && defaultEntry.compareTo(entryBuffer) == 0;
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
            int entryLength = entryLengthField.readAndDecode(buffer);
            if (entryLength == 0) {
                return defaultEntry;
            }
            ByteBuffer elementBuffer = ByteBuffer.allocate(entryLength);
            buffer.get(elementBuffer.array(), elementBuffer.arrayOffset(), entryLength);
            elementBuffer.rewind();
            return elementBuffer;
        } catch (Exception e) {
            throw e;
        }
    }

    public <T> T readFieldAndDecode(ByteBuffer buffer, DataField<T> field) {
        buffer.rewind();
        for (DataFieldBase<?> thisField : fields) {
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

    public <C, T> T readFieldAndDecode(ByteBuffer buffer, DataFieldWithContext<C, T> field, C context) {
        buffer.rewind();
        for (DataFieldBase<?> thisField : fields) {
            if (thisField == entryLengthField) {
                // Skip entry length field
                continue;
            } else if (thisField == field) {
                return field.readAndDecode(context, buffer);
            } else {
                thisField.move(buffer);
            }
        }
        throw new IllegalArgumentException("Unknown field " + field);
    }

    public ByteBuffer readField(ByteBuffer buffer, DataFieldBase<?> field) {
        buffer.rewind();
        for (DataFieldBase<?> thisField : fields) {
            if (thisField == entryLengthField) {
                // Skip entry length field
                continue;
            } else if (thisField == field) {
                return field.read(buffer);
            } else {
                thisField.move(buffer);
            }
        }
        throw new IllegalArgumentException("Unknown field " + field);
    }

}
