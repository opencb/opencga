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
 * The ByteBuffer contains a list of documents, each document contains a set of fieldValues.
 * <p>
 * The fields of each document are stored in the same order as they are added to the schema.
 * <p>
 *  - ByteBuffer
 *    - Doc 1
 *      - Doc length
 *      - FieldValue 1
 *      - ...
 *      - FieldValue n
 *    - ...
 *    - Doc n
 */
public abstract class DataSchema extends AbstractSchema<DataFieldBase<?>> {

    private final List<DataFieldBase<?>> fields;
    protected final DataField<Integer> documentLengthField;
    private ByteBuffer defaultDocument;

//    private boolean sparse = false;

    public DataSchema() {
        fields = new ArrayList<>();
        documentLengthField = new VarIntDataField(new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "DOC_LENGTH", null));
        defaultDocument = ByteBuffer.allocate(0);
    }

    protected void addField(DataFieldBase<?> field) {
        fields.add(field);
        ExposedByteArrayOutputStream defaultDocumentStream = new ExposedByteArrayOutputStream();
        for (DataFieldBase<?> dataField : fields) {
            writeDefaultValue(dataField, defaultDocumentStream);
        }
        defaultDocument = defaultDocumentStream.toByteByffer().asReadOnlyBuffer();
    }

    private static <T> void writeDefaultValue(DataFieldBase<T> dataField, ByteArrayOutputStream documentStream) {
        T defaultValue = dataField.getDefault();
        dataField.write(defaultValue, documentStream);
    }

    @Override
    public List<DataFieldBase<?>> getFields() {
        return fields;
    }

    public void writeDocument(ByteBuffer buffer, ByteBuffer docBuffer) {
        docBuffer.rewind();
        if (isDefaultDocument(docBuffer)) {
            // This is the default document
            documentLengthField.write(0, buffer);
            return;
        }
        int documentLength = docBuffer.limit();
        documentLengthField.write(documentLength, buffer);
        buffer.put(docBuffer.array(), buffer.arrayOffset(), documentLength);
    }


    public void writeDocument(ByteArrayOutputStream stream, ByteBuffer docBuffer) {
        docBuffer.rewind();
        if (isDefaultDocument(docBuffer)) {
            // This is the default document
            documentLengthField.write(0, stream);
            return;
        }
        int docLength = docBuffer.limit();
        documentLengthField.write(docLength, stream);
        stream.write(docBuffer.array(), docBuffer.arrayOffset(), docLength);
    }

    private boolean isDefaultDocument(ByteBuffer docBuffer) {
        return defaultDocument.limit() == docBuffer.limit()
                && defaultDocument.compareTo(docBuffer) == 0;
    }

    public ByteBuffer readDocument(ByteBuffer buffer, int docPosition) {
        try {
            buffer.rewind();
            for (int i = 0; i < docPosition; i++) {
                if (!buffer.hasRemaining()) {
                    return ByteBuffer.allocate(0);
                }
                int docLength = documentLengthField.readAndDecode(buffer);
                buffer.position(buffer.position() + docLength);
            }
            return readNextDocument(buffer);
        } catch (Exception e) {
            throw e;
        }
    }

    public ByteBuffer readNextDocument(ByteBuffer buffer) {
        try {
            if (!buffer.hasRemaining()) {
                return ByteBuffer.allocate(0);
            }
            int docLength = documentLengthField.readAndDecode(buffer);
            if (docLength == 0) {
                return defaultDocument;
            }
            ByteBuffer docBuffer = ByteBuffer.allocate(docLength);
            buffer.get(docBuffer.array(), docBuffer.arrayOffset(), docLength);
            docBuffer.rewind();
            return docBuffer;
        } catch (Exception e) {
            throw e;
        }
    }

    public <T> T readFieldAndDecode(ByteBuffer docBuffer, DataField<T> field) {
        docBuffer.rewind();
        for (DataFieldBase<?> thisField : fields) {
            if (thisField == documentLengthField) {
                // Skip document length field
                continue;
            } else if (thisField == field) {
                return field.readAndDecode(docBuffer);
            } else {
                thisField.move(docBuffer);
            }
        }
        throw new IllegalArgumentException("Unknown field " + field);
    }

    public <C, T> T readFieldAndDecode(ByteBuffer docBuffer, DataFieldWithContext<C, T> field, C context) {
        docBuffer.rewind();
        for (DataFieldBase<?> thisField : fields) {
            if (thisField == documentLengthField) {
                // Skip document length field
                continue;
            } else if (thisField == field) {
                return field.readAndDecode(context, docBuffer);
            } else {
                thisField.move(docBuffer);
            }
        }
        throw new IllegalArgumentException("Unknown field " + field);
    }

    public ByteBuffer readField(ByteBuffer docBuffer, DataFieldBase<?> field) {
        docBuffer.rewind();
        for (DataFieldBase<?> thisField : fields) {
            if (thisField == documentLengthField) {
                // Skip document length field
                continue;
            } else if (thisField == field) {
                return field.read(docBuffer);
            } else {
                thisField.move(docBuffer);
            }
        }
        throw new IllegalArgumentException("Unknown field " + field);
    }

}
