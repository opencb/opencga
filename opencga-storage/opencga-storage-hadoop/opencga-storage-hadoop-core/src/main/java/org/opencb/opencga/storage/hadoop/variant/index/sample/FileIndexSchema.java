package org.opencb.opencga.storage.hadoop.variant.index.sample;

import htsjdk.variant.vcf.VCFConstants;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.core.config.storage.SampleIndexConfiguration;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.index.core.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;


public class FileIndexSchema extends FixedSizeIndexSchema {

    public static final String TYPE_KEY = VariantQueryParam.TYPE.key();
    public static final String FILE_POSITION_KEY = "filePosition";
    private final List<IndexField<?>> fixedFields;
    private final List<IndexField<String>> customFields;
    private final List<IndexField<String>> customFieldsSourceSample;
    private final IndexField<Boolean> multiFileIndex;
    private final IndexField<VariantType> typeIndex;
    private final IndexField<Integer> filePositionIndex;

    public FileIndexSchema(SampleIndexConfiguration.FileIndexConfiguration fileIndexConfiguration) {
        this(fileIndexConfiguration.getCustomFields(),
                fileIndexConfiguration.getFilePositionBits(),
                fileIndexConfiguration.isFixedFieldsFirst());
    }

    public FileIndexSchema(List<IndexFieldConfiguration> customFieldConfigurations, int filePositionSizeBits, boolean fixedFieldsFirst) {
        if (fixedFieldsFirst) {
            multiFileIndex = buildMultiFile(null);
            filePositionIndex = buildFilePositionIndexField(multiFileIndex, filePositionSizeBits);
            typeIndex = buildVariantTypeIndexField(filePositionIndex);
            this.fixedFields = Arrays.asList(
                    multiFileIndex,
                    filePositionIndex,
                    typeIndex);

            this.customFields = buildCustomIndexFields(this.fixedFields, customFieldConfigurations);
        } else {
            this.customFields = buildCustomIndexFields(Collections.emptyList(), customFieldConfigurations);

            multiFileIndex = buildMultiFile(customFields.get(customFields.size() - 1));
            filePositionIndex = buildFilePositionIndexField(multiFileIndex, filePositionSizeBits);
            typeIndex = buildVariantTypeIndexField(filePositionIndex);
            this.fixedFields = Arrays.asList(
                    multiFileIndex,
                    filePositionIndex,
                    typeIndex);

        }
        this.fields = new ArrayList<>(fixedFields.size() + customFields.size());
        this.fields.addAll(fixedFields);
        this.fields.addAll(customFields);
        customFieldsSourceSample = customFields.stream()
                .filter(c -> c.getSource().equals(IndexFieldConfiguration.Source.SAMPLE))
                .collect(Collectors.toList());

        updateIndexSizeBits();
    }

    public IndexField<String> getCustomField(IndexFieldConfiguration.Source source, String key) {
        return customFields.stream().filter(i -> i.getSource().equals(source) && i.getKey().equals(key)).findFirst().orElse(null);
    }

    public List<IndexField<?>> getFixedFields() {
        return fixedFields;
    }

    public List<IndexField<String>> getCustomFields() {
        return customFields;
    }

    public List<IndexField<String>> getCustomFieldsSourceSample() {
        return customFieldsSourceSample;
    }

    public IndexField<Boolean> getMultiFileIndex() {
        return multiFileIndex;
    }

    public IndexField<Integer> getFilePositionIndex() {
        return filePositionIndex;
    }

    public IndexField<VariantType> getTypeIndex() {
        return typeIndex;
    }

    public boolean hasMoreValues(BitBuffer bitBuffer) {
        return getMultiFileIndex().readAndDecode(bitBuffer);
    }

    public boolean isMultiFile(BitBuffer fileIndex) {
        return getMultiFileIndex().readAndDecode(fileIndex);
    }

    public boolean isMultiFile(BitBuffer fileIndex, int elementIndex) {
        int code = fileIndex.getIntPartial(
                (elementIndex * getBitsLength()) + getMultiFileIndex().getBitOffset(), getMultiFileIndex().getBitLength());
        return getMultiFileIndex().decode(code);
    }

    public void setMultiFile(BitBuffer bitBuffer, int bitOffset) {
        setMultiFile(bitBuffer, bitOffset, true);
    }

    public void setMultiFile(BitBuffer bitBuffer, int bitOffset, boolean value) {
        bitBuffer.setIntPartial(getMultiFileIndex().encode(value),
                getMultiFileIndex().getBitOffset() + bitOffset, getMultiFileIndex().getBitLength());
    }

    @Deprecated
    public static void setFilePosition(BitBuffer fileIndex, int filePosition) {
        throw new UnsupportedOperationException("Deprecated!");
//        fileIndex.setBytePartial(((byte) filePosition), FILE_POSITION_SHIFT, FILE_POSITION_SIZE);
    }

    protected CategoricalIndexField<Boolean> buildMultiFile(IndexField<?> prevIndex) {
        return new CategoricalIndexField<>(
                new IndexFieldConfiguration(IndexFieldConfiguration.Source.FILE, "multiFile", IndexFieldConfiguration.Type.CATEGORICAL)
                .setNullable(false),
                prevIndex == null ? 0 : (prevIndex.getBitOffset() + prevIndex.getBitLength()),
                new Boolean[]{false, true});
    }

    private CategoricalIndexField<VariantType> buildVariantTypeIndexField(IndexField<?> prevIndex) {
        return new CategoricalIndexField<>(
                new IndexFieldConfiguration(IndexFieldConfiguration.Source.VARIANT, TYPE_KEY,
                        IndexFieldConfiguration.Type.CATEGORICAL),
                prevIndex == null ? 0 : (prevIndex.getBitOffset() + prevIndex.getBitLength()),
                VariantTypeIndexCodec.TYPE_NUM_VALUES, new VariantTypeIndexCodec());
    }

    private static CategoricalIndexField<Integer> buildFilePositionIndexField(IndexField<?> prevIndex, int filePositionSize) {
        int maxValues = (1 << filePositionSize) - 1;
        return new CategoricalIndexField<>(
                new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, FILE_POSITION_KEY,
                        IndexFieldConfiguration.Type.CATEGORICAL),
                prevIndex == null ? 0 : (prevIndex.getBitOffset() + prevIndex.getBitLength()), maxValues,
                new IndexCodec<Integer>() {
                    @Override
                    public int encode(Integer value) {
                        if (value > maxValues) {
                            throw new IllegalArgumentException("Error converting filePosition."
                                    + " Unable to load more than " + maxValues + " files for the same sample.");
                        }
                        return value;
                    }

                    @Override
                    public Integer decode(int code) {
                        return code;
                    }

                    @Override
                    public boolean ambiguous(int code) {
                        return false;
                    }
                });
    }

    private static List<IndexField<String>> buildCustomIndexFields(List<IndexField<?>> fixedIndexFields,
                                                                   List<IndexFieldConfiguration> configurations) {
        int bitOffset = 0;
        for (IndexField<?> indexField : fixedIndexFields) {
            if (indexField.getBitOffset() != bitOffset) {
                throw new IllegalArgumentException("Wrong offset for fixed index field " + indexField.getId() + " ."
                        + " Expected " + bitOffset + " but got " + indexField.getBitOffset());
            }
            bitOffset += indexField.getBitLength();
        }

        List<IndexField<String>> list = new ArrayList<>();
        for (IndexFieldConfiguration conf : configurations) {
            IndexField<String> stringIndexField = buildCustomIndexField(conf, bitOffset);
            list.add(stringIndexField);
            bitOffset += stringIndexField.getBitLength();
        }
        return list;
    }

    private static IndexField<String> buildCustomIndexField(IndexFieldConfiguration conf, int bitOffset) {
        switch (conf.getType()) {
            case RANGE_LT:
            case RANGE_GT:
                return new RangeIndexField(conf, bitOffset).from(s -> {
                    try {
                        if (s == null || s.isEmpty() || s.equals(VCFConstants.MISSING_VALUE_v4)) {
                            return null;
                        } else {
                            return Double.parseDouble(s);
                        }
                    } catch (NumberFormatException e) {
                        return null;
                    }
                }, v -> v == null ? null : v.toString());
            case CATEGORICAL:
                return new CategoricalIndexField<>(conf, bitOffset, conf.getValues());
            case CATEGORICAL_MULTI_VALUE:
                if (conf.getSource() == IndexFieldConfiguration.Source.FILE && conf.getKey().equals(StudyEntry.FILTER)) {
                    return new CategoricalMultiValuedIndexField<>(conf, bitOffset, conf.getValues())
                            .from(s -> {
                                if (s == null || s.isEmpty() || s.equals(VCFConstants.MISSING_VALUE_v4)) {
                                    return Collections.emptyList();
                                } else {
                                    return Arrays.asList(s.split(VCFConstants.FILTER_CODE_SEPARATOR));
                                }
                            }, v -> v == null ? null : String.join(VCFConstants.FILTER_CODE_SEPARATOR, v));
                } else {
                    return new CategoricalMultiValuedIndexField<>(conf, bitOffset, conf.getValues())
                            .from(s -> {
                                if (s == null || s.isEmpty() || s.equals(VCFConstants.MISSING_VALUE_v4)) {
                                    return Collections.emptyList();
                                } else {
                                    return Arrays.asList(s.split(VCFConstants.INFO_FIELD_ARRAY_SEPARATOR));
                                }
                            }, v -> v == null ? null : String.join(",", v));
                }
            default:
                throw new IllegalArgumentException("Unknown index type '" + conf.getType() + "'");
        }
    }

}
