package org.opencb.opencga.storage.hadoop.variant.index.sample;

import htsjdk.variant.vcf.VCFConstants;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.hadoop.variant.index.core.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class FileIndex extends Index {

    private static final int DEFAULT_FILE_POSITION_SIZE_BITS = 4;

    private static final IndexField<Boolean> MULTI_FILE_INDEX = new CategoricalIndexField<>(
            new IndexFieldConfiguration(IndexFieldConfiguration.Source.FILE, "multiFile", IndexFieldConfiguration.Type.CATEGORICAL), 0,
            new Boolean[]{false, true}, false);

    private final List<IndexField<?>> fixedFields;
    private final List<IndexField<String>> customFields;
    private final IndexField<VariantType> typeIndex;
    private final IndexField<Integer> filePositionIndex;

    public FileIndex(SampleIndexConfiguration configuration) {
        this(configuration.getFileIndexFieldsConfiguration(), DEFAULT_FILE_POSITION_SIZE_BITS);
    }

    public FileIndex(List<IndexFieldConfiguration> customFieldConfigurations, int filePositionSizeBits) {
        filePositionIndex = buildFilePositionIndexField(MULTI_FILE_INDEX, filePositionSizeBits);
        typeIndex = buildVariantTypeIndexField(filePositionIndex);
        this.fixedFields = Arrays.asList(
                MULTI_FILE_INDEX,
                filePositionIndex,
                typeIndex);

        this.customFields = buildStringIndexFields(this.fixedFields, customFieldConfigurations);

        this.fields = new ArrayList<>(fixedFields.size() + customFields.size());
        this.fields.addAll(fixedFields);
        this.fields.addAll(customFields);

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

    public IndexField<Boolean> getMultiFileIndex() {
        return MULTI_FILE_INDEX;
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

    public static boolean isMultiFile(BitBuffer fileIndex) {
        return MULTI_FILE_INDEX.readAndDecode(fileIndex);
    }

    public static boolean isMultiFile(BitBuffer fileIndex, int bitOffset) {
        int code = fileIndex.getIntPartial(bitOffset + MULTI_FILE_INDEX.getBitOffset(), MULTI_FILE_INDEX.getBitLength());
        return MULTI_FILE_INDEX.decode(code);
    }

    public static void setMultiFile(BitBuffer bitBuffer, int bitOffset) {
        setMultiFile(bitBuffer, bitOffset, true);
    }

    public static void setMultiFile(BitBuffer bitBuffer, int bitOffset, boolean value) {
        bitBuffer.setIntPartial(MULTI_FILE_INDEX.encode(value),
                MULTI_FILE_INDEX.getBitOffset() + bitOffset, MULTI_FILE_INDEX.getBitLength());
    }

    @Deprecated
    public static void setFilePosition(BitBuffer fileIndex, int filePosition) {
        throw new UnsupportedOperationException("Deprecated!");
//        fileIndex.setBytePartial(((byte) filePosition), FILE_POSITION_SHIFT, FILE_POSITION_SIZE);
    }

    private CategoricalIndexField<VariantType> buildVariantTypeIndexField(IndexField<?> prevIndex) {
        return new CategoricalIndexField<>(
                new IndexFieldConfiguration(IndexFieldConfiguration.Source.VARIANT, VariantQueryParam.TYPE.key(),
                        IndexFieldConfiguration.Type.CATEGORICAL),
                prevIndex.getBitOffset() + prevIndex.getBitLength(),
                VariantTypeIndexCodec.TYPE_NUM_VALUES, new VariantTypeIndexCodec());
    }

    private static CategoricalIndexField<Integer> buildFilePositionIndexField(IndexField<?> prevIndex, int filePositionSize) {
        int maxValues = (1 << filePositionSize) - 1;
        return new CategoricalIndexField<>(
                new IndexFieldConfiguration(IndexFieldConfiguration.Source.META, "filePosition",
                        IndexFieldConfiguration.Type.CATEGORICAL),
                prevIndex.getBitOffset() + prevIndex.getBitLength(), maxValues,
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

    private static List<IndexField<String>> buildStringIndexFields(List<IndexField<?>> fixedIndexFields,
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
            IndexField<String> stringIndexField = buildStringIndexField(conf, bitOffset);
            list.add(stringIndexField);
            bitOffset += stringIndexField.getBitLength();
        }
        return list;
    }

    private static IndexField<String> buildStringIndexField(IndexFieldConfiguration conf, int bitOffset) {
        switch (conf.getType()) {
            case RANGE:
                return new RangeIndexField(conf, bitOffset, conf.getThresholds()).from(s -> {
                    try {
                        if (s == null || s.isEmpty() || s.equals(VCFConstants.MISSING_VALUE_v4)) {
                            return 0d;
                        } else {
                            return Double.parseDouble(s);
                        }
                    } catch (NumberFormatException e) {
                        return 0d;
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
