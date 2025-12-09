package org.opencb.opencga.storage.core.variant.index.sample.file;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.OriginalCall;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.core.config.storage.FieldConfiguration;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.core.variant.index.core.IndexField;
import org.opencb.opencga.storage.core.variant.index.sample.models.SampleIndexVariant;
import org.opencb.opencga.storage.core.variant.index.sample.schema.FileDataSchema;
import org.opencb.opencga.storage.core.variant.index.sample.schema.FileIndexSchema;
import org.opencb.opencga.storage.core.variant.index.sample.schema.SampleIndexSchema;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

public class SampleIndexVariantConverter {

    private final FileIndexSchema fileIndex;
    private final FileDataSchema fileDataSchema;

    public SampleIndexVariantConverter(SampleIndexSchema configuration) {
        fileIndex = configuration.getFileIndex();
        fileDataSchema = configuration.getFileData();
    }

    public SampleIndexVariant createSampleIndexVariant(int sampleIdx, int filePosition, Variant variant) {
        // Expecting only one study and only one file
        StudyEntry study = variant.getStudies().get(0);
        FileEntry file = study.getFiles().get(0);

        BitBuffer fileIndexValue =  createFileIndexValue(variant.getType(), filePosition, file.getData(),
                study.getSampleDataKeyPositions(), study.getSampleData(sampleIdx));
        ByteBuffer fileDataIndexValue = createFileDataIndexValue(variant, filePosition, file.getCall(),
                study.getSecondaryAlternates());

        return new SampleIndexVariant(variant, fileIndexValue, fileDataIndexValue);
    }

    public SampleIndexVariant createSampleIndexVariant(
            int filePosition, Variant variant, OriginalCall call, List<AlternateCoordinate> alts,
            Function<String, String> fileAttributes, Function<String, String> sampleData) {
        BitBuffer fileIndexValue =  createFileIndexValue(variant.getType(), filePosition, fileAttributes, sampleData);
        ByteBuffer fileDataIndexValue = createFileDataIndexValue(variant, filePosition, call,
                alts);

        return new SampleIndexVariant(variant, fileIndexValue, fileDataIndexValue);
    }

    /**
     * Create the FileIndex value for this specific sample and variant.
     *
     * @param sampleIdx Sample position in the StudyEntry. Used to get the DP from the format.
     * @param filePosition   In case of having multiple files for the same sample, the cardinal value of the load order of the file.
     * @param variant   Full variant.
     * @return 16 bits of file index.
     */
    protected BitBuffer createFileIndexValue(int sampleIdx, int filePosition, Variant variant) {
        // Expecting only one study and only one file
        StudyEntry study = variant.getStudies().get(0);
        FileEntry file = study.getFiles().get(0);

        return createFileIndexValue(variant.getType(), filePosition, file.getData(),
                study.getSampleDataKeyPositions(), study.getSampleData(sampleIdx));
    }

    /**
     * Create the FileIndex value for this specific sample and variant.
     *
     * @param type           Variant type
     * @param filePosition   In case of having multiple files for the same sample, the cardinal value of the load order of the file.
     * @param fileAttributes File attributes
     * @param sampleDataKeyPositions          Sample data key positions
     * @param sampleData     Sample data values
     * @return BitBuffer of file index.
     */
    protected BitBuffer createFileIndexValue(VariantType type, int filePosition, Map<String, String> fileAttributes,
                                           Map<String, Integer> sampleDataKeyPositions, List<String> sampleData) {
        return createFileIndexValue(type, filePosition, fileAttributes::get, (key) -> {
            Integer position = sampleDataKeyPositions.get(key);
            return position == null ? null : sampleData.get(position);
        });
    }

    /**
     * Create the FileIndex value for this specific sample and variant.
     *
     * @param type           Variant type
     * @param filePosition   In case of having multiple files for the same sample, the cardinal value of the load order of the file.
     * @param fileAttributes File attributes
     * @param sampleData     Sample data values
     * @return BitBuffer of file index.
     */
    private BitBuffer createFileIndexValue(VariantType type, int filePosition, Function<String, String> fileAttributes,
                                           Function<String, String> sampleData) {
        BitBuffer bitBuffer = new BitBuffer(fileIndex.getBitsLength());

//        setMultiFile(bos, false);
        fileIndex.getFilePositionIndex().write(filePosition, bitBuffer);
        fileIndex.getTypeIndex().write(type, bitBuffer);

        for (IndexField<String> fileDataIndexField : fileIndex.getCustomFields()) {
            String key = fileDataIndexField.getKey();
            String value;
            if (fileDataIndexField.getSource() == FieldConfiguration.Source.FILE) {
                value = fileAttributes.apply(key);
            } else if (fileDataIndexField.getSource() == FieldConfiguration.Source.SAMPLE) {
                value = sampleData.apply(key);
            } else {
                throw new IllegalArgumentException("Unable to build file index with index source "
                        + fileDataIndexField.getSource()
                        + " for index "
                        + fileDataIndexField.getId());
            }
            fileDataIndexField.write(value, bitBuffer);
        }
        return bitBuffer;
    }

    /**
     * Create the FileIndex value for this specific sample and variant.
     *
     * @param filePosition   In case of having multiple files for the same sample, the cardinal value of the load order of the file.
     * @param call           Original call
     * @param secondaryAlternates Secondary alternates
     * @return BitBuffer of file index.
     */
    private ByteBuffer createFileDataIndexValue(Variant variant, int filePosition, OriginalCall call,
                                                List<AlternateCoordinate> secondaryAlternates) {
//        if (fileDataIndex.isSparse()) {
//        }
        int fileDataSize = 0;
        if (fileDataSchema.isIncludeOriginalCall()) {
            fileDataSize += fileDataSchema.getOriginalCallField().getByteLength(variant, call);
        }
        if (fileDataSchema.isIncludeSecondaryAlternates()) {
            fileDataSize += fileDataSchema.getSecondaryAlternatesField().getByteLength(variant, secondaryAlternates);
        }
        if (fileDataSize == 0) {
            return null;
        }
        ByteBuffer bb = ByteBuffer.allocate(fileDataSize);
        if (fileDataSchema.isIncludeOriginalCall()) {
            fileDataSchema.getOriginalCallField().write(variant, call, bb);
        }
        if (fileDataSchema.isIncludeSecondaryAlternates()) {
            fileDataSchema.getSecondaryAlternatesField().write(variant, secondaryAlternates, bb);
        }
        return bb;
    }

    public BitBuffer addSampleDataIndexValues(BitBuffer bitBuffer, Map<String, Integer> sampleDataKeyPositions,
                                              IntFunction<String> sampleData) {
        for (IndexField<String> fileDataIndexField : fileIndex.getCustomFieldsSourceSample()) {
            String key = fileDataIndexField.getKey();
            Integer position = sampleDataKeyPositions.get(key);
            if (position != null) {
                String value = sampleData.apply(position);
                fileDataIndexField.write(value, bitBuffer);
            }
        }
        return bitBuffer;
    }

    private int getDp(Map<String, String> fileAttributes, String dpStr) {
        int dp;
        if (StringUtils.isEmpty(dpStr)) {
            dpStr = fileAttributes.get(VCFConstants.DEPTH_KEY);
        }
        if (StringUtils.isNumeric(dpStr)) {
            dp = Integer.parseInt(dpStr);
        } else {
            dp = 0;
        }
        return dp;
    }

    private double getQual(Map<String, String> fileAttributes) {
        String qualStr = fileAttributes.get(StudyEntry.QUAL);
        double qual;
        try {
            if (qualStr == null || qualStr.isEmpty() || ".".equals(qualStr)) {
                qual = 0;
            } else {
                qual = Double.parseDouble(qualStr);
            }
        } catch (NumberFormatException e) {
            qual = 0;
        }
        return qual;
    }

}
