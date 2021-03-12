package org.opencb.opencga.storage.hadoop.variant.index.sample;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.io.bit.BitBuffer;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexField;
import org.opencb.opencga.storage.core.config.IndexFieldConfiguration;

import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

public class VariantFileIndexConverter {

    private final FileIndexSchema fileIndex;

    public VariantFileIndexConverter(SampleIndexSchema configuration) {
        fileIndex = configuration.getFileIndex();
    }

    /**
     * Create the FileIndex value for this specific sample and variant.
     *
     * @param sampleIdx Sample position in the StudyEntry. Used to get the DP from the format.
     * @param filePosition   In case of having multiple files for the same sample, the cardinal value of the load order of the file.
     * @param variant   Full variant.
     * @return 16 bits of file index.
     */
    public BitBuffer createFileIndexValue(int sampleIdx, int filePosition, Variant variant) {
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
    public BitBuffer createFileIndexValue(VariantType type, int filePosition, Map<String, String> fileAttributes,
                                           Map<String, Integer> sampleDataKeyPositions, List<String> sampleData) {
        BitBuffer bitBuffer = new BitBuffer(fileIndex.getBitsLength());

//        setMultiFile(bos, false);
        fileIndex.getFilePositionIndex().write(filePosition, bitBuffer);
        fileIndex.getTypeIndex().write(type, bitBuffer);

        for (IndexField<String> fileDataIndexField : fileIndex.getCustomFields()) {
            String key = fileDataIndexField.getKey();
            String value;
            if (fileDataIndexField.getSource().equals(IndexFieldConfiguration.Source.FILE)) {
                value = fileAttributes.get(key);
            } else if (fileDataIndexField.getSource().equals(IndexFieldConfiguration.Source.SAMPLE)) {
                Integer position = sampleDataKeyPositions.get(key);
                if (position == null) {
                    value = null;
                } else {
                    value = sampleData.get(position);
                }
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
