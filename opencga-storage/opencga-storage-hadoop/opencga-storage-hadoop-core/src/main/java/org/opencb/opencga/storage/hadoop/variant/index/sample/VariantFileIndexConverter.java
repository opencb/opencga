package org.opencb.opencga.storage.hadoop.variant.index.sample;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Map;

public class VariantFileIndexConverter {

    public static final int FILE_POSITION_SIZE = 4;
    public static final int FILE_IDX_MAX = 1 << FILE_POSITION_SIZE;
    public static final int TYPE_SIZE = 3;
    public static final int QUAL_SIZE = 2;
    public static final int DP_SIZE = 3;

    public static final int FILE_POSITION_SHIFT = 1;
    public static final int TYPE_SHIFT = FILE_POSITION_SHIFT + FILE_POSITION_SIZE;
    public static final int FILTER_PASS_SHIFT = TYPE_SHIFT + TYPE_SIZE;
    public static final int QUAL_SHIFT = FILTER_PASS_SHIFT + 1;
    public static final int DP_SHIFT = QUAL_SHIFT + QUAL_SIZE;

    public static final short MULTI_FILE_MASK      = (short) (1 << 0);
    public static final short FILE_POSITION_1_MASK = (short) (1 << FILE_POSITION_SHIFT + 0);
    public static final short FILE_POSITION_2_MASK = (short) (1 << FILE_POSITION_SHIFT + 1);
    public static final short FILE_POSITION_3_MASK = (short) (1 << FILE_POSITION_SHIFT + 2);
    public static final short FILE_POSITION_4_MASK = (short) (1 << FILE_POSITION_SHIFT + 3);
    public static final short TYPE_1_MASK          = (short) (1 << TYPE_SHIFT);
    public static final short TYPE_2_MASK          = (short) (1 << TYPE_SHIFT + 1);
    public static final short TYPE_3_MASK          = (short) (1 << TYPE_SHIFT + 2);
    public static final short FILTER_PASS_MASK     = (short) (1 << FILTER_PASS_SHIFT);
    public static final short QUAL_1_MASK          = (short) (1 << QUAL_SHIFT);
    public static final short QUAL_2_MASK          = (short) (1 << QUAL_SHIFT + 1);
    public static final short DP_1_MASK            = (short) (1 << DP_SHIFT);
    public static final short DP_2_MASK            = (short) (1 << DP_SHIFT + 1);
    public static final short DP_3_MASK            = (short) (1 << DP_SHIFT + 2);
//    public static final short DISCREPANCY_MASK     = (short) (1 << DP_SHIFT+DP_SIZE);

    public static final short FILE_IDX_MASK        = (short) (FILE_POSITION_1_MASK | FILE_POSITION_2_MASK
                                                            | FILE_POSITION_3_MASK | FILE_POSITION_4_MASK);
    public static final short TYPE_MASK            = (short) (TYPE_1_MASK | TYPE_2_MASK | TYPE_3_MASK);
    public static final short QUAL_MASK            = (short) (QUAL_1_MASK | QUAL_2_MASK);
    public static final short DP_MASK              = (short) (DP_1_MASK | DP_2_MASK | DP_3_MASK);

    public static final int TYPE_SNV_CODE = 0;
    public static final int TYPE_INDEL_CODE = 1;
    public static final int TYPE_MNV_CODE = 2;
    public static final int TYPE_INS_CODE = 3;
    public static final int TYPE_DEL_CODE = 4;
    public static final int TYPE_CNV_CODE = 5;
    public static final int TYPE_REAR_CODE = 6;
    public static final int TYPE_OTHER_CODE = 7;

    public static final int BYTES = Short.BYTES;


    /**
     * Create the FileIndex value for this specific sample and variant.
     *
     * @param sampleIdx Sample position in the StudyEntry. Used to get the DP from the format.
     * @param filePosition   In case of having multiple files for the same sample, the cardinal value of the load order of the file.
     * @param variant   Full variant.
     * @return 16 bits of file index.
     */
    public short createFileIndexValue(int sampleIdx, int filePosition, Variant variant) {
        // Expecting only one study and only one file
        StudyEntry study = variant.getStudies().get(0);
        FileEntry file = study.getFiles().get(0);

        Integer dpIdx = study.getSampleDataKeyPosition(VCFConstants.DEPTH_KEY);
        String dpStr;
        if (dpIdx != null) {
            dpStr = study.getSampleData(sampleIdx).get(dpIdx);
        } else {
            dpStr = null;
        }

        return createFileIndexValue(variant.getType(), filePosition, file.getData(), dpStr);
    }

    /**
     * Create the FileIndex value for this specific sample and variant.
     *
     * @param type           Variant type
     * @param filePosition        In case of having multiple files for the same sample, the cardinal value of the load order of the file.
     * @param fileAttributes File attributes
     * @param dpStr          DP in String format.
     * @return 16 bits of file index.
     */
    public short createFileIndexValue(VariantType type, int filePosition, Map<String, String> fileAttributes, String dpStr) {
        short fileIndex = 0;

        if (filePosition > FILE_IDX_MAX) {
            throw new IllegalArgumentException("Error converting filePosition. Unable to load more than 16 files for the same sample.");
        }
        fileIndex |= filePosition << FILE_POSITION_SHIFT;

        String filter = fileAttributes.get(StudyEntry.FILTER);
        if (VCFConstants.PASSES_FILTERS_v4.equals(filter)) {
            fileIndex |= FILTER_PASS_MASK;
        }

        fileIndex |= getTypeCode(type) << TYPE_SHIFT;

        double qual = getQual(fileAttributes);
        byte qualCode = IndexUtils.getRangeCode(qual, SampleIndexConfiguration.QUAL_THRESHOLDS);
        fileIndex |= qualCode << QUAL_SHIFT;

        int dp = getDp(fileAttributes, dpStr);
        byte dpCode = IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS);
        fileIndex |= dpCode << DP_SHIFT;

        return fileIndex;
    }

    public static boolean isMultiFile(short fileIndex) {
        return IndexUtils.testIndexAny(fileIndex, MULTI_FILE_MASK);
    }

    public static short setMultiFile(short fileIndex) {
        return (short) (fileIndex | MULTI_FILE_MASK);
    }

    public static void setMultiFile(byte[] bytes, int offset) {
        // TODO: Could be improved
        Bytes.putShort(bytes, offset, setMultiFile(Bytes.toShort(bytes, offset)));
    }

    public static short setFilePosition(short fileIndex, int filePosition) {
        return ((short) (fileIndex | filePosition << FILE_POSITION_SHIFT));
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

    public static int getTypeCode(VariantType type) {
        switch (type) {
            case SNV:
            case SNP:
                return TYPE_SNV_CODE;
            case INDEL:
                return TYPE_INDEL_CODE;
            case MNP:
            case MNV:
                return TYPE_MNV_CODE;
            case INSERTION:
                return TYPE_INS_CODE;
            case DELETION:
                return TYPE_DEL_CODE;
            case CNV:
                return TYPE_CNV_CODE;
            case BREAKEND:
                return TYPE_REAR_CODE;
            case SV:
            case TRANSLOCATION:
            case INVERSION:
            case DUPLICATION:
            case NO_VARIATION:
            case SYMBOLIC:
            case MIXED:
            default:
                return TYPE_OTHER_CODE;
        }
    }

}
