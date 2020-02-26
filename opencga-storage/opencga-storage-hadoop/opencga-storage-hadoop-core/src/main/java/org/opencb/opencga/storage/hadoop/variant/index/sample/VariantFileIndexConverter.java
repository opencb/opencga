package org.opencb.opencga.storage.hadoop.variant.index.sample;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexSchema.INTRA_CHROMOSOME_VARIANT_COMPARATOR;

public class VariantFileIndexConverter {

    public static final int TYPE_SHIFT = 1;
    public static final int QUAL_SHIFT = 4;
    public static final int DP_SHIFT = 6;

    public static final int TYPE_SIZE = 3;
    public static final int QUAL_SIZE = 2;
    public static final int DP_SIZE = 2;

    public static final byte FILTER_PASS_MASK     = (byte) (1 << 0);
    public static final byte TYPE_1_MASK          = (byte) (1 << TYPE_SHIFT);
    public static final byte TYPE_2_MASK          = (byte) (1 << TYPE_SHIFT + 1);
    public static final byte TYPE_3_MASK          = (byte) (1 << TYPE_SHIFT + 2);
    public static final byte QUAL_1_MASK          = (byte) (1 << QUAL_SHIFT);
    public static final byte QUAL_2_MASK          = (byte) (1 << QUAL_SHIFT + 1);
    public static final byte DP_1_MASK            = (byte) (1 << DP_SHIFT);
    public static final byte DP_2_MASK            = (byte) (1 << DP_SHIFT + 1);

    public static final byte TYPE_MASK            = (byte) (TYPE_1_MASK | TYPE_2_MASK | TYPE_3_MASK);
    public static final byte QUAL_MASK            = (byte) (QUAL_1_MASK | QUAL_2_MASK);
    public static final byte DP_MASK              = (byte) (DP_1_MASK | DP_2_MASK);

    public static final int TYPE_SNV_CODE = 0;
    public static final int TYPE_INDEL_CODE = 1;
    public static final int TYPE_MNV_CODE = 2;
    public static final int TYPE_INS_CODE = 3;
    public static final int TYPE_DEL_CODE = 4;
    public static final int TYPE_CNV_CODE = 5;
    public static final int TYPE_REAR_CODE = 6;
    public static final int TYPE_OTHER_CODE = 7;


    public VariantFileIndex toVariantFileIndex(int sampleIdx, Variant variant) {
        return new VariantFileIndex(variant, createFileIndexValue(sampleIdx, variant));
    }

    public byte createFileIndexValue(int sampleIdx, Variant variant) {
        // Expecting only one study and only one file
        StudyEntry study = variant.getStudies().get(0);
        FileEntry file = study.getFiles().get(0);

        Integer dpIdx = study.getFormatPositions().get(VCFConstants.DEPTH_KEY);
        String dpStr;
        if (dpIdx != null) {
            dpStr = study.getSampleData(sampleIdx).get(dpIdx);
        } else {
            dpStr = null;
        }

        return createFileIndexValue(variant.getType(), file.getAttributes(), dpStr);
    }

    public byte createFileIndexValue(VariantType type, Map<String, String> fileAttributes, String dpStr) {
        byte b = 0;

        String filter = fileAttributes.get(StudyEntry.FILTER);
        if (VCFConstants.PASSES_FILTERS_v4.equals(filter)) {
            b |= FILTER_PASS_MASK;
        }

        b |= getTypeCode(type) << TYPE_SHIFT;

        double qual = getQual(fileAttributes);
        byte qualCode = IndexUtils.getRangeCode(qual, SampleIndexConfiguration.QUAL_THRESHOLDS);
        b |= qualCode << QUAL_SHIFT;

        int dp = getDp(fileAttributes, dpStr);
        byte dpCode = IndexUtils.getRangeCode(dp, SampleIndexConfiguration.DP_THRESHOLDS);
        b |= dpCode << DP_SHIFT;

        return b;
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

    public static class VariantFileIndex implements Comparable<VariantFileIndex> {

        private final Variant variant;
        private final byte fileIndex;

        public VariantFileIndex(Variant variant, byte fileIndex) {
            this.variant = variant;
            this.fileIndex = fileIndex;
        }

        public Variant getVariant() {
            return variant;
        }

        public byte getFileIndex() {
            return fileIndex;
        }

        @Override
        public int compareTo(VariantFileIndex o) {
            return INTRA_CHROMOSOME_VARIANT_COMPARATOR.compare(variant, o.variant);
        }
    }
}
