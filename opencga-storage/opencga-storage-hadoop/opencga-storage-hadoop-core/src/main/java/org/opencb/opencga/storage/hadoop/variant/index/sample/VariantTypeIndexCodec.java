package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.index.core.IndexCodec;

public class VariantTypeIndexCodec implements IndexCodec<VariantType> {
    public static final int TYPE_NUM_VALUES = 8;

    public static final int TYPE_SNV_CODE = 0;
    public static final int TYPE_INDEL_CODE = 1;
    public static final int TYPE_MNV_CODE = 2;
    public static final int TYPE_INS_CODE = 3;
    public static final int TYPE_DEL_CODE = 4;
    public static final int TYPE_CNV_CODE = 5;
    public static final int TYPE_REAR_CODE = 6;
    public static final int TYPE_OTHER_CODE = 7;

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
            case COPY_NUMBER:
            case COPY_NUMBER_GAIN:
            case COPY_NUMBER_LOSS:
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

    @Override
    public int encode(VariantType value) {
        return getTypeCode(value);
    }

    @Override
    public VariantType decode(int code) {
        // TODO
        return null;
    }

    @Override
    public boolean ambiguous(int code) {
        return code == TYPE_OTHER_CODE || code == TYPE_CNV_CODE;
    }
}
