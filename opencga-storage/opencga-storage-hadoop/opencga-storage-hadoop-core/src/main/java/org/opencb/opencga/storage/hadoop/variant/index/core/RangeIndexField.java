package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.storage.core.config.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.RangeIndexFieldFilter;

/**
 * Single value range index.
 */
public class RangeIndexField extends IndexField<Double> {
    public static final int MAX = 500000;
    private final double[] thresholds;
    private final double min;
    private final double max;
    private final int numBits;

    // TODO: Support NA
    public RangeIndexField(IndexFieldConfiguration configuration, int bitOffset, double[] thresholds) {
        super(configuration, bitOffset);
        this.thresholds = thresholds;
        if (Integer.bitCount(thresholds.length + 1) != 1) {
            throw new IllegalArgumentException("Invalid number of thresholds. Got "
                    + thresholds.length + " thresholds. "
                    + "Must be a power of 2 minus 1. e.g. 1, 3, 7, 15...");
        }
        min = Double.MIN_VALUE;
        max = MAX;
        numBits = Integer.bitCount(thresholds.length);
    }

    public double[] getThresholds() {
        return thresholds;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    @Override
    public int getBitLength() {
        return numBits;
    }

    @Override
    public int encode(Double value) {
        return RangeIndexFieldFilter.getRangeCode(value, thresholds);
    }

    @Override
    public Double decode(int code) {
        return null;
    }

    @Override
    protected IndexFieldFilter getSingleValueIndexFilter(OpValue<Double> opValue) {
        return new RangeIndexFieldFilter(this, opValue);
    }



}
