package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.core.RangeIndexField;

public class RangeIndexFieldFilter extends IndexFieldFilter {

    public static final double DELTA = 0.0000001;
    private final RangeIndexField index;
    private final double minValueInclusive;
    private final double maxValueExclusive;
    private final byte minCodeInclusive;
    private final byte maxCodeExclusive;
    private final boolean exactFilter;

    public RangeIndexFieldFilter(RangeIndexField index, OpValue<Double> opValue) {
        this(index, opValue.getOp(), opValue.getValue());
    }

    public RangeIndexFieldFilter(RangeIndexField index, String op, double value) {
        this(index, queryRange(op, value, index.getMin(), index.getMax()));
    }

    public RangeIndexFieldFilter(RangeIndexField index, double[] range) {
        super(index);
        this.index = index;
        double[] thresholds = index.getThresholds();
        double min = index.getMin();
        double max = index.getMax();

        byte[] rangeCode = getRangeCodes(range, thresholds);

        boolean exactFilter;
        if (rangeCode[0] == 0) {
            if (rangeCode[1] - 1 == thresholds.length) {
                exactFilter = equalsTo(range[0], min) && equalsTo(range[1], max);
            } else {
                exactFilter = equalsTo(range[1], thresholds[rangeCode[1] - 1]) && equalsTo(range[0], min);
            }
        } else if (rangeCode[1] - 1 == thresholds.length) {
            exactFilter = equalsTo(range[0], thresholds[rangeCode[0] - 1]) && equalsTo(range[1], max);
        } else {
            exactFilter = false;
        }
        this.minValueInclusive = range[0];
        this.maxValueExclusive = range[1];
        this.minCodeInclusive = rangeCode[0];
        this.maxCodeExclusive = rangeCode[1];
        this.exactFilter = exactFilter;
    }

    @Override
    public boolean test(int code) {
        return minCodeInclusive <= code && code < maxCodeExclusive;
    }

    @Override
    public RangeIndexField getIndex() {
        return index;
    }

    @Override
    public boolean isExactFilter() {
        return exactFilter;
    }

    public double getMinValueInclusive() {
        return minValueInclusive;
    }

    public double getMaxValueExclusive() {
        return maxValueExclusive;
    }

    public byte getMinCodeInclusive() {
        return minCodeInclusive;
    }

    public byte getMaxCodeExclusive() {
        return maxCodeExclusive;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + getIndex().getId()
                + " (offset=" + getIndex().getBitOffset() + ", length=" + getIndex().getBitLength() + ") "
                + "query [" + (minValueInclusive == Double.MIN_VALUE ? "-inf" : minValueInclusive) + ", "
                    + (maxValueExclusive == Double.MAX_VALUE ? "inf" : maxValueExclusive) + ")"
                + ", code [" + minCodeInclusive + ", " + maxCodeExclusive + ")"
                + ", exact: " + exactFilter
                + '}';
    }


    public static double[] queryRange(String op, double value) {
        return queryRange(op, value, Double.MIN_VALUE, Double.MAX_VALUE);
    }

    public static double[] queryRange(String op, double value, double min, double max) {
        switch (op) {
            case "":
            case "=":
            case "==":
                return new double[]{value, value + DELTA};
            case "<=":
            case "<<=":
                // Range is with exclusive end. For inclusive "<=" operator, need to add a DELTA to the value
                value += DELTA;
            case "<":
            case "<<":
                return new double[]{min, value};

            case ">":
            case ">>":
                // Range is with inclusive start. For exclusive ">" operator, need to add a DELTA to the value
                value += DELTA;
            case ">=":
            case ">>=":
                return new double[]{value, max};
            default:
                throw new VariantQueryException("Unknown query operator" + op);
        }
    }

    public static byte[] getRangeCodes(double[] queryRange, double[] thresholds) {
        return new byte[]{getRangeCode(queryRange[0], thresholds), getRangeCodeExclusive(queryRange[1], thresholds)};
    }

    public static byte getRangeCodeExclusive(double queryValue, double[] thresholds) {
        return (byte) (1 + getRangeCode(queryValue - DELTA, thresholds));
    }

    /**
     * Gets the range code given a value and a list of ranges.
     * Each point in the array indicates a range threshold.
     *
     * range 1 = ( -inf , th[0] )       ~   value < th[0]
     * range 2 = [ th[0] , th[1] )      ~   value >= th[0] && value < th[1]
     * range n = [ th[n-1] , +inf )     ~   value >= th[n-1]
     *
     * @param value     Value to convert
     * @param thresholds    List of thresholds
     * @return range code
     */
    public static byte getRangeCode(double value, double[] thresholds) {
        byte code = (byte) (thresholds.length);
        for (byte i = 0; i < thresholds.length; i++) {
            if (lessThan(value, thresholds[i])) {
                code = i;
                break;
            }
        }
        return code;
    }

    public static boolean lessThan(double a, double b) {
        return a < b && !equalsTo(a, b);
    }

    public static boolean equalsTo(double a, double b) {
        return Math.abs(a - b) < (DELTA / 10);
    }

}
