package org.opencb.opencga.storage.hadoop.variant.index.core.filters;

import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.hadoop.variant.index.core.RangeIndexField;

public class RangeIndexFieldFilter extends IndexFieldFilter {

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
        super(index);
        this.index = index;

        switch (op) {
            case "":
            case "=":
            case "==":
                this.minValueInclusive = value;
                this.maxValueExclusive = value + RangeIndexField.DELTA;
                break;
            case "<=":
            case "<<=":
                // Range is with exclusive end. For inclusive "<=" operator, need to add a DELTA to the value
                value += RangeIndexField.DELTA;
            case "<":
            case "<<":
                this.minValueInclusive = index.getMin();
                this.maxValueExclusive = value;
                break;
            case ">":
            case ">>":
                // Range is with inclusive start. For exclusive ">" operator, need to add a DELTA to the value
                value += RangeIndexField.DELTA;
            case ">=":
            case ">>=":
                this.minValueInclusive = value;
                this.maxValueExclusive = index.getMax();
                break;
            default:
                throw new VariantQueryException("Unknown query operator '" + op + "'");
        }

        this.minCodeInclusive = (byte) index.encode(minValueInclusive);
        this.maxCodeExclusive = (byte) index.encodeExclusive(maxValueExclusive);


        this.exactFilter = (minValueInclusive == index.getMin() || index.containsThreshold(minValueInclusive))
                        && (maxValueExclusive == index.getMax() || index.containsThreshold(maxValueExclusive));
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

    @Deprecated
    public static double[] queryRange(String op, double value, double min, double max) {
        switch (op) {
            case "":
            case "=":
            case "==":
                return new double[]{value, value + RangeIndexField.DELTA};
            case "<=":
            case "<<=":
                // Range is with exclusive end. For inclusive "<=" operator, need to add a DELTA to the value
                value += RangeIndexField.DELTA;
            case "<":
            case "<<":
                return new double[]{min, value};

            case ">":
            case ">>":
                // Range is with inclusive start. For exclusive ">" operator, need to add a DELTA to the value
                value += RangeIndexField.DELTA;
            case ">=":
            case ">>=":
                return new double[]{value, max};
            default:
                throw new VariantQueryException("Unknown query operator" + op);
        }
    }

    @Deprecated
    public static byte[] getRangeCodes(double[] queryRange, double[] thresholds) {
        return new byte[]{RangeIndexField.getRangeCode(queryRange[0], thresholds), getRangeCodeExclusive(queryRange[1], thresholds)};
    }

    @Deprecated
    public static byte getRangeCodeExclusive(double queryValue, double[] thresholds) {
        return (byte) (1 + RangeIndexField.getRangeCode(queryValue - RangeIndexField.DELTA, thresholds));
    }

}
