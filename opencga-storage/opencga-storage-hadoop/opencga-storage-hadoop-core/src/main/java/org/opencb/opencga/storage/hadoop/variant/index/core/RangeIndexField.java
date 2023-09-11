package org.opencb.opencga.storage.hadoop.variant.index.core;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.core.variant.query.OpValue;
import org.opencb.opencga.storage.core.variant.query.executors.accumulators.Range;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.IndexFieldFilter;
import org.opencb.opencga.storage.hadoop.variant.index.core.filters.RangeIndexFieldFilter;

import java.util.Arrays;
import java.util.List;

/**
 * Single value range index.
 */
public class RangeIndexField extends IndexField<Double> {
    public static final double MAX = 500000000;
    public static final double DELTA = 0.0000001;
    private final double[] thresholds;
    private final double min;
    private final double max;
    private final int bitLength;
    private final IndexCodec<Double> codec;
    private int numRanges;

    public RangeIndexField(IndexFieldConfiguration configuration, int bitOffset) {
        this(configuration, bitOffset, Double.MIN_VALUE, MAX);
    }

    public RangeIndexField(IndexFieldConfiguration configuration, int bitOffset, double minValue, double max) {
        super(configuration, bitOffset);
        this.thresholds = getConfiguration().getThresholds().clone();
        min = minValue;
        this.max = max;
        // There is one range more than thresholds.
        numRanges = thresholds.length + 1;
        if (configuration.getNullable()) {
            // Add one range for the NA
            numRanges++;
            codec = new NullableRangeCodec();
        } else {
            codec = new NonNullableRangeCodec();
        }
        bitLength = Math.max(1, IndexUtils.log2(numRanges - 1) + 1);
        if (configuration.getType().equals(IndexFieldConfiguration.Type.RANGE_GT)) {
            // Add one DELTA to each value to invert ranges from [s, e) to (s, e], therefore the operation ">" is exact
            for (int i = 0; i < thresholds.length; i++) {
                thresholds[i] += DELTA;
            }

            // If two consecutive values have the same value, subtract a DELTA to the first one to create a range (v-DELTA, v]
            for (int i = 1; i < thresholds.length; i++) {
                if (thresholds[i] == thresholds[i - 1]) {
                    thresholds[i - 1] -= DELTA;
                }
            }
        } else {
            // If two consecutive values have the same value, add a DELTA to the second one to create a range [v, v+DELTA)
            for (int i = 1; i < thresholds.length; i++) {
                if (thresholds[i] == thresholds[i - 1]) {
                    thresholds[i] += DELTA;
                }
            }
        }
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
        return bitLength;
    }

    @Override
    public int encode(Double value) {
        return codec.encode(value);
    }

    public int encodeExclusive(Double value) {
        return (1 + encode(value - RangeIndexField.DELTA));
    }

    @Override
    public Double decode(int code) {
        return codec.decode(code);
    }

    public List<Range<Double>> getRanges() {
        return Range.buildRanges(getConfiguration());
    }

    @Override
    protected IndexFieldFilter getSingleValueIndexFilter(OpValue<Double> opValue) {
        return new RangeIndexFieldFilter(this, opValue);
    }

    public boolean containsThreshold(double value) {
        for (double v : thresholds) {
            if (RangeIndexField.equalsTo(v, value)) {
                return true;
            }
        }
        return false;
    }

    public class NonNullableRangeCodec implements IndexCodec<Double> {
        @Override
        public int encode(Double value) {
            return value == null ? 0 : getRangeCode(value, thresholds);
        }

        @Override
        public Double decode(int code) {
            return code <= 0 ? min : thresholds[code - 1];
        }

        @Override
        public boolean ambiguous(int code) {
            return true;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("NonNullableRangeCodec{");
            sb.append('}');
            return sb.toString();
        }
    }

    public class NullableRangeCodec extends NonNullableRangeCodec {

        public static final int NA = 0;

        @Override
        public int encode(Double value) {
            return value == null ? NA : getRangeCode(value, thresholds) + 1;
        }

        @Override
        public Double decode(int code) {
            return code == NA ? null : super.decode(code - 1);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("NullableRangeCodec{");
            sb.append('}');
            return sb.toString();
        }
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RangeIndexField{");
        sb.append("configuration=").append(getConfiguration());
        sb.append(", bitOffset=").append(getBitOffset());
        sb.append(", bitLength=").append(bitLength);
        sb.append(", thresholds=").append(Arrays.toString(thresholds));
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", codec=").append(codec);
        sb.append(", numRanges=").append(numRanges);
        sb.append('}');
        return sb.toString();
    }
}
