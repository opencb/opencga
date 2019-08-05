package org.opencb.opencga.storage.hadoop.variant.index.query;

import java.util.Objects;

public class RangeQuery {

    protected final double minValueInclusive;
    protected final double maxValueExclusive;
    protected final byte minCodeInclusive;
    protected final byte maxCodeExclusive;

    public RangeQuery(double minValueInclusive, double maxValueExclusive, byte minCodeInclusive, byte maxCodeExclusive) {
        this.minValueInclusive = minValueInclusive;
        this.maxValueExclusive = maxValueExclusive;
        this.minCodeInclusive = minCodeInclusive;
        this.maxCodeExclusive = maxCodeExclusive;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RangeQuery that = (RangeQuery) o;
        return Double.compare(that.minValueInclusive, minValueInclusive) == 0
                && Double.compare(that.maxValueExclusive, maxValueExclusive) == 0
                && minCodeInclusive == that.minCodeInclusive
                && maxCodeExclusive == that.maxCodeExclusive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minValueInclusive, maxValueExclusive, minCodeInclusive, maxCodeExclusive);
    }


    @Override
    public String toString() {
        return "RangeQuery{"
                + "query [" + minValueInclusive + ", " + maxValueExclusive + ")"
                + ", code [" + minCodeInclusive + ", " + maxCodeExclusive + ")"
                + '}';
    }
}
