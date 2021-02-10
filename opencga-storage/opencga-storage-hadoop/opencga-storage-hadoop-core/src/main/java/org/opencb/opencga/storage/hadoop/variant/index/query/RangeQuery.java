package org.opencb.opencga.storage.hadoop.variant.index.query;

import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;

import java.util.Objects;

public class RangeQuery {

    protected final double minValueInclusive;
    protected final double maxValueExclusive;
    protected final byte minCodeInclusive;
    protected final byte maxCodeExclusive;
    protected final boolean exactQuery;

    public RangeQuery(double minValueInclusive, double maxValueExclusive, byte minCodeInclusive, byte maxCodeExclusive) {
        this(minValueInclusive, maxValueExclusive, minCodeInclusive, maxCodeExclusive, false);
    }

    public RangeQuery(
            double minValueInclusive, double maxValueExclusive, byte minCodeInclusive, byte maxCodeExclusive, boolean exactQuery) {
        this.minValueInclusive = minValueInclusive;
        this.maxValueExclusive = maxValueExclusive;
        this.minCodeInclusive = minCodeInclusive;
        this.maxCodeExclusive = maxCodeExclusive;
        this.exactQuery = exactQuery;
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

    public boolean isExactQuery() {
        return exactQuery;
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
        return IndexUtils.equalsTo(that.minValueInclusive, minValueInclusive)
                && IndexUtils.equalsTo(that.maxValueExclusive, maxValueExclusive)
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
                + ", exact: " + exactQuery
                + '}';
    }
}
