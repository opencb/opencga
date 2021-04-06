package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class Range<N extends Number & Comparable<N>> implements Comparable<Range<N>> {
    private final N start;
    private final boolean startInclusive;
    private final N end;
    private final boolean endInclusive;
    private final String s;

    public static Range<Double> parse(String string) {
        final boolean startInclusive = string.startsWith("[");
        final boolean endInclusive = string.endsWith("]");
        final Double start;
        final Double end;
        String[] split = StringUtils.replaceChars(string, "[]() ", "").split(",");
        if (split[0].equals("-inf")) {
            start = null;
        } else {
            start = Double.valueOf(split[0]);
        }
        if (split.length == 1) {
            end = start;
        } else {
            if (split[1].equals("inf")) {
                end = null;
            } else {
                end = Double.valueOf(split[1]);
            }
        }
        return new Range<>(start, startInclusive, end, endInclusive);
    }

    public Range(N start, N end) {
        this(start, true, end, false);
    }

    public Range(N start, boolean startInclusive, N end, boolean endInclusive) {
        this.start = start;
        this.startInclusive = startInclusive;
        this.end = end;
        this.endInclusive = endInclusive;
        StringBuilder sb = new StringBuilder();
        if (startInclusive) {
            sb.append("[");
        } else {
            sb.append("(");
        }
        if (start == null) {
            sb.append("-inf");
        } else {
            sb.append(start);
        }
        if (!Objects.equals(start, end) || start == null) {
            sb.append(", ");
            if (end == null) {
                sb.append("inf");
            } else {
                sb.append(end);
            }
        }
        if (endInclusive) {
            sb.append("]");
        } else {
            sb.append(")");
        }
        s = sb.toString();
    }

    public static List<Range<Double>> buildRanges(IndexFieldConfiguration index) {
        return buildRanges(index, null, null);
    }

    public static List<Range<Double>> buildRanges(IndexFieldConfiguration index, Double min, Double max) {
        List<Range<Double>> ranges = new LinkedList<>();
        if (index.getNullable()) {
            ranges.add(new Range.NA<>());
        }
        double[] thresholds = index.getThresholds();
        boolean startInclusive = index.getType() == IndexFieldConfiguration.Type.RANGE_LT;
        boolean endInclusive = index.getType() == IndexFieldConfiguration.Type.RANGE_GT;
        ranges.add(new Range<>(min, false, thresholds[0], endInclusive));
        for (int i = 1; i < thresholds.length; i++) {
            ranges.add(new Range<>(thresholds[i - 1], startInclusive, thresholds[i], endInclusive));
        }
        ranges.add(new Range<>(thresholds[thresholds.length - 1], false, max, false));

        // Check duplicated values
        for (int i = index.getNullable() ? 2 : 1; i < ranges.size() - 1; i++) {
            Range<Double> range = ranges.get(i);
            if (range.start.equals(range.end)) {
                Range<Double> pre = ranges.get(i - 1);
                ranges.set(i - 1, new Range<>(pre.start, pre.startInclusive, pre.end, false));

                ranges.set(i, new Range<>(range.start, true, range.end, true));

                Range<Double> post = ranges.get(i + 1);
                ranges.set(i + 1, new Range<>(post.start, false, post.end, post.endInclusive));
            }
        }
        return ranges;
    }

    public static <N extends Number & Comparable<N>> List<Range<N>> buildRanges(List<N> thresholds, N start, N end) {
        List<Range<N>> ranges = new ArrayList<>(thresholds.size() + 1);

        N prev = start;
        for (int i = 0; i < thresholds.size(); i++) {
            N n = thresholds.get(i);
            ranges.add(new Range<>(prev, n));
            prev = n;
        }
        ranges.add(new Range<>(prev, end));

        return ranges;
    }

    @Override
    public String toString() {
        return s;
    }

//        public boolean contains(N number) {
//            int c = start.compareTo(number);
//            if (c == 0) {
//                return startInclusive;
//            } else if (c < 0) {
//                return false;
//            } else {
//
//            }
//        }

    public boolean isBeforeEnd(N number) {
        if (end == null) {
            return true;
        }
        int c = number.compareTo(end);
        if (c == 0) {
            return endInclusive;
        } else {
            return c < 0;
        }
    }

    @Override
    public int compareTo(Range<N> o) {
        return start.compareTo(o.start);
    }

    public N getStart() {
        return start;
    }

    public boolean isStartInfinity() {
        return start == null;
    }

    public boolean isStartInclusive() {
        return startInclusive;
    }

    public N getEnd() {
        return end;
    }

    public boolean isEndInfinity() {
        return end == null;
    }

    public boolean isEndInclusive() {
        return endInclusive;
    }

    public static class NA<N extends Number & Comparable<N>> extends Range<N> {

        public NA() {
            super(null, null);
        }

        @Override
        public String toString() {
            return "NA";
        }

        @Override
        public boolean isBeforeEnd(N number) {
            return number != null;
        }

        @Override
        public int compareTo(Range<N> o) {
            if (o instanceof Range.NA) {
                return 0;
            } else {
                return -1;
            }
        }
    }
}
