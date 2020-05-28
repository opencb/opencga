package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import java.util.ArrayList;
import java.util.List;

public class Range<N extends Number & Comparable<N>> implements Comparable<Range<N>> {
    private final N start;
    private final boolean startInclusive;
    private final N end;
    private final boolean endInclusive;
    private final String s;

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
        sb.append(", ");
        if (end == null) {
            sb.append("inf");
        } else {
            sb.append(end);
        }
        if (endInclusive) {
            sb.append("]");
        } else {
            sb.append(")");
        }
        s = sb.toString();
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
}
