package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.opencb.commons.datastore.core.FacetField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;

public class RangeAccumulator<T, N extends Number & Comparable<N>> extends FieldVariantAccumulator<T> {

    private final ToIntFunction<T> getRangeIdx;
    private final String name;
    private final List<Range<N>> ranges;

    public static <T, N extends Number & Comparable<N>> RangeAccumulator<T, N> fromValue(
            Function<T, N> getValue, String name, List<Range<N>> ranges, FieldVariantAccumulator<T> nestedFieldAccumulator) {
        ToIntFunction<T> getRangeIdx = (T t) -> {
            N number = getValue.apply(t);
            if (number == null) {
                return -1;
            }
            int i = 0;
            for (Range<N> range : ranges) {
                if (range.isBeforeEnd(number)) {
                    return i;
                }
                i++;
            }
            return -1;
        };
        return new RangeAccumulator<>(getRangeIdx, name, ranges, nestedFieldAccumulator);
    }

    public static <T, N extends Number & Comparable<N>> RangeAccumulator<T, N> fromIndex(
            ToIntFunction<T> getRangeIdx, String name, List<Range<N>> ranges, FieldVariantAccumulator<T> nestedFieldAccumulator) {
        return new RangeAccumulator<>(getRangeIdx, name, ranges, nestedFieldAccumulator);
    }

    protected RangeAccumulator(ToIntFunction<T> getRangeIdx, String name, List<Range<N>> ranges,
                            FieldVariantAccumulator<T> nestedFieldAccumulator) {
        super(nestedFieldAccumulator);
        this.getRangeIdx = getRangeIdx;
        this.name = name;
        this.ranges = ranges;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected List<FacetField.Bucket> prepareBuckets1() {
        ArrayList<FacetField.Bucket> buckets = new ArrayList<>(ranges.size());
        for (Range<N> range : ranges) {
            buckets.add(new FacetField.Bucket(range.toString(), 0, null));
        }
        return buckets;
    }

    @Override
    protected List<FacetField.Bucket> getBuckets(FacetField field, T variant) {
        int idx = getRangeIdx.applyAsInt(variant);
        if (idx >= 0) {
            return Collections.singletonList(field.getBuckets().get(idx));
        }
        return null;
    }
}
