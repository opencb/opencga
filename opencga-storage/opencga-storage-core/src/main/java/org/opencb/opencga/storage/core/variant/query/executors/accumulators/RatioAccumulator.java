package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.opencb.commons.datastore.core.FacetField;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.ToDoubleFunction;

public class RatioAccumulator<T> extends FacetFieldAccumulator<T> {

    private final String name;
    private final ToDoubleFunction<T> getNumerator;
    private final ToDoubleFunction<T> getDenominator;

    public RatioAccumulator(ToDoubleFunction<T> getNumerator, ToDoubleFunction<T> getDenominator, String name) {
        super(null);
        this.getNumerator = getNumerator;
        this.getDenominator = getDenominator;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public FacetField createField() {
        return super.createField()
                .setAggregationName("ratio")
                .setAggregationValues(Arrays.asList(0d, 0d));
    }

    @Override
    protected List<FacetField.Bucket> prepareBuckets1() {
        return Collections.emptyList();
    }

    @Override
    protected List<FacetField.Bucket> getBuckets(FacetField field, T t) {
        List<Double> aggregationValues = field.getAggregationValues();
        aggregationValues.set(0, aggregationValues.get(0) + getNumerator.applyAsDouble(t));
        aggregationValues.set(1, aggregationValues.get(1) + getDenominator.applyAsDouble(t));
        return null;
    }

    @Override
    public void evaluate(FacetField field) {
        super.evaluate(field);
        List<Double> aggregationValues = field.getAggregationValues();
        field.setAggregationValues(Collections.singletonList(aggregationValues.get(0) / aggregationValues.get(1)));
    }
}
