package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.opencb.commons.datastore.core.FacetField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class CategoricalAccumulator<T> extends FieldVariantAccumulator<T> {

    private final Function<T, Collection<String>> getCategory;
    private final String name;

    public CategoricalAccumulator(Function<T, Collection<String>> getCategory, String name) {
        this(getCategory, name, null);
        // TODO: Accept subset of categories
    }

    public CategoricalAccumulator(Function<T, Collection<String>> getCategory, String name,
                                  FieldVariantAccumulator<T> nestedFieldAccumulator) {
        super(nestedFieldAccumulator);
        this.getCategory = getCategory;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<FacetField.Bucket> prepareBuckets1() {
        return new ArrayList<>();
    }

    @Override
    protected List<FacetField.Bucket> getBuckets(FacetField field, T variant) {
        Collection<String> values = getCategory.apply(variant);
        List<FacetField.Bucket> buckets = new ArrayList<>();
        for (String value : values) {
            FacetField.Bucket bucket = null;
            for (FacetField.Bucket thisBucket : field.getBuckets()) {
                if (thisBucket.getValue().equals(value)) {
                    buckets.add(thisBucket);
                    bucket = thisBucket;
                }
            }
            if (bucket == null) {
                buckets.add(addBucket(field, value));
            }
        }
        return buckets;
    }

}
