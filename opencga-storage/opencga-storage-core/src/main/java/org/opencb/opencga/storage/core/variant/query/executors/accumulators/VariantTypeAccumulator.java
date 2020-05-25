package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class VariantTypeAccumulator<T> extends FieldVariantAccumulator<T> {

    private final Function<T, VariantType> getType;

    public VariantTypeAccumulator(Function<T, VariantType> getType) {
        this(getType, null);
        // TODO: Accept subset of variant type
    }

    public VariantTypeAccumulator(Function<T, VariantType> getType, FieldVariantAccumulator<T> nestedFieldAccumulator) {
        super(nestedFieldAccumulator);
        this.getType = getType;
    }

    @Override
    public String getName() {
        return VariantField.TYPE.fieldName();
    }

    @Override
    public List<FacetField.Bucket> prepareBuckets1() {
        List<FacetField.Bucket> buckets = new ArrayList<>(VariantType.values().length);
        for (VariantType variantType : VariantType.values()) {
            buckets.add(new FacetField.Bucket(variantType.name(), 0, null));
        }
        return buckets;
    }

    @Override
    protected List<FacetField.Bucket> getBuckets(FacetField field, T variant) {
        return Collections.singletonList(field.getBuckets().get(getType(variant).ordinal()));
    }

    protected VariantType getType(T variant) {
        return getType.apply(variant);
    }
}
