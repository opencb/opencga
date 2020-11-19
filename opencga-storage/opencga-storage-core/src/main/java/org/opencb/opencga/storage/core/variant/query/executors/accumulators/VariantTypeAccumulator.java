package org.opencb.opencga.storage.core.variant.query.executors.accumulators;

import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import java.util.*;
import java.util.function.Function;

public class VariantTypeAccumulator<T> extends FacetFieldAccumulator<T> {

    private final Function<T, VariantType> getType;
    private EnumSet<VariantType> typesSet;
    private boolean allTypes;

    public VariantTypeAccumulator(Function<T, VariantType> getType) {
        super(null);
        this.getType = getType;
        // TODO: Accept subset of variant type
    }

    public VariantTypeAccumulator(Function<T, VariantType> getType, Collection<VariantType> types) {
        super(null);
        this.getType = getType;
        if (types == null || types.isEmpty()) {
            allTypes = true;
            typesSet = EnumSet.allOf(VariantType.class);
        } else {
            allTypes = false;
            typesSet = EnumSet.noneOf(VariantType.class);
            typesSet.addAll(types);
        }
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
    protected List<FacetField.Bucket> getBuckets(FacetField field, T t) {
        VariantType type = getType(t);
        if (allTypes || typesSet.contains(type)) {
            return Collections.singletonList(field.getBuckets().get(type.ordinal()));
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public void evaluate(FacetField field) {
        field.getBuckets().removeIf(b -> !typesSet.contains(VariantType.valueOf(b.getValue())));
        super.evaluate(field);
    }

    protected VariantType getType(T variant) {
        return getType.apply(variant);
    }
}
