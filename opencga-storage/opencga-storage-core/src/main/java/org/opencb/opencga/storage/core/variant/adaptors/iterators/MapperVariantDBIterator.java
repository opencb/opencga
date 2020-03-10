package org.opencb.opencga.storage.core.variant.adaptors.iterators;

import org.opencb.biodata.models.variant.Variant;

import java.util.function.UnaryOperator;

public class MapperVariantDBIterator extends DelegatedVariantDBIterator {

    private final UnaryOperator<Variant> map;

    MapperVariantDBIterator(VariantDBIterator delegated, UnaryOperator<Variant> map) {
        super(delegated);
        this.map = map;
    }

    @Override
    public Variant next() {
        return map.apply(super.next());
    }
}
