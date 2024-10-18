package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.core.config.storage.FieldConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalMultiValuedIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.SingleFieldIndexSchema;

import java.util.List;

public class ConsequenceTypeIndexSchema extends SingleFieldIndexSchema<List<String>> {

    public ConsequenceTypeIndexSchema(FieldConfiguration ctConfiguration) {
        super(new CategoricalMultiValuedIndexField<>(ctConfiguration, 0, ctConfiguration.getValues(), ctConfiguration.getValuesMapping()));
    }

    @Override
    public CategoricalMultiValuedIndexField<String> getField() {
        return (CategoricalMultiValuedIndexField<String>) super.getField();
    }
}
