package org.opencb.opencga.storage.core.variant.index.sample.schema;

import org.opencb.opencga.core.config.storage.FieldConfiguration;
import org.opencb.opencga.storage.core.variant.index.core.CategoricalMultiValuedIndexField;
import org.opencb.opencga.storage.core.variant.index.core.SingleFieldIndexSchema;

import java.util.List;

public class BiotypeIndexSchema extends SingleFieldIndexSchema<List<String>> {

    public BiotypeIndexSchema(FieldConfiguration configuration) {
        super(new CategoricalMultiValuedIndexField<>(configuration, 0, configuration.getValues(), configuration.getValuesMapping()));
    }

    @Override
    public CategoricalMultiValuedIndexField<String> getField() {
        return (CategoricalMultiValuedIndexField<String>) super.getField();
    }

}
