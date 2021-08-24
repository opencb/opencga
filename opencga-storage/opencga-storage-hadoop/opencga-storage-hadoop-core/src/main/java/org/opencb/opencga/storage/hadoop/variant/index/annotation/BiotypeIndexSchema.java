package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalMultiValuedIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.SingleFieldIndexSchema;

import java.util.List;

public class BiotypeIndexSchema extends SingleFieldIndexSchema<List<String>> {

    public BiotypeIndexSchema(IndexFieldConfiguration configuration) {
        super(new CategoricalMultiValuedIndexField<>(configuration, 0, configuration.getValues(), configuration.getValuesMapping()));
    }

    @Override
    public CategoricalMultiValuedIndexField<String> getField() {
        return (CategoricalMultiValuedIndexField<String>) super.getField();
    }

}
