package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalMultiValuedIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.FixedSizeIndexSchema;

import java.util.Arrays;

public class ClinicalIndexSchema extends FixedSizeIndexSchema {

    private final CategoricalMultiValuedIndexField<String> sourceField;
    private final CategoricalMultiValuedIndexField<String> clinicalSignificanceField;

    public ClinicalIndexSchema(IndexFieldConfiguration sourceConfiguration, IndexFieldConfiguration clinicalConfiguration) {
        sourceField = new CategoricalMultiValuedIndexField<>(sourceConfiguration, 0, sourceConfiguration.getValues());
        clinicalSignificanceField = new CategoricalMultiValuedIndexField<>(
                clinicalConfiguration, sourceField.getBitLength(), clinicalConfiguration.getValues());
        fields = Arrays.asList(sourceField, clinicalSignificanceField);
        updateIndexSizeBits();
    }

    public CategoricalMultiValuedIndexField<String> getSourceField() {
        return sourceField;
    }

    public CategoricalMultiValuedIndexField<String> getClinicalSignificanceField() {
        return clinicalSignificanceField;
    }
}
