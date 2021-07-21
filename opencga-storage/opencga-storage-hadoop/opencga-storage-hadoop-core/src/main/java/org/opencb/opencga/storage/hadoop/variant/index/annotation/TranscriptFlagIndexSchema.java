package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.core.config.storage.IndexFieldConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.core.CategoricalMultiValuedIndexField;
import org.opencb.opencga.storage.hadoop.variant.index.core.SingleFieldIndexSchema;

import java.util.List;

public class TranscriptFlagIndexSchema extends SingleFieldIndexSchema<List<String>> {

    public TranscriptFlagIndexSchema(IndexFieldConfiguration transcriptFlagConfiguration) {
        super(new CategoricalMultiValuedIndexField<>(
                transcriptFlagConfiguration, 0, transcriptFlagConfiguration.getValues(), transcriptFlagConfiguration.getValuesMapping()));
    }

    @Override
    public CategoricalMultiValuedIndexField<String> getField() {
        return (CategoricalMultiValuedIndexField<String>) super.getField();
    }
}
