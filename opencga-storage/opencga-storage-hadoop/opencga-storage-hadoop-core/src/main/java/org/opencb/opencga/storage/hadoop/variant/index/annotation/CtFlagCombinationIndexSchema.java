package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.hadoop.variant.index.core.CombinationIndexSchema;

public class CtFlagCombinationIndexSchema extends CombinationIndexSchema {

    public CtFlagCombinationIndexSchema(ConsequenceTypeIndexSchema consequenceTypeIndexSchema, TranscriptFlagIndexSchema flagIndexSchema) {
        super(consequenceTypeIndexSchema.getField(), flagIndexSchema.getField());
    }
}
