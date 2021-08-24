package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.hadoop.variant.index.core.CombinationIndexSchema;

public class CtBtCombinationIndexSchema extends CombinationIndexSchema {

    public CtBtCombinationIndexSchema(ConsequenceTypeIndexSchema consequenceTypeIndexSchema, BiotypeIndexSchema biotypeIndexSchema) {
        super(consequenceTypeIndexSchema.getField(), biotypeIndexSchema.getField());
    }
}
