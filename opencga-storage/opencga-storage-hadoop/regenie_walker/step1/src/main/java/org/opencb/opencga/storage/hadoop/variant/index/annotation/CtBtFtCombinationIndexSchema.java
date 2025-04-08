package org.opencb.opencga.storage.hadoop.variant.index.annotation;

import org.opencb.opencga.storage.hadoop.variant.index.core.CombinationTripleIndexSchema;

public class CtBtFtCombinationIndexSchema extends CombinationTripleIndexSchema {

    public CtBtFtCombinationIndexSchema(ConsequenceTypeIndexSchema consequenceTypeIndexSchema,
                                        BiotypeIndexSchema biotypeIndexSchema,
                                        TranscriptFlagIndexSchema transcriptFlagIndexSchema) {
        super(consequenceTypeIndexSchema.getField(), biotypeIndexSchema.getField(), transcriptFlagIndexSchema.getField());
    }
}
