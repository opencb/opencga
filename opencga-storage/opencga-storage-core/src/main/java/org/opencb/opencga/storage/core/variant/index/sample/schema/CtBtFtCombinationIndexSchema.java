package org.opencb.opencga.storage.core.variant.index.sample.schema;

import org.opencb.opencga.storage.core.variant.index.core.CombinationTripleIndexSchema;

public class CtBtFtCombinationIndexSchema extends CombinationTripleIndexSchema {

    public CtBtFtCombinationIndexSchema(ConsequenceTypeIndexSchema consequenceTypeIndexSchema,
                                        BiotypeIndexSchema biotypeIndexSchema,
                                        TranscriptFlagIndexSchema transcriptFlagIndexSchema) {
        super(consequenceTypeIndexSchema.getField(), biotypeIndexSchema.getField(), transcriptFlagIndexSchema.getField());
    }
}
