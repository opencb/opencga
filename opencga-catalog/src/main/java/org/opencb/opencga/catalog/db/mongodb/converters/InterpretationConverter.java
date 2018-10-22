package org.opencb.opencga.catalog.db.mongodb.converters;

import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.models.Interpretation;

public class InterpretationConverter extends GenericDocumentComplexConverter<Interpretation> {

    public InterpretationConverter() {
        super(Interpretation.class);
    }

}
