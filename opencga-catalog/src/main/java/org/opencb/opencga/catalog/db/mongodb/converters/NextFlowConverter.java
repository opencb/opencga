package org.opencb.opencga.catalog.db.mongodb.converters;

import org.opencb.opencga.core.models.nextflow.NextFlow;

public class NextFlowConverter extends OpenCgaMongoConverter<NextFlow> {

    public NextFlowConverter() {
        super(NextFlow.class);
    }
}
