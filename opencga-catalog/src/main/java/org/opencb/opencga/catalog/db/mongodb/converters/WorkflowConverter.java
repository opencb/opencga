package org.opencb.opencga.catalog.db.mongodb.converters;

import org.opencb.opencga.core.models.workflow.Workflow;

public class WorkflowConverter extends OpenCgaMongoConverter<Workflow> {

    public WorkflowConverter() {
        super(Workflow.class);
    }
}
