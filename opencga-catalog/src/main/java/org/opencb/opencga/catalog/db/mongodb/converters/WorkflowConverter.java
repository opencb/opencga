package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.opencga.core.models.workflow.Workflow;

public class WorkflowConverter extends OpenCgaMongoConverter<Workflow> {

    public WorkflowConverter() {
        super(Workflow.class);
    }

    @Override
    public Document convertToStorageType(Workflow object) {
        Document document = super.convertToStorageType(object);
        // Keep long values in uids
        document.put("uid", object.getUid());
        document.put("studyUid", object.getStudyUid());
        return document;
    }
}
