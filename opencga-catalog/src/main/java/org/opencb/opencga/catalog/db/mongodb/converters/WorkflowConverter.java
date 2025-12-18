package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.opencga.core.models.externalTool.ExternalTool;

public class WorkflowConverter extends OpenCgaMongoConverter<ExternalTool> {

    public WorkflowConverter() {
        super(ExternalTool.class);
    }

    @Override
    public Document convertToStorageType(ExternalTool object) {
        Document document = super.convertToStorageType(object);
        // Keep long values in uids
        document.put("uid", object.getUid());
        document.put("studyUid", object.getStudyUid());
        return document;
    }
}
