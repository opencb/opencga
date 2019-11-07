package org.opencb.opencga.catalog.db.mongodb.converters;

import org.bson.Document;
import org.opencb.commons.datastore.mongodb.GenericDocumentComplexConverter;
import org.opencb.opencga.core.models.Task;

public class TaskConverter extends GenericDocumentComplexConverter<Task> {

    public TaskConverter() {
        super(Task.class);
    }

    @Override
    public Document convertToStorageType(Task task) {
        Document document = super.convertToStorageType(task);
        document.put("uid", task.getUid());
        document.put("studyUid", task.getStudyUid());
        return document;
    }

}
