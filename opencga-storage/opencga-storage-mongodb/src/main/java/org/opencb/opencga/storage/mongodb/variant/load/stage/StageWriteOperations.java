package org.opencb.opencga.storage.mongodb.variant.load.stage;

import com.google.common.collect.ListMultimap;
import org.bson.Document;
import org.bson.types.Binary;

public class StageWriteOperations {

    private final ListMultimap<Document, Binary> documents;

    public StageWriteOperations(ListMultimap<Document, Binary> documents) {
        this.documents = documents;
    }

    public ListMultimap<Document, Binary> getDocuments() {
        return documents;
    }

}
