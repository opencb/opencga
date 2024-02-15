package org.opencb.opencga.catalog.db.mongodb.converters;

import org.opencb.opencga.core.models.notes.Notes;

public class NotesConverter extends OpenCgaMongoConverter<Notes> {

    public NotesConverter() {
        super(Notes.class);
    }

}
