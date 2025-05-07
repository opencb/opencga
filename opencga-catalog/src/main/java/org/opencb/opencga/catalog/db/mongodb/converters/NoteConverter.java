package org.opencb.opencga.catalog.db.mongodb.converters;

import org.opencb.opencga.core.models.notes.Note;

public class NoteConverter extends OpenCgaMongoConverter<Note> {

    public NoteConverter() {
        super(Note.class);
    }

}
