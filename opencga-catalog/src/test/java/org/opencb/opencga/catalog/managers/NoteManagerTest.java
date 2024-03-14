package org.opencb.opencga.catalog.managers;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.notes.NoteCreateParams;
import org.opencb.opencga.core.models.notes.NoteUpdateParams;
import org.opencb.opencga.core.testclassification.duration.MediumTests;

import java.util.Arrays;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class NoteManagerTest extends AbstractManagerTest {

    @Test
    public void createOrganizationNoteTest() throws CatalogException {
        NoteCreateParams noteCreateParams = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.ORGANIZATION)
                .setVisibility(Note.Visibility.PRIVATE)
                .setValueType(Note.Type.STRING)
                .setValue("hello");
        Note note = catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, ownerToken).first();
        assertEquals(noteCreateParams.getId(), note.getId());
        assertEquals(orgOwnerUserId, note.getUserId());

        noteCreateParams.setId("note2");
        note = catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, orgAdminToken1).first();
        assertEquals(noteCreateParams.getId(), note.getId());
        assertEquals(orgAdminUserId1, note.getUserId());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("denied");
        catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, studyAdminToken1).first();
    }

    @Test
    public void updateOrganizationNoteTest() throws CatalogException {
        NoteCreateParams noteCreateParams = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.ORGANIZATION)
                .setVisibility(Note.Visibility.PRIVATE)
                .setValueType(Note.Type.STRING)
                .setValue("hello");
        Note note = catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, ownerToken).first();
        assertEquals(1, note.getVersion());

        NoteUpdateParams noteUpdateParams = new NoteUpdateParams()
                .setTags(Arrays.asList("tag1", "tag2"));
        note = catalogManager.getNotesManager().update(note.getId(), noteUpdateParams, INCLUDE_RESULT, ownerToken).first();
        assertEquals(2, note.getVersion());
        assertEquals(orgOwnerUserId, note.getUserId());
        assertEquals(2, note.getTags().size());
        assertArrayEquals(noteUpdateParams.getTags().toArray(), note.getTags().toArray());

        noteUpdateParams = new NoteUpdateParams()
                .setVisibility(Note.Visibility.PUBLIC);
        note = catalogManager.getNotesManager().update(note.getId(), noteUpdateParams, INCLUDE_RESULT, orgAdminToken1).first();
        assertEquals(3, note.getVersion());
        assertEquals(orgAdminUserId1, note.getUserId());
        assertEquals(noteUpdateParams.getVisibility(), note.getVisibility());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("denied");
        catalogManager.getNotesManager().update(note.getId(), noteUpdateParams, INCLUDE_RESULT, studyAdminToken1).first();
    }

    @Test
    public void createStudyNoteTest() throws CatalogException {
        NoteCreateParams noteCreateParams = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.STUDY)
                .setVisibility(Note.Visibility.PRIVATE)
                .setValueType(Note.Type.STRING)
                .setValue("hello");
        assertThrows(CatalogParameterException.class, () -> catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, ownerToken));

        noteCreateParams.setStudy(studyFqn);
        Note note = catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, ownerToken).first();
        assertEquals(noteCreateParams.getId(), note.getId());
        assertEquals(orgOwnerUserId, note.getUserId());

        noteCreateParams.setId("note2");
        note = catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, orgAdminToken1).first();
        assertEquals(noteCreateParams.getId(), note.getId());
        assertEquals(orgAdminUserId1, note.getUserId());

        noteCreateParams.setId("note3");
        note = catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, studyAdminToken1).first();
        assertEquals(noteCreateParams.getId(), note.getId());
        assertEquals(studyAdminUserId1, note.getUserId());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("denied");
        catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, normalToken1).first();
    }

    @Test
    public void updateStudyNoteTest() throws CatalogException {
        NoteCreateParams noteCreateParams = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.STUDY)
                .setVisibility(Note.Visibility.PRIVATE)
                .setValueType(Note.Type.STRING)
                .setValue("hello")
                .setStudy(studyFqn);
        Note note = catalogManager.getNotesManager().create(noteCreateParams, INCLUDE_RESULT, ownerToken).first();
        assertEquals(1, note.getVersion());

        NoteUpdateParams noteUpdateParams = new NoteUpdateParams()
                .setTags(Arrays.asList("tag1", "tag2"));
        note = catalogManager.getNotesManager().update(note.getId(), studyFqn, noteUpdateParams, INCLUDE_RESULT, ownerToken).first();
        assertEquals(2, note.getVersion());
        assertEquals(orgOwnerUserId, note.getUserId());
        assertEquals(2, note.getTags().size());
        assertArrayEquals(noteUpdateParams.getTags().toArray(), note.getTags().toArray());

        noteUpdateParams = new NoteUpdateParams()
                .setVisibility(Note.Visibility.PUBLIC);
        note = catalogManager.getNotesManager().update(note.getId(), studyFqn, noteUpdateParams, INCLUDE_RESULT, orgAdminToken1).first();
        assertEquals(3, note.getVersion());
        assertEquals(orgAdminUserId1, note.getUserId());
        assertEquals(noteUpdateParams.getVisibility(), note.getVisibility());

        noteUpdateParams = new NoteUpdateParams()
                .setValue("my new value");
        note = catalogManager.getNotesManager().update(note.getId(), studyFqn, noteUpdateParams, INCLUDE_RESULT, studyAdminToken1).first();
        assertEquals(4, note.getVersion());
        assertEquals(studyAdminUserId1, note.getUserId());
        assertEquals(noteUpdateParams.getValue(), note.getValue());

        thrown.expect(CatalogAuthorizationException.class);
        thrown.expectMessage("denied");
        catalogManager.getNotesManager().update(note.getId(), studyFqn, noteUpdateParams, INCLUDE_RESULT, normalToken1);
    }

}
