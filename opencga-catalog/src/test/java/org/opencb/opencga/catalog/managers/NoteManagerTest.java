package org.opencb.opencga.catalog.managers;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.notes.NoteCreateParams;
import org.opencb.opencga.core.models.notes.NoteUpdateParams;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.models.study.Study;
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
    public void getOrganizationNoteTest() throws CatalogException {
        NoteCreateParams noteCreateParams1 = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.ORGANIZATION)
                .setVisibility(Note.Visibility.PRIVATE)
                .setValueType(Note.Type.STRING)
                .setValue("hello");
        catalogManager.getNotesManager().create(noteCreateParams1, INCLUDE_RESULT, ownerToken).first();

        NoteCreateParams noteCreateParams2 = new NoteCreateParams()
                .setId("note2")
                .setScope(Note.Scope.ORGANIZATION)
                .setVisibility(Note.Visibility.PUBLIC)
                .setValueType(Note.Type.STRING)
                .setValue("hello");
        catalogManager.getNotesManager().create(noteCreateParams2, INCLUDE_RESULT, ownerToken).first();

        Organization organization = catalogManager.getOrganizationManager().get(organizationId, null, ownerToken).first();
        assertEquals(2, organization.getNotes().size());
        assertEquals(noteCreateParams1.getId(), organization.getNotes().get(0).getId());
        assertEquals(noteCreateParams2.getId(), organization.getNotes().get(1).getId());

        // Check accessibility to private notes
        organization = catalogManager.getOrganizationManager().get(organizationId, null, orgAdminToken1).first();
        assertEquals(2, organization.getNotes().size());
        assertEquals(noteCreateParams1.getId(), organization.getNotes().get(0).getId());
        assertEquals(noteCreateParams2.getId(), organization.getNotes().get(1).getId());

        organization = catalogManager.getOrganizationManager().get(organizationId, null, studyAdminToken1).first();
        assertEquals(1, organization.getNotes().size());
        assertEquals(noteCreateParams2.getId(), organization.getNotes().get(0).getId());

        organization = catalogManager.getOrganizationManager().get(organizationId, null, normalToken1).first();
        assertEquals(1, organization.getNotes().size());
        assertEquals(noteCreateParams2.getId(), organization.getNotes().get(0).getId());
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

    @Test
    public void getStudyNoteTest() throws CatalogException {
        NoteCreateParams noteCreateParams1 = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.STUDY)
                .setVisibility(Note.Visibility.PRIVATE)
                .setStudy(studyFqn)
                .setValueType(Note.Type.STRING)
                .setValue("hello");
        catalogManager.getNotesManager().create(noteCreateParams1, QueryOptions.empty(), ownerToken);

        NoteCreateParams noteCreateParams2 = new NoteCreateParams()
                .setId("note2")
                .setScope(Note.Scope.STUDY)
                .setVisibility(Note.Visibility.PUBLIC)
                .setStudy(studyFqn)
                .setValueType(Note.Type.STRING)
                .setValue("hello");
        catalogManager.getNotesManager().create(noteCreateParams2, QueryOptions.empty(), ownerToken);

        Study study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        assertEquals(2, study.getNotes().size());
        assertEquals(noteCreateParams1.getId(), study.getNotes().get(0).getId());
        assertEquals(noteCreateParams2.getId(), study.getNotes().get(1).getId());

        // Check accessibility to private notes
        study = catalogManager.getStudyManager().get(studyFqn, null, orgAdminToken1).first();
        assertEquals(2, study.getNotes().size());
        assertEquals(noteCreateParams1.getId(), study.getNotes().get(0).getId());
        assertEquals(noteCreateParams2.getId(), study.getNotes().get(1).getId());

        study = catalogManager.getStudyManager().get(studyFqn, null, studyAdminToken1).first();
        assertEquals(2, study.getNotes().size());
        assertEquals(noteCreateParams1.getId(), study.getNotes().get(0).getId());
        assertEquals(noteCreateParams2.getId(), study.getNotes().get(1).getId());

        study = catalogManager.getStudyManager().get(studyFqn, null, normalToken1).first();
        assertEquals(1, study.getNotes().size());
        assertEquals(noteCreateParams2.getId(), study.getNotes().get(0).getId());
    }

    @Test
    public void getNoteTest() throws CatalogException {
        NoteCreateParams study1Note1 = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.STUDY)
                .setVisibility(Note.Visibility.PRIVATE)
                .setStudy(studyFqn)
                .setValueType(Note.Type.STRING)
                .setValue("study1Note1");
        catalogManager.getNotesManager().create(study1Note1, QueryOptions.empty(), ownerToken);

        NoteCreateParams study1Note2 = new NoteCreateParams()
                .setId("note2")
                .setScope(Note.Scope.STUDY)
                .setVisibility(Note.Visibility.PUBLIC)
                .setStudy(studyFqn)
                .setValueType(Note.Type.STRING)
                .setValue("study1Note2");
        catalogManager.getNotesManager().create(study1Note2, QueryOptions.empty(), ownerToken);

        NoteCreateParams study2Note1 = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.STUDY)
                .setVisibility(Note.Visibility.PUBLIC)
                .setStudy(studyFqn2)
                .setValueType(Note.Type.STRING)
                .setValue("study2Note1");
        catalogManager.getNotesManager().create(study2Note1, QueryOptions.empty(), ownerToken);

        NoteCreateParams orgNote1 = new NoteCreateParams()
                .setId("note1")
                .setScope(Note.Scope.ORGANIZATION)
                .setVisibility(Note.Visibility.PRIVATE)
                .setValueType(Note.Type.STRING)
                .setValue("orgNote1");
        catalogManager.getNotesManager().create(orgNote1, INCLUDE_RESULT, ownerToken);

        NoteCreateParams orgNote2 = new NoteCreateParams()
                .setId("note2")
                .setScope(Note.Scope.ORGANIZATION)
                .setVisibility(Note.Visibility.PUBLIC)
                .setValueType(Note.Type.STRING)
                .setValue("orgNote2");
        catalogManager.getNotesManager().create(orgNote2, INCLUDE_RESULT, ownerToken);

        Study study = catalogManager.getStudyManager().get(studyFqn, null, ownerToken).first();
        assertEquals(2, study.getNotes().size());
        assertEquals(study1Note1.getId(), study.getNotes().get(0).getId());
        assertEquals(study1Note1.getValue(), study.getNotes().get(0).getValue());
        assertEquals(study1Note2.getId(), study.getNotes().get(1).getId());
        assertEquals(study1Note2.getValue(), study.getNotes().get(1).getValue());

        study = catalogManager.getStudyManager().get(studyFqn2, null, ownerToken).first();
        assertEquals(1, study.getNotes().size());
        assertEquals(study2Note1.getId(), study.getNotes().get(0).getId());
        assertEquals(study2Note1.getValue(), study.getNotes().get(0).getValue());

        Organization organization = catalogManager.getOrganizationManager().get(organizationId, QueryOptions.empty(), ownerToken).first();
        assertEquals(2, organization.getNotes().size());
        assertEquals(orgNote1.getId(), organization.getNotes().get(0).getId());
        assertEquals(orgNote1.getValue(), organization.getNotes().get(0).getValue());
        assertEquals(orgNote2.getId(), organization.getNotes().get(1).getId());
        assertEquals(orgNote2.getValue(), organization.getNotes().get(1).getValue());
    }

}
