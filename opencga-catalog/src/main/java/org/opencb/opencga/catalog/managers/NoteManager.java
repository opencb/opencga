package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.notes.Note;
import org.opencb.opencga.core.models.notes.NoteCreateParams;
import org.opencb.opencga.core.models.notes.NoteUpdateParams;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class NoteManager extends AbstractManager {

    private final Logger logger;

    NoteManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.logger = LoggerFactory.getLogger(NoteManager.class);
    }

    private OpenCGAResult<Note> internalGet(String organizationId, String noteId) throws CatalogException {
        return internalGet(organizationId, -1L, noteId);
    }

    private OpenCGAResult<Note> internalGet(String organizationId, long studyUid, String noteId) throws CatalogException {
        Query query = new Query();
        if (studyUid > 0) {
            query.put(NoteDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        } else {
            query.put(NoteDBAdaptor.QueryParams.STUDY_UID.key(), -1L);
        }
        if (UuidUtils.isOpenCgaUuid(noteId)) {
            query.put(NoteDBAdaptor.QueryParams.UUID.key(), noteId);
        } else {
            query.put(NoteDBAdaptor.QueryParams.ID.key(), noteId);
        }
        OpenCGAResult<Note> result = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).get(query, QueryOptions.empty());
        if (result.getNumResults() == 0) {
            throw CatalogException.notFound("note", Collections.singletonList(noteId));
        }
        return result;
    }

    public OpenCGAResult<Note> create(NoteCreateParams noteCreateParams, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("note", noteCreateParams)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        try {
            Note note = noteCreateParams.toNote(tokenPayload.getUserId());
            validateNewNote(note, tokenPayload.getUserId());

            // Check permissions to create
            if (note.getScope() == Note.Scope.ORGANIZATION) {
                authorizationManager.checkIsOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());
            } else if (note.getScope() == Note.Scope.STUDY) {
                CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(note.getStudy(), tokenPayload);
                Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
                // Set study fqn and uid
                note.setStudy(study.getFqn());
                note.setStudyUid(study.getUid());
                authorizationManager.checkIsStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());
            } else {
                throw CatalogParameterException.isNull(NoteDBAdaptor.QueryParams.SCOPE.key());
            }

            OpenCGAResult<Note> insert = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).insert(note);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created note
                Query query = new Query(NoteDBAdaptor.QueryParams.UID.key(), note.getUid());
                OpenCGAResult<Note> result = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }

            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, note.getId(), note.getUuid(),
                    note.getStudy(), "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteCreateParams.getId(), "",
                    noteCreateParams.getStudy(), "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            throw e;
        }
    }

    public OpenCGAResult<Note> update(String noteId, NoteUpdateParams noteUpdateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(noteId, null, noteUpdateParams, options, token);
    }

    public OpenCGAResult<Note> update(String noteStr, String studyStr, NoteUpdateParams noteUpdateParams, QueryOptions options,
                                      String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("noteId", noteStr)
                .append("studyStr", studyStr)
                .append("note", noteUpdateParams)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String noteId = noteStr;
        String noteUuid = "";
        String studyId = "";
        String studyUuid = "";
        try {
            long studyUid = -1L;
            if (StringUtils.isNotEmpty(studyStr)) {
                CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
                Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
                studyUid = study.getUid();
                studyId = study.getFqn();
                studyUuid = study.getUuid();
                authorizationManager.checkIsStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());
            } else {
                authorizationManager.checkIsOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());
            }
            Note note = internalGet(organizationId, studyUid, noteStr).first();
            noteId = note.getId();
            noteUuid = note.getUuid();
            studyId = note.getStudy();

            ObjectMap updateMap;
            try {
                updateMap = noteUpdateParams != null ? noteUpdateParams.getUpdateMap() : null;
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse NoteUpdateParams object: " + e.getMessage(), e);
            }

            // Write who's performing the update
            updateMap.put(NoteDBAdaptor.QueryParams.USER_ID.key(), tokenPayload.getUserId(organizationId));

            OpenCGAResult<Note> update = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).update(note.getUid(),
                    updateMap, QueryOptions.empty());
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch updated note
                OpenCGAResult<Note> result = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).get(note.getUid(),
                        options);
                update.setResults(result.getResults());
            }

            auditManager.auditUpdate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, noteUuid, studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return update;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, noteUuid, studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            throw e;
        }
    }

    public static void validateNewNote(Note note, String userId) throws CatalogParameterException {
        ParamUtils.checkIdentifier(note.getId(), NoteDBAdaptor.QueryParams.ID.key());
        ParamUtils.checkObj(note.getScope(), NoteDBAdaptor.QueryParams.SCOPE.key());
        if (note.getScope().equals(Note.Scope.STUDY)) {
            ParamUtils.checkParameter(note.getStudy(), NoteDBAdaptor.QueryParams.STUDY.key());
        }
        ParamUtils.checkObj(note.getVisibility(), NoteDBAdaptor.QueryParams.VISIBILITY.key());
        ParamUtils.checkObj(note.getValueType(), NoteDBAdaptor.QueryParams.VALUE_TYPE.key());
        ParamUtils.checkObj(note.getValue(), NoteDBAdaptor.QueryParams.VALUE.key());

        note.setTags(CollectionUtils.isNotEmpty(note.getTags()) ? note.getTags() : Collections.emptyList());
        note.setUserId(userId);

        note.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.NOTES));
        note.setVersion(1);
        note.setCreationDate(TimeUtils.getTime());
        note.setModificationDate(TimeUtils.getTime());
    }
}
