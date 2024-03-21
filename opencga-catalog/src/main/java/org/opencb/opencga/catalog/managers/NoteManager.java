package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
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

import javax.annotation.Nullable;
import java.util.Collections;

public class NoteManager extends AbstractManager {

    private final Logger logger;

    NoteManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);
        this.logger = LoggerFactory.getLogger(NoteManager.class);
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

    public OpenCGAResult<Note> search(Note.Scope scope, Query query, QueryOptions options, String token) throws CatalogException {
        return search(null, scope, query, options, token);
    }

    public OpenCGAResult<Note> search(@Nullable String studyStr, Note.Scope scope, Query query, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("scope", scope)
                .append("query", query)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String studyId = studyStr != null ? studyStr : "";
        String studyUuid = "";
        try {
            query = ParamUtils.defaultObject(query, Query::new);
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            // Check permissions to create
            if (scope == Note.Scope.ORGANIZATION) {
                organizationId = tokenPayload.getOrganization();
                authorizationManager.checkUserBelongsToOrganization(organizationId, tokenPayload.getUserId());

                if (!authorizationManager.isOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId())) {
                    // Only show public notes
                    query.put(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PUBLIC);
                }
            } else if (scope == Note.Scope.STUDY) {
                CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
                organizationId = studyFqn.getOrganizationId();
                Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
                studyId = study.getId();
                studyUuid = study.getUuid();
                authorizationManager.checkCanViewStudy(organizationId, study.getUid(), tokenPayload.getUserId());

                if (!authorizationManager.isStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId())) {
                    // Only show public notes
                    query.put(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PUBLIC);
                }
            } else {
                throw CatalogParameterException.isNull(NoteDBAdaptor.QueryParams.SCOPE.key());
            }

            OpenCGAResult<Note> result = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).get(query, options);
            auditManager.auditSearch(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (Exception e) {
            auditManager.auditSearch(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note search", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> create(NoteCreateParams noteCreateParams, QueryOptions options, String token) throws CatalogException {
        return create(null, noteCreateParams, options, token);
    }

    public OpenCGAResult<Note> create(@Nullable String studyStr, NoteCreateParams noteCreateParams, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("noteCreateParams", noteCreateParams)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String studyId = studyStr;
        String studyUuid = "";
        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            Note note = noteCreateParams.toNote(tokenPayload.getUserId());

            // Check permissions to create
            if (note.getScope() == Note.Scope.ORGANIZATION) {
                authorizationManager.checkIsOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());
            } else if (note.getScope() == Note.Scope.STUDY) {
                ParamUtils.checkParameter(studyStr, "study");
                CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
                Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
                // Set study fqn and uid
                note.setStudy(study.getFqn());
                note.setStudyUid(study.getUid());
                studyId = study.getFqn();
                studyUuid = study.getUuid();
                authorizationManager.checkIsStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());
            } else {
                throw CatalogParameterException.isNull(NoteDBAdaptor.QueryParams.SCOPE.key());
            }

            validateNewNote(note, tokenPayload.getUserId());
            OpenCGAResult<Note> insert = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).insert(note);
            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                // Fetch created note
                Query query = new Query(NoteDBAdaptor.QueryParams.UID.key(), note.getUid());
                OpenCGAResult<Note> result = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).get(query, options);
                insert.setResults(result.getResults());
            }

            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, note.getId(), note.getUuid(),
                    studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteCreateParams.getId(), "",
                    studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                            new Error(0, "Note create", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> update(Note.Scope scope, String noteId, NoteUpdateParams noteUpdateParams, QueryOptions options,
                                      String token) throws CatalogException {
        return update(scope, null, noteId, noteUpdateParams, options, token);
    }

    public OpenCGAResult<Note> update(Note.Scope scope, @Nullable String studyStr, String noteStr, NoteUpdateParams noteUpdateParams,
                                      QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("scope", scope)
                .append("studyStr", studyStr)
                .append("noteId", noteStr)
                .append("note", noteUpdateParams)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String noteId = noteStr;
        String noteUuid = "";
        String studyId = "";
        String studyUuid = "";
        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            long studyUid = -1L;
            if (scope == Note.Scope.STUDY) {
                ParamUtils.checkParameter(studyStr, "study");
                CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
                Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
                studyUid = study.getUid();
                studyId = study.getFqn();
                studyUuid = study.getUuid();
                authorizationManager.checkIsStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());
            } else if (scope == Note.Scope.ORGANIZATION) {
                authorizationManager.checkIsOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());
            } else {
                throw CatalogParameterException.isNull(NoteDBAdaptor.QueryParams.SCOPE.key());
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
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note update", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> delete(Note.Scope scope, String noteId, QueryOptions options, String token) throws CatalogException {
        return delete(null, scope, noteId, options, token);
    }

    public OpenCGAResult<Note> delete(@Nullable String studyStr, Note.Scope scope, String noteId, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("scope", scope)
                .append("noteId", noteId)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String studyId = studyStr;
        String studyUuid = "";
        try {
            options = ParamUtils.defaultObject(options, QueryOptions::new);
            long studyUid = -1;

            // Check permissions to delete note
            if (scope == Note.Scope.ORGANIZATION) {
                authorizationManager.checkIsOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());
            } else if (scope == Note.Scope.STUDY) {
                ParamUtils.checkParameter(studyStr, "study");
                CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
                organizationId = studyFqn.getOrganizationId();

                Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
                // Set study fqn and uid
                studyId = study.getFqn();
                studyUuid = study.getUuid();
                studyUid = study.getUid();
                authorizationManager.checkIsStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());
            } else {
                throw CatalogParameterException.isNull(NoteDBAdaptor.QueryParams.SCOPE.key());
            }
            ParamUtils.checkParameter(noteId, "note id");
            ParamUtils.checkObj(scope, "scope");

            Note note = internalGet(organizationId, studyUid, noteId).first();
            OpenCGAResult<Note> delete = catalogDBAdaptorFactory.getCatalogNoteDBAdaptor(organizationId).delete(note);

            if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
                delete.setResults(Collections.singletonList(note));
            }

            auditManager.auditDelete(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, note.getId(), note.getUuid(),
                    note.getStudy(), "", auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return delete;
        } catch (Exception e) {
            auditManager.auditDelete(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, "",
                    studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                            new Error(0, "Note delete", e.getMessage())));
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
