package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.NoteDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
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
        OpenCGAResult<Note> result = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(organizationId).get(query, QueryOptions.empty());
        if (result.getNumResults() == 0) {
            throw CatalogException.notFound("note", Collections.singletonList(noteId));
        }
        return result;
    }

    public OpenCGAResult<Note> searchOrganizationNote(Query query, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("query", query)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        try {
            Query queryCopy = ParamUtils.defaultObject(query, Query::new);
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);

            if (!authorizationManager.isAtLeastOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId())) {
                String visibility = queryCopy.getString(NoteDBAdaptor.QueryParams.VISIBILITY.key());
                String scope = queryCopy.getString(NoteDBAdaptor.QueryParams.SCOPE.key());
                if (StringUtils.isNotEmpty(visibility) && !Note.Visibility.PUBLIC.name().equals(visibility)) {
                    throw new CatalogAuthorizationException("User '" + tokenPayload.getUserId() + "' is only authorised to see "
                            + Note.Visibility.PUBLIC + " organization notes.");
                }
                if (StringUtils.isNotEmpty(scope) && !Note.Scope.ORGANIZATION.name().equals(scope)) {
                    throw new CatalogAuthorizationException("User '" + tokenPayload.getUserId() + "' is only authorised to see "
                            + "organization notes of scope '" + Note.Scope.ORGANIZATION + "' from this method.");
                }
                queryCopy.put(NoteDBAdaptor.QueryParams.SCOPE.key(), Note.Scope.ORGANIZATION);
                queryCopy.put(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PUBLIC);
            }

            OpenCGAResult<Note> result = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(organizationId).get(queryCopy, optionsCopy);
            auditManager.auditSearch(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (Exception e) {
            auditManager.auditSearch(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note search", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> searchStudyNote(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String studyId = "";
        String studyUuid = "";
        try {
            Query queryCopy = ParamUtils.defaultObject(query, Query::new);
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);

            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
            organizationId = studyFqn.getOrganizationId();
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
            authorizationManager.checkCanViewStudy(organizationId, study.getUid(), tokenPayload.getUserId());
            studyId = study.getId();
            studyUuid = study.getUuid();
            queryCopy.put(NoteDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            if (!authorizationManager.isAtLeastStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId())) {
                String visibility = queryCopy.getString(NoteDBAdaptor.QueryParams.VISIBILITY.key());
                if (StringUtils.isNotEmpty(visibility) && !Note.Visibility.PUBLIC.name().equals(visibility)) {
                    throw new CatalogAuthorizationException("User '" + tokenPayload.getUserId() + "' is only authorised to see "
                            + Note.Visibility.PUBLIC + " study notes.");
                }
                queryCopy.put(NoteDBAdaptor.QueryParams.VISIBILITY.key(), Note.Visibility.PUBLIC);
            }

            OpenCGAResult<Note> result = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(organizationId).get(queryCopy, optionsCopy);
            auditManager.auditSearch(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (Exception e) {
            auditManager.auditSearch(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note search", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> createOrganizationNote(NoteCreateParams noteCreateParams, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("noteCreateParams", noteCreateParams)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        try {
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);
            Note note = noteCreateParams.toNote(Note.Scope.ORGANIZATION, tokenPayload.getUserId());

            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());

            OpenCGAResult<Note> insert = create(note, optionsCopy, tokenPayload);
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, note.getId(), note.getUuid(), "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteCreateParams.getId(), "", "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note create", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> createStudyNote(String studyStr, NoteCreateParams noteCreateParams, QueryOptions options, String token)
            throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("noteCreateParams", noteCreateParams)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String studyId = studyStr;
        String studyUuid = "";
        try {
            ParamUtils.checkParameter(studyStr, "study");
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);
            Note note = noteCreateParams.toNote(Note.Scope.STUDY, tokenPayload.getUserId());

            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
            // Set study fqn and uid
            note.setStudy(study.getFqn());
            note.setStudyUid(study.getUid());
            studyId = study.getFqn();
            studyUuid = study.getUuid();
            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());

            OpenCGAResult<Note> insert = create(note, optionsCopy, tokenPayload);
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, note.getId(), note.getUuid(), studyId,
                    studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return insert;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteCreateParams.getId(), "", studyId,
                    studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note create",
                            e.getMessage())));
            throw e;
        }
    }

    private OpenCGAResult<Note> create(Note note, QueryOptions options, JwtPayload tokenPayload) throws CatalogException {
        String organizationId = tokenPayload.getOrganization();
        validateNewNote(note, tokenPayload.getUserId());
        OpenCGAResult<Note> insert = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(organizationId).insert(note);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch created note
            Query query = new Query(NoteDBAdaptor.QueryParams.UID.key(), note.getUid());
            OpenCGAResult<Note> result = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(organizationId).get(query, options);
            insert.setResults(result.getResults());
        }
        return insert;
    }

    public OpenCGAResult<Note> updateOrganizationNote(String noteStr, NoteUpdateParams noteUpdateParams, QueryOptions options,
                                                      String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("noteId", noteStr)
                .append("update", noteUpdateParams)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String noteId = noteStr;
        String noteUuid = "";
        try {
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());
            Note note = internalGet(organizationId, -1L, noteStr).first();
            noteId = note.getId();
            noteUuid = note.getUuid();

            OpenCGAResult<Note> update = update(note.getUid(), noteUpdateParams, optionsCopy, tokenPayload);
            auditManager.auditUpdate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, noteUuid, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return update;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, noteUuid, "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note update", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> updateStudyNote(String studyStr, String noteStr, NoteUpdateParams noteUpdateParams, QueryOptions options,
                                               String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("noteId", noteStr)
                .append("update", noteUpdateParams)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String noteId = noteStr;
        String noteUuid = "";
        String studyId = "";
        String studyUuid = "";
        try {
            ParamUtils.checkParameter(studyStr, "study");
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);

            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
            Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
            studyId = study.getFqn();
            studyUuid = study.getUuid();
            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());

            Note note = internalGet(organizationId, study.getUid(), noteStr).first();
            noteId = note.getId();
            noteUuid = note.getUuid();

            OpenCGAResult<Note> update = update(note.getUid(), noteUpdateParams, optionsCopy, tokenPayload);
            auditManager.auditUpdate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, noteUuid, studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return update;
        } catch (Exception e) {
            auditManager.auditCreate(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, noteUuid, studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note update", e.getMessage())));
            throw e;
        }
    }

    private OpenCGAResult<Note> update(long noteUid, NoteUpdateParams noteUpdateParams, QueryOptions options, JwtPayload tokenPayload)
            throws CatalogException {
        ObjectMap updateMap;
        try {
            updateMap = noteUpdateParams != null ? noteUpdateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse NoteUpdateParams object: " + e.getMessage(), e);
        }
        String organizationId = tokenPayload.getOrganization();

        // Write who's performing the update
        updateMap.put(NoteDBAdaptor.QueryParams.USER_ID.key(), tokenPayload.getUserId());

        OpenCGAResult<Note> update = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(organizationId).update(noteUid, updateMap,
                options);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            // Fetch updated note
            OpenCGAResult<Note> result = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(organizationId).get(noteUid, options);
            update.setResults(result.getResults());
        }
        return update;
    }

    public OpenCGAResult<Note> deleteOrganizationNote(String noteId, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("noteId", noteId)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        try {
            authorizationManager.checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, tokenPayload.getUserId());

            ParamUtils.checkParameter(noteId, "note id");
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);

            Note note = internalGet(organizationId, -1L, noteId).first();

            OpenCGAResult<Note> delete = delete(note, optionsCopy, tokenPayload);
            auditManager.auditDelete(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, note.getId(), note.getUuid(), "", "",
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return delete;
        } catch (Exception e) {
            auditManager.auditDelete(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, "", "", "", auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note delete", e.getMessage())));
            throw e;
        }
    }

    public OpenCGAResult<Note> deleteStudyNote(String studyStr, String noteId, QueryOptions options, String token) throws CatalogException {
        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("noteId", noteId)
                .append("options", options)
                .append("token", token);

        JwtPayload tokenPayload = catalogManager.getUserManager().validateToken(token);
        String organizationId = tokenPayload.getOrganization();
        String studyId = studyStr;
        String studyUuid = "";
        try {
            QueryOptions optionsCopy = ParamUtils.defaultObject(options, QueryOptions::new);

            ParamUtils.checkParameter(studyStr, "study");
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyStr, tokenPayload);
            organizationId = studyFqn.getOrganizationId();

            Study study = catalogManager.getStudyManager().resolveId(studyFqn, null, tokenPayload);
            // Set study fqn and uid
            studyId = study.getFqn();
            studyUuid = study.getUuid();
            authorizationManager.checkIsAtLeastStudyAdministrator(organizationId, study.getUid(), tokenPayload.getUserId());

            ParamUtils.checkParameter(noteId, "note id");

            Note note = internalGet(organizationId, study.getUid(), noteId).first();

            OpenCGAResult<Note> delete = delete(note, optionsCopy, tokenPayload);
            auditManager.auditDelete(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, note.getId(), note.getUuid(),
                    studyId, studyUuid, auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return delete;
        } catch (Exception e) {
            auditManager.auditDelete(organizationId, tokenPayload.getUserId(), Enums.Resource.NOTE, noteId, "", studyId, studyUuid,
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "Note delete", e.getMessage())));
            throw e;
        }
    }

    private OpenCGAResult<Note> delete(Note note, QueryOptions options, JwtPayload jwtPayload) throws CatalogException {
        OpenCGAResult<Note> delete = getCatalogDBAdaptorFactory().getCatalogNoteDBAdaptor(jwtPayload.getOrganization()).delete(note);
        if (options.getBoolean(ParamConstants.INCLUDE_RESULT_PARAM)) {
            delete.setResults(Collections.singletonList(note));
        }
        return delete;
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
