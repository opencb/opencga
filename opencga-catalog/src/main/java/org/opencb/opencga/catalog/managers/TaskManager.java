package org.opencb.opencga.catalog.managers;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.TaskDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.Task;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.OpenCGAResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opencb.opencga.catalog.utils.Constants.DELETE_EXTERNAL_FILES;
import static org.opencb.opencga.catalog.utils.Constants.SKIP_TRASH;

public class TaskManager extends ResourceManager<Task> {

    protected static Logger logger = LoggerFactory.getLogger(TaskManager.class);
    private UserManager userManager;
    private StudyManager studyManager;

    TaskManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
               DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.TASK;
    }

    @Override
    OpenCGAResult<Task> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException {
        if (!authorizationManager.checkIsAdmin(user)) {
            throw CatalogAuthorizationException.adminOnlySupportedOperation();
        }

        ParamUtils.checkIsSingleID(entry);
        Query queryCopy = query == null ? new Query() : new Query(query);
        if (studyUid > 0) {
            queryCopy.put(TaskDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        }

        if (UUIDUtils.isOpenCGAUUID(entry)) {
            queryCopy.put(TaskDBAdaptor.QueryParams.UUID.key(), entry);
        } else {
            queryCopy.put(TaskDBAdaptor.QueryParams.ID.key(), entry);
        }

        OpenCGAResult<Task> taskDataResult = taskDBAdaptor.get(queryCopy, options);
        if (taskDataResult.getNumResults() == 0) {
            throw new CatalogException("Task " + entry + " not found");
        } else if (taskDataResult.getNumResults() > 1) {
            throw new CatalogException("More than one task found based on " + entry);
        } else {
            return taskDataResult;
        }
    }

    @Override
    InternalGetDataResult<Task> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options, String user,
                                            boolean ignoreException) throws CatalogException {
        if (!authorizationManager.checkIsAdmin(user)) {
            throw CatalogAuthorizationException.adminOnlySupportedOperation();
        }

        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing task entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));
        Query queryCopy = query == null ? new Query() : new Query(query);
        if (studyUid > 0) {
            queryCopy.put(TaskDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        }

        Function<Task, String> taskStringFunction = Task::getId;
        TaskDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            TaskDBAdaptor.QueryParams param = TaskDBAdaptor.QueryParams.ID;
            if (UUIDUtils.isOpenCGAUUID(entry)) {
                param = TaskDBAdaptor.QueryParams.UUID;
                taskStringFunction = Task::getUuid;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Task> taskDataResult = taskDBAdaptor.get(queryCopy, queryOptions);
        if (ignoreException || taskDataResult.getNumResults() == uniqueList.size()) {
            return keepOriginalOrder(uniqueList, taskStringFunction, taskDataResult, ignoreException, false);
        } else {
            throw CatalogException.notFound("tasks", getMissingFields(uniqueList, taskDataResult.getResults(), taskStringFunction));
        }
    }

    @Override
    public OpenCGAResult<Task> create(String studyStr, Task task, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getUserId(token);
        String studyId = studyStr;
        String studyUuid = "";

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("task", task)
                .append("options", options)
                .append("token", token);
        try {
            if (!authorizationManager.checkIsAdmin(userId)) {
                throw CatalogAuthorizationException.adminOnlySupportedOperation();
            }

            validateTask(task, userId);

            long studyUid = -1;
            if (StringUtils.isNotEmpty(studyStr)) {
                Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_STUDY_ID);
                studyUid = study.getUid();
                studyId = study.getId();
                studyUuid = study.getUuid();
            }

            taskDBAdaptor.insert(studyUid, task, QueryOptions.empty());
            auditManager.auditCreate(userId, Enums.Resource.TASK, task.getId(), task.getUuid(), studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return getTask(task.getUuid(), options);
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.TASK, task.getId(), "", studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public DBIterator<Task> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String studyId = studyStr;
        String studyUuid = "";

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("options", options)
                .append("token", token);

        String userId = userManager.getUserId(token);

        try {
            if (!authorizationManager.checkIsAdmin(userId)) {
                throw CatalogAuthorizationException.adminOnlySupportedOperation();
            }

            if (StringUtils.isNotEmpty(studyStr)) {
                Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_STUDY_ID);
                studyId = study.getId();
                studyUuid = study.getUuid();
                query.put(TaskDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            }

            DBIterator<Task> iterator = taskDBAdaptor.iterator(query, options);
            auditManager.auditSearch(userId, Enums.Resource.TASK, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return iterator;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.TASK, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Task> search(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String studyId = studyStr;
        String studyUuid = "";

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("options", options)
                .append("token", token);

        String userId = userManager.getUserId(token);

        try {
            if (!authorizationManager.checkIsAdmin(userId)) {
                throw CatalogAuthorizationException.adminOnlySupportedOperation();
            }

            if (StringUtils.isNotEmpty(studyStr)) {
                Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_STUDY_ID);
                studyId = study.getId();
                studyUuid = study.getUuid();
                query.put(TaskDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            }

            OpenCGAResult<Task> result = taskDBAdaptor.get(query, options);
            auditManager.auditSearch(userId, Enums.Resource.TASK, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.TASK, studyId, studyUuid, auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<Task> count(String studyStr, Query query, String token) throws CatalogException {
        return null;
    }

    public OpenCGAResult<Task> deleteFile(String studyStr, List<String> fileIds, ObjectMap params, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("resource", Enums.Resource.FILE)
                .append("action", Enums.Action.DELETE)
                .append("study", studyStr)
                .append("files", fileIds)
                .append("params", params)
                .append("token", token);

        Map<String, Object> taskParams = new HashMap<>();
        taskParams.put("study", studyStr);
        taskParams.put("files", fileIds);
        taskParams.put("params", params);
        try {
            FileManager fileManager = catalogManager.getFileManager();

            InternalGetDataResult<File> fileResult = fileManager.internalGet(study.getUid(), fileIds, FileManager.INCLUDE_FILE_URI_PATH,
                    userId, false);
            boolean physicalDelete = params.getBoolean(SKIP_TRASH, false) || params.getBoolean(DELETE_EXTERNAL_FILES, false);
            for (File file : fileResult.getResults()) {
                fileManager.checkCanDeleteFile(studyStr, file.getPath(), physicalDelete, userId);
            }

            // Set the status of all files to PENDING_DELETE
            ObjectMap updateParams = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE);
            for (File file : fileResult.getResults()) {
                Query fileQuery = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(FileDBAdaptor.QueryParams.PATH.key(), file.getType() == File.Type.FILE
                                ? file.getPath()
                                : "~^" + file.getPath() + "*");
                fileDBAdaptor.update(fileQuery, updateParams, QueryOptions.empty());
            }

            Task task = generateValidTask(Enums.Resource.FILE, Enums.Action.DELETE, taskParams, new Enums.ExecutionStatus(),
                    Enums.Priority.MEDIUM, userId);

            OpenCGAResult<Task> result = taskDBAdaptor.insert(study.getUid(), task, QueryOptions.empty());
            auditManager.auditCreate(userId, Enums.Resource.TASK, task.getId(), task.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            Task task = generateValidTask(Enums.Resource.FILE, Enums.Action.DELETE, taskParams,
                    new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED), Enums.Priority.MEDIUM, userId);

            auditManager.auditCreate(userId, Enums.Resource.TASK, task.getId(), task.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR));

            throw e;
        }
    }

    public OpenCGAResult<Task> deleteFile(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("resource", Enums.Resource.FILE)
                .append("action", Enums.Action.DELETE)
                .append("study", studyStr)
                .append("query", query)
                .append("params", params)
                .append("token", token);

        Map<String, Object> taskParams = new HashMap<>();
        taskParams.put("study", studyStr);
        taskParams.put("query", query);
        taskParams.put("params", params);
        try {
            FileManager fileManager = catalogManager.getFileManager();
            boolean physicalDelete = params.getBoolean(SKIP_TRASH, false) || params.getBoolean(DELETE_EXTERNAL_FILES, false);

            DBIterator<File> iterator = fileManager.iterator(studyStr, query, FileManager.INCLUDE_FILE_URI_PATH, token);
            while (iterator.hasNext()) {
                fileManager.checkCanDeleteFile(studyStr, iterator.next().getPath(), physicalDelete, userId);
            }

            // Set the status of all files to PENDING_DELETE
            iterator = fileManager.iterator(studyStr, query, FileManager.INCLUDE_FILE_URI_PATH, token);
            ObjectMap updateParams = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE);
            while (iterator.hasNext()) {
                File file = iterator.next();
                Query fileQuery = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(FileDBAdaptor.QueryParams.PATH.key(), file.getType() == File.Type.FILE
                                ? file.getPath()
                                : "~^" + file.getPath() + "*");
                fileDBAdaptor.update(fileQuery, updateParams, QueryOptions.empty());
            }

            Task task = generateValidTask(Enums.Resource.FILE, Enums.Action.DELETE, taskParams, new Enums.ExecutionStatus(),
                    Enums.Priority.MEDIUM, userId);

            OpenCGAResult<Task> result = taskDBAdaptor.insert(study.getUid(), task, QueryOptions.empty());
            auditManager.auditCreate(userId, Enums.Resource.TASK, task.getId(), task.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            Task task = generateValidTask(Enums.Resource.FILE, Enums.Action.DELETE, taskParams,
                    new Enums.ExecutionStatus(Enums.ExecutionStatus.ABORTED), Enums.Priority.MEDIUM, userId);

            auditManager.auditCreate(userId, Enums.Resource.TASK, task.getId(), task.getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR));

            throw e;
        }
    }

    private Task generateValidTask(Enums.Resource resource, Enums.Action action, Map<String, Object> params, Enums.ExecutionStatus status,
                                   Enums.Priority priority, String userId) {
        String creationDate = TimeUtils.getTime();
        return new Task()
                .setId(resource + "__" + action + "__" + creationDate + "__" + org.opencb.commons.utils.StringUtils.randomString(6))
                .setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.TASK))
                .setResource(resource)
                .setAction(action)
                .setUserId(userId)
                .setCreationDate(creationDate)
                .setStatus(status)
                .setPriority(priority)
                .setParams(params);
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> ids, ObjectMap params, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        return null;
    }


    // Private methods
    private void validateTask(Task task, String userId) throws CatalogParameterException {
        ParamUtils.checkObj(task.getResource(), "resource");
        ParamUtils.checkObj(task.getAction(), "action");
        ParamUtils.checkObj(task.getPriority(), "priority");
        if (MapUtils.isEmpty(task.getParams())) {
            throw new CatalogParameterException("Missing 'params' map");
        }

        task.setUserId(userId);
        task.setCreationDate(TimeUtils.getTime());
        task.setStatus(new Enums.ExecutionStatus(Enums.ExecutionStatus.PENDING));
        task.setId(task.getResource().name().toLowerCase() + "_" + task.getAction().name().toLowerCase() + "_" + TimeUtils.getTime()
                + "-" + org.opencb.commons.utils.StringUtils.randomString(6));
        task.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.TASK));
    }

    private OpenCGAResult<Task> getTask(String taskUuid, QueryOptions options) throws CatalogDBException {
        Query query = new Query(TaskDBAdaptor.QueryParams.UUID.key(), taskUuid);
        return taskDBAdaptor.get(query, options);
    }

}
