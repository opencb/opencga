package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.CatalogDBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.CatalogDatasetDBAdaptor;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.DatasetAclEntry;
import org.opencb.opencga.catalog.models.acls.FileAclEntry;
import org.opencb.opencga.catalog.models.acls.StudyAclEntry;
import org.opencb.opencga.catalog.utils.BioformatDetector;
import org.opencb.opencga.catalog.utils.FormatDetector;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileManager extends AbstractManager implements IFileManager {

    private static final QueryOptions INCLUDE_STUDY_URI;
    private static final QueryOptions INCLUDE_FILE_URI_PATH;
    private static final Comparator<File> ROOT_FIRST_COMPARATOR;
    private static final Comparator<File> ROOT_LAST_COMPARATOR;

    protected static Logger logger;

    public static final String SKIP_TRASH = "SKIP_TRASH";
    public static final String DELETE_EXTERNAL_FILES = "DELETE_EXTERNAL_FILES";
    public static final String FORCE_DELETE = "FORCE_DELETE";

    static {
        INCLUDE_STUDY_URI = new QueryOptions("include", Collections.singletonList("projects.studies.uri"));
        INCLUDE_FILE_URI_PATH = new QueryOptions("include", Arrays.asList("projects.studies.files.uri", "projects.studies.files.path"));
        ROOT_FIRST_COMPARATOR = (f1, f2) -> (f1.getPath() == null ? 0 : f1.getPath().length())
                - (f2.getPath() == null ? 0 : f2.getPath().length());
        ROOT_LAST_COMPARATOR = (f1, f2) -> (f2.getPath() == null ? 0 : f2.getPath().length())
                - (f1.getPath() == null ? 0 : f1.getPath().length());

        logger = LoggerFactory.getLogger(FileManager.class);
    }

    @Deprecated
    public FileManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                       CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public FileManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager, AuditManager auditManager,
                       CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogConfiguration);
    }

    public static List<String> getParentPaths(String filePath) {
        String path = "";
        String[] split = filePath.split("/");
        List<String> paths = new ArrayList<>(split.length + 1);
        paths.add("");  //Add study root folder
        //Add intermediate folders
        //Do not add the last split, could be a file or a folder..
        //Depending on this, it could end with '/' or not.
        for (int i = 0; i < split.length - 1; i++) {
            String f = split[i];
            path = path + f + "/";
            paths.add(path);
        }
        paths.add(filePath); //Add the file path
        return paths;
    }

    @Override
    public URI getStudyUri(long studyId) throws CatalogException {
        return studyDBAdaptor.getStudy(studyId, INCLUDE_STUDY_URI).first().getUri();
    }

    @Override
    public URI getFileUri(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            // This should never be executed, since version 0.8-rc1 the URI is stored always.
            return getFileUri(studyDBAdaptor.getStudy(getStudyId(file.getId()), INCLUDE_STUDY_URI).first(), file);
        }
    }

    @Override
    public URI getFileUri(Study study, File file) throws CatalogException {
        ParamUtils.checkObj(study, "Study");
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            QueryResult<File> parents = getParents(file, false, INCLUDE_FILE_URI_PATH);
            for (File parent : parents.getResult()) {
                if (parent.getUri() != null) {
                    String relativePath = file.getPath().replaceFirst(parent.getPath(), "");
                    return parent.getUri().resolve(relativePath);
                }
            }
            URI studyUri = study.getUri() == null ? getStudyUri(study.getId()) : study.getUri();
            return file.getPath().isEmpty()
                    ? studyUri
                    : catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, file.getPath());
        }
    }

    @Deprecated
    @Override
    public URI getFileUri(URI studyUri, String relativeFilePath) throws CatalogException {
        ParamUtils.checkObj(studyUri, "studyUri");
        ParamUtils.checkObj(relativeFilePath, "relativeFilePath");

        return relativeFilePath.isEmpty()
                ? studyUri
                : catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
    }

    public URI getFileUri(long studyId, String filePath) throws CatalogException {
        ParamUtils.checkObj(filePath, "filePath");

        List<File> parents = getParents(false, new QueryOptions("include", "projects.studies.files.path,projects.studies.files.uri"),
                filePath, studyId).getResult();

        for (File parent : parents) {
            if (parent.getUri() != null) {
                String relativePath = filePath.replaceFirst(parent.getPath(), "");
                return Paths.get(parent.getUri()).resolve(relativePath).toUri();
            }
        }
        URI studyUri = getStudyUri(studyId);
        return filePath.isEmpty()
                ? studyUri
                : catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, filePath);
    }

    @Override
    public String getUserId(long fileId) throws CatalogException {
        return fileDBAdaptor.getFileOwnerId(fileId);
    }

    @Override
    public Long getStudyId(long fileId) throws CatalogException {
        return fileDBAdaptor.getStudyIdByFileId(fileId);
    }

    @Override
    public Long getFileId(String userId, String fileStr) throws CatalogException {
        if (StringUtils.isNumeric(fileStr)) {
            return Long.parseLong(fileStr);
        }

        // Resolve the studyIds and filter the fileStr
        ObjectMap parsedSampleStr = parseFeatureId(userId, fileStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String fileName = parsedSampleStr.getString("featureName");

        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }

        // We search as a path
        Query query = new Query(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), fileName)
                .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=EMPTY");
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.files.id");
        QueryResult<File> pathQueryResult = fileDBAdaptor.get(query, qOptions);
        if (pathQueryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one file id found based on " + fileName);
        }

        if (!fileName.contains("/")) {
            // We search as a fileName as well
            query = new Query(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                    .append(CatalogFileDBAdaptor.QueryParams.NAME.key(), fileName)
                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=EMPTY");
            QueryResult<File> nameQueryResult = fileDBAdaptor.get(query, qOptions);
            if (nameQueryResult.getNumResults() > 1) {
                throw new CatalogException("Error: More than one file id found based on " + fileName);
            }

            if (pathQueryResult.getNumResults() == 1 && nameQueryResult.getNumResults() == 0) {
                return pathQueryResult.first().getId();
            } else if (pathQueryResult.getNumResults() == 0 && nameQueryResult.getNumResults() == 1) {
                return nameQueryResult.first().getId();
            } else if (pathQueryResult.getNumResults() == 1 && nameQueryResult.getNumResults() == 1) {
                if (pathQueryResult.first().getId() == nameQueryResult.first().getId()) {
                    // The file was in the root folder, so it could be found based on the path and the name
                    return pathQueryResult.first().getId();
                } else {
                    throw new CatalogException("Error: More than one file id found based on " + fileName);
                }
            } else {
                // No results
                return -1L;
            }
        }

        if (pathQueryResult.getNumResults() == 1) {
            return pathQueryResult.first().getId();
        } else {
            return -1L;
        }
    }

    @Deprecated
    @Override
    public Long getFileId(String id) throws CatalogException {
        if (StringUtils.isNumeric(id)) {
            return Long.parseLong(id);
        }

        String[] split = id.split("@", 2);
        if (split.length != 2) {
            return -1L;
        }
        String[] projectStudyPath = split[1].replace(':', '/').split("/", 3);
        if (projectStudyPath.length <= 2) {
            return -2L;
        }
        long projectId = projectDBAdaptor.getProjectId(split[0], projectStudyPath[0]);
        long studyId = studyDBAdaptor.getStudyId(projectId, projectStudyPath[1]);
        return fileDBAdaptor.getFileId(studyId, projectStudyPath[2]);
    }

    /**
     * Returns if a file is externally located.
     * <p>
     * A file externally located is the one with a URI or a parent folder with an external URI.
     *
     * @throws CatalogException
     */
    @Override
    public boolean isExternal(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
//        return file.getUri() != null;
        return file.isExternal();
    }

    public boolean isRootFolder(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        return file.getPath().isEmpty();
    }

    @Override
    public QueryResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return getParents(true, options, read(fileId, new QueryOptions("include", "projects.studies.files.path"), sessionId).first()
                .getPath(), getStudyId(fileId));
    }

    /**
     * Return all parent folders from a file.
     *
     * @param file
     * @param options
     * @return
     * @throws CatalogException
     */
    private QueryResult<File> getParents(File file, boolean rootFirst, QueryOptions options) throws CatalogException {
        String filePath = file.getPath();
        return getParents(rootFirst, options, filePath, getStudyId(file.getId()));
    }

    private QueryResult<File> getParents(boolean rootFirst, QueryOptions options, String filePath, long studyId) throws CatalogException {
        List<String> paths = getParentPaths(filePath);

        Query query = new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), paths);
        query.put(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<File> result = fileDBAdaptor.get(query, options);
        result.getResult().sort(rootFirst ? ROOT_FIRST_COMPARATOR : ROOT_LAST_COMPARATOR);
        return result;
    }

    @Deprecated
    @Override
    public QueryResult<File> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Deprecated create method.");
    }

    @Override
    public QueryResult<File> createFolder(long studyId, String path, File.FileStatus status, boolean parents, String description,
                                          QueryOptions options, String sessionId) throws CatalogException {
        return create(studyId, File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE,
                path, null, null, description, status, 0, -1, null, -1, null, null,
                parents, options, sessionId);
    }

    @Override
    public QueryResult<File> create(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path, String ownerId,
                                    String creationDate, String description, File.FileStatus status, long diskUsage, long experimentId,
                                    List<Long> sampleIds, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                                    boolean parents, QueryOptions options, String sessionId) throws CatalogException {
        /** Check and set all the params and create a File object **/
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkPath(path, "filePath");

        type = ParamUtils.defaultObject(type, File.Type.FILE);
        format = ParamUtils.defaultObject(format, File.Format.PLAIN);  //TODO: Inference from the file name
        bioformat = ParamUtils.defaultObject(bioformat, File.Bioformat.NONE);
        ownerId = ParamUtils.defaultString(ownerId, userId);
        creationDate = ParamUtils.defaultString(creationDate, TimeUtils.getTime());
        description = ParamUtils.defaultString(description, "");
        if (type == File.Type.FILE) {
            status = (status == null) ? new File.FileStatus(File.FileStatus.STAGE) : status;
        } else {
            status = (status == null) ? new File.FileStatus(File.FileStatus.READY) : status;
        }
        if (diskUsage < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }
        if (experimentId > 0 && !jobDBAdaptor.experimentExists(experimentId)) {
            throw new CatalogException("Experiment { id: " + experimentId + "} does not exist.");
        }
        sampleIds = ParamUtils.defaultObject(sampleIds, LinkedList<Long>::new);

        for (Long sampleId : sampleIds) {
            if (!sampleDBAdaptor.sampleExists(sampleId)) {
                throw new CatalogException("Sample { id: " + sampleId + "} does not exist.");
            }
        }

        if (jobId > 0 && !jobDBAdaptor.jobExists(jobId)) {
            throw new CatalogException("Job { id: " + jobId + "} does not exist.");
        }

        stats = ParamUtils.defaultObject(stats, HashMap<String, Object>::new);
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (!Objects.equals(status.getName(), File.FileStatus.STAGE) && type == File.Type.FILE) {
//            if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
            throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with status != STAGE and INDEXING");
//            }
        }

        if (type == File.Type.DIRECTORY && !path.endsWith("/")) {
            path += "/";
        }
        if (type == File.Type.FILE && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        URI uri = Paths.get(getStudyUri(studyId)).resolve(path).toUri();

        //Create file object
//        File file = new File(-1, Paths.get(path).getFileName().toString(), type, format, bioformat,
//                path, ownerId, creationDate, description, status, diskUsage, experimentId, sampleIds, jobId,
//                new LinkedList<>(), stats, attributes);

        File file = new File(-1, Paths.get(path).getFileName().toString(), type, format, bioformat, uri, path, ownerId, TimeUtils.getTime(),
                TimeUtils.getTime(), description, status, false, diskUsage, experimentId, sampleIds, jobId, Collections.emptyList(),
                Collections.emptyList(), null, stats, attributes);

        //Find parent. If parents == true, create folders.
        Path parent = Paths.get(file.getPath()).getParent();
        String parentPath;
//        boolean isRoot = false;
        if (parent == null) {   //If parent == null, the file is in the root of the study
            parentPath = "";
//            isRoot = true;
        } else {
            parentPath = parent.toString() + "/";
        }

        long parentFileId = fileDBAdaptor.getFileId(studyId, parentPath);
        boolean newParent = false;
        if (parentFileId < 0 && parent != null) {
            if (parents) {
                newParent = true;
                parentFileId = create(studyId, File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, parent.toString(),
                        file.getOwnerId(), file.getCreationDate(), "", new File.FileStatus(File.FileStatus.READY), 0, -1,
                        Collections.<Long>emptyList(), -1, Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap(), true,
                        options, sessionId).first().getId();
            } else {
                throw new CatalogDBException("Directory not found " + parent.toString());
            }
        }

        //Check permissions
        if (parentFileId < 0) {
            throw new CatalogException("Unable to create file without a parent file");
        } else {
            if (!newParent) {
                //If parent has been created, for sure we have permissions to create the new file.
                authorizationManager.checkFilePermission(parentFileId, userId, FileAclEntry.FilePermissions.CREATE);
            }
        }


        //Check external file
//        boolean isExternal = isExternal(file);

        if (file.getType() == File.Type.DIRECTORY && Objects.equals(file.getStatus().getName(), File.FileStatus.READY)) {
//            URI fileUri = getFileUri(studyId, file.getPath());
            CatalogIOManager ioManager = catalogIOManagerFactory.get(uri);
            ioManager.createDirectory(uri, parents);
        }

        QueryResult<File> queryResult = fileDBAdaptor.createFile(studyId, file, options);
//        auditManager.recordCreation(AuditRecord.Resource.file, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.file, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<File> read(Long id, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        authorizationManager.checkFilePermission(id, userId, CatalogPermission.READ);
        authorizationManager.checkFilePermission(id, userId, FileAclEntry.FilePermissions.VIEW);

        QueryResult<File> fileQueryResult = fileDBAdaptor.getFile(id, options);
        authorizationManager.filterFiles(userId, getStudyId(id), fileQueryResult.getResult());
        fileQueryResult.setNumResults(fileQueryResult.getResult().size());
        return fileQueryResult;
    }

    @Override
    public QueryResult<File> getParent(long fileId, QueryOptions options, String sessionId)
            throws CatalogException {

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        File file = read(fileId, null, sessionId).first();
        Path parent = Paths.get(file.getPath()).getParent();
        String parentPath;
        if (parent == null) {
            parentPath = "";
        } else {
            parentPath = parent.toString().endsWith("/") ? parent.toString() : parent.toString() + "/";
        }
        return read(fileDBAdaptor.getFileId(studyId, parentPath), options, sessionId);
    }

    @Override
    public QueryResult<File> readAll(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        return readAll(query.getInt("studyId", -1), query, options, sessionId);
    }

    @Override
    public QueryResult<File> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (studyId <= 0) {
            throw new CatalogDBException("Permission denied. Only the files of one study can be seen at a time.");
        } else {
            if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
                throw CatalogAuthorizationException.deny(userId, "view", "files", studyId, null);
            }
            query.put(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        }

        QueryResult<File> queryResult = fileDBAdaptor.get(query, options);
        authorizationManager.filterFiles(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
    }

    @Override
    public QueryResult<File> update(Long fileId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        File file = read(fileId, null, sessionId).first();

        if (isRootFolder(file)) {
            throw new CatalogException("Can not modify root folder");
        }

        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.UPDATE);
        for (String s : parameters.keySet()) {
            switch (s) { //Special cases
                //Can be modified anytime
                case "format":
                case "bioformat":
                case "description":
                case "status.name":
                case "attributes":
                case "stats":
                case "index":
                case "sampleIds":
                case "jobId":
                    break;
                case "uri":
                    logger.info("File {id: " + fileId + "} uri modified. New value: " + parameters.get("uri"));
                    break;
                //Can only be modified when file.status == STAGE
                case "creationDate":
                case "modificationDate":
                case "diskUsage":
//                            if (!file.getName().equals(File.Status.STAGE)) {
//                                throw new CatalogException("Parameter '" + s + "' can't be changed when " +
//                                        "status == " + file.getName().name() + ". " +
//                                        "Required status STAGE or admin account");
//                            }
                    break;
                //Path and Name must be changed with "raname" and/or "move" methods.
                case "path":
                case "name":
                    break;
                case "type":
                default:
                    throw new CatalogException("Parameter '" + s + "' can't be changed. "
                            + "Requires admin account");
            }
        }

        //Path and Name must be changed with "raname" and/or "move" methods.
        if (parameters.containsKey("name")) {
            logger.info("Rename file using update method!");
            rename(fileId, parameters.getString("name"), sessionId);
        }
        if (parameters.containsKey("path")) {
            logger.info("Move file using update method!");
            move(fileId, parameters.getString("path"), options, sessionId);
        }

        String ownerId = fileDBAdaptor.getFileOwnerId(fileId);
        QueryResult queryResult = fileDBAdaptor.update(fileId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
        userDBAdaptor.updateUserLastModified(ownerId);
        return queryResult;
    }

//    @Override
//    public QueryResult<File> delete(Long id, QueryOptions options, String sessionId) throws CatalogException {
//        return deleteOld(id, options, sessionId);
//    }

    @Deprecated
    @Override
    public QueryResult<File> delete(Long fileId, QueryOptions options, String sessionId)
            throws CatalogException {        //Safe delete: Don't delete. Just rename file and set {deleting:true}
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DELETE);

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        long projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = projectDBAdaptor.getProjectOwnerId(projectId);

        File file = fileDBAdaptor.getFile(fileId, null).first();

        if (isRootFolder(file)) {
            throw new CatalogException("Can not delete root folder");
        }

        QueryResult<File> result = checkCanDeleteFile(file, userId);
        if (result != null) {
            return result;
        }

        userDBAdaptor.updateUserLastModified(ownerId);

        ObjectMap objectMap = new ObjectMap();
        objectMap.put(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
        objectMap.put(CatalogFileDBAdaptor.QueryParams.STATUS_DATE.key(), System.currentTimeMillis());

        switch (file.getType()) {
            case DIRECTORY: {
                QueryResult<File> allFilesInFolder = fileDBAdaptor.getAllFilesInFolder(studyId, file.getPath(), null);
                // delete recursively. Walk tree depth first
                for (File subfolder : allFilesInFolder.getResult()) {
                    if (subfolder.getType() == File.Type.DIRECTORY) {
                        delete(subfolder.getId(), null, sessionId);
                    }
                }
                //Check can delete files
                for (File subfile : allFilesInFolder.getResult()) {
                    if (subfile.getType() == File.Type.FILE) {
                        checkCanDeleteFile(subfile, userId);
                    }
                }
                for (File subfile : allFilesInFolder.getResult()) {
                    if (subfile.getType() == File.Type.FILE) {
                        delete(subfile.getId(), null, sessionId);
                    }
                }

                QueryResult<File> queryResult = fileDBAdaptor.update(fileId, objectMap);
//                QueryResult<File> queryResult = rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, objectMap, null, null);
                return queryResult; //TODO: Return the modified file
            }
            case FILE: {
//                rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
                QueryResult<File> queryResult = fileDBAdaptor.update(fileId, objectMap);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, objectMap, null, null);
                return queryResult; //TODO: Return the modified file
            }
            default:
                break;
        }
        return null;
    }

    @Override
    public QueryResult<File> delete(String fileIdStr, QueryOptions options, String sessionId) throws CatalogException, IOException {
        /*
         * This method checks:
         * 1. fileIdStr converts easily to a valid fileId.
         * 2. The user belonging to the sessionId has permissions to delete files.
         * 3. If file is external and DELETE_EXTERNAL_FILE is false, we will call unlink method.
         * 4. Check if the status of the file/folder and the children is a valid one.
         * 5. Root folders cannot be deleted.
         * 6. No external files or folders are found within the path.
         */

        QueryResult<File> deletedFileResult = new QueryResult<>("Delete file", -1, 0, 0, "", "No changes made", Collections.emptyList());

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        // FIXME use userManager instead of userDBAdaptor
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        // Check 1. No comma-separated values are valid, only one single File or Directory can be deleted.
        Long fileId = getFileId(userId, fileIdStr);
        fileDBAdaptor.checkFileId(fileId);

        // Check 2. User has the proper permissions to delete the file.
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DELETE);

        // Check if we can obtain the file from the dbAdaptor properly.
        QueryResult<File> fileQueryResult = fileDBAdaptor.getFile(fileId, options);
        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
            throw new CatalogException("Cannot delete file '" + fileIdStr + "'. There was an error with the database.");
        }
        File file = fileQueryResult.first();

        // Check 3.
        // If file is not externally linked or if it is external but with DELETE_EXTERNAL_FILES set to true then can be deleted.
        // This prevents external linked files to be accidentally deleted.
        // If file is linked externally and DELETE_EXTERNAL_FILES is false then we just unlink the file.
        if (file.isExternal() && !options.getBoolean(DELETE_EXTERNAL_FILES, false)) {
            return unlink(fileIdStr, options, sessionId);
        }

        // Check 4.
        // We cannot delete the root folder
        if (file.getType().equals(File.Type.DIRECTORY) && isRootFolder(file)) {
            throw new CatalogException("Root directories cannot be deleted");
        }

        // Check 5.
        // Only READY, TRASHED and PENDING_DELETE files can be deleted
        String fileStatus = file.getStatus().getName();
        // TODO chang this to accept only valid statuses
        if (fileStatus.equalsIgnoreCase(File.FileStatus.STAGE) || fileStatus.equalsIgnoreCase(File.FileStatus.MISSING)
                || fileStatus.equalsIgnoreCase(File.FileStatus.DELETING) || fileStatus.equalsIgnoreCase(File.FileStatus.DELETED)
                || fileStatus.equalsIgnoreCase(File.FileStatus.REMOVED)) {
            throw new CatalogException("File cannot be deleted, status is: " + fileStatus);
        }

        // Check 6.
        // We cannot delete a folder containing files or folders with status missing or staged
        long studyId = -1;
        if (file.getType().equals(File.Type.DIRECTORY)) {
            studyId = fileDBAdaptor.getStudyIdByFileId(fileId);

            Query query = new Query()
                    .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(),
                            Arrays.asList(File.FileStatus.MISSING, File.FileStatus.STAGE));

            long count = fileDBAdaptor.count(query).first();
            if (count > 0) {
                throw new CatalogException("Cannot delete folder. " + count + " files have been found with status missing or staged.");
            }
        }

        // Check 7.
        // We cannot delete a folder containing any linked file/folder, these must be unlinked first
        if (file.getType().equals(File.Type.DIRECTORY)) {
            if (studyId == -1) {
                studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
            }

            Query query = new Query()
                    .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY)
                    .append(CatalogFileDBAdaptor.QueryParams.EXTERNAL.key(), true);

            long count = fileDBAdaptor.count(query).first();
            if (count > 0) {
                throw new CatalogException("Cannot delete folder. " + count + " linked files have been found within the path. Please, "
                        + "unlink them first.");
            }
        }

        if (options.getBoolean(SKIP_TRASH, false)) {
            deletedFileResult = deleteFromDisk(file, userId, options);
        } else {
            if (fileStatus.equalsIgnoreCase(File.FileStatus.READY)) {

                if (file.getType().equals(File.Type.FILE)) {
                    deletedFileResult = fileDBAdaptor.delete(fileId, new QueryOptions());
                } else {
                    if (studyId == -1) {
                        studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
                    }

                    // Send to trash all the files and subfolders
                    Query query = new Query()
                            .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                            .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~" + file.getPath() + "*")
                            .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
                    QueryResult<File> allFiles = fileDBAdaptor.get(query,
                            new QueryOptions(QueryOptions.INCLUDE, CatalogFileDBAdaptor.QueryParams.ID.key()));

                    if (allFiles != null && allFiles.getNumResults() > 0) {
                        for (File fileToDelete : allFiles.getResult()) {
                            fileDBAdaptor.delete(fileToDelete.getId(), new QueryOptions());
                        }
                    }

                    deletedFileResult = fileDBAdaptor.getFile(fileId, new QueryOptions());
                }
            }
        }

        return deletedFileResult;
    }

    private QueryResult<File> deleteFromDisk(File fileOrDirectory, String userId, QueryOptions options)
            throws CatalogException, IOException {
        QueryResult<File> removedFileResult;

        // Check permissions for the current file
        authorizationManager.checkFilePermission(fileOrDirectory.getId(), userId, FileAclEntry.FilePermissions.DELETE);

        // Not external file
        URI fileUri = getFileUri(fileOrDirectory);
        Path filesystemPath = Paths.get(fileUri);
//        FileUtils.checkFile(filesystemPath);
        CatalogIOManager ioManager = catalogIOManagerFactory.get(fileUri);

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileOrDirectory.getId());
        String suffixName = ".DELETED_" + TimeUtils.getTime();

        // If file is not a directory then we can just delete it from disk and update Catalog.
        if (fileOrDirectory.getType().equals(File.Type.FILE)) {
            // 1. Set the file status to deleting
            ObjectMap update = new ObjectMap()
                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING);
            fileDBAdaptor.delete(fileOrDirectory.getId(), update, options);

            // 2. Delete the file from disk
            ioManager.deleteFile(fileUri);

            // 3. Update the file status in the database. Set to delete
            update = new ObjectMap()
                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED)
                    .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), fileOrDirectory.getPath() + suffixName);
            removedFileResult = fileDBAdaptor.delete(fileOrDirectory.getId(), update, options);
        } else {
            // Directories can be marked to be deferred removed by setting FORCE_DELETE to false, then File daemon will remove it.
            // In this mode directory is just renamed and URIs and Paths updated in Catalog. By default removal is deferred.
            if (!options.getBoolean(FORCE_DELETE, false)
                    && !fileOrDirectory.getStatus().getName().equals(File.FileStatus.PENDING_DELETE)) {
                // Rename the directory in the filesystem.
                URI newURI = Paths.get(Paths.get(fileUri).toString() + suffixName).toUri();

                String basePath = Paths.get(fileOrDirectory.getPath()).toString();
                String suffixedPath = basePath + suffixName;

                // Get all the files that starts with path
                logger.debug("Looking for files and folders inside {}", fileOrDirectory.getPath());
                Query query = new Query()
                        .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~^" + fileOrDirectory.getPath() + "*")
                        .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), fileOrDirectory.getStatus().getName());
                QueryResult<File> queryResult = fileDBAdaptor.get(query, new QueryOptions());

                if (queryResult != null && queryResult.getNumResults() > 0) {
                    logger.debug("Renaming {} to {}", fileUri.toString(), newURI.toString());
                    ioManager.rename(fileUri, newURI);

                    logger.debug("Changing the URI in catalog to {} and setting the status to {}", newURI.toString(),
                            File.FileStatus.PENDING_DELETE);

                    // We update the uri and status of all the files and folders so it can be later deleted by the daemon
                    for (File file : queryResult.getResult()) {

                        String newUri = file.getUri().toString().replace(fileUri.toString(), newURI.toString());
                        String newPath = file.getPath().replace(basePath, suffixedPath);

                        System.out.println("newPath = " + newPath);

                        logger.debug("Replacing old uri {} per {} and setting the status to {}", file.getUri().toString(),
                                newUri, File.FileStatus.PENDING_DELETE);

                        ObjectMap update = new ObjectMap()
                                .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE)
                                .append(CatalogFileDBAdaptor.QueryParams.URI.key(), newUri)
                                .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), newPath);
                        fileDBAdaptor.delete(file.getId(), update, new QueryOptions());
                    }
                } else {
                    // The uri in the disk has been changed but not in the database !!
                    throw new CatalogException("ERROR: Could not retrieve all the files and folders hanging from " + fileUri.toString());
                }

            } else if (options.getBoolean(FORCE_DELETE, false)) {
                // Physically delete all the files hanging from the folder
                Files.walkFileTree(filesystemPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                        try {
                            // Look for the file in catalog
                            Query query = new Query()
                                    .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                    .append(CatalogFileDBAdaptor.QueryParams.URI.key(), path.toUri().toString());

                            QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                            if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                                logger.debug("Cannot delete " + path.toString() + ". The file could not be found in catalog.");
                                return FileVisitResult.CONTINUE;
                            }

                            if (fileQueryResult.getNumResults() > 1) {
                                logger.error("Internal error: More than one file was found in catalog for uri " + path.toString());
                                return FileVisitResult.CONTINUE;
                            }

                            File file = fileQueryResult.first();

                            // 1. Set the file status to deleting
                            ObjectMap update = new ObjectMap()
                                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING);
                            fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                            logger.debug("Deleting file '" + path.toString() + "' from filesystem and Catalog");

                            // 2. Delete the file from disk
                            ioManager.deleteFile(path.toUri());

                            // 3. Update the file status and path in the database. Set to delete
                            update = new ObjectMap()
                                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);

                            QueryResult<File> deleteQueryResult = fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                            if (deleteQueryResult == null || deleteQueryResult.getNumResults() != 1) {
                                // The file could not be removed from catalog. This should ONLY be happening when the file that
                                // has been removed from the filesystem was not registered in catalog.
                                logger.error("Internal error: The file {} could not be deleted from the database." + path.toString());
                            }

                            logger.debug("DELETE: {} successfully removed from the filesystem and catalog", path.toString());
                        } catch (CatalogDBException | CatalogIOException e) {
                            logger.error(e.getMessage());
                            e.printStackTrace();
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException io) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        if (exc == null) {
                            // Only empty folders can be deleted for safety reasons
                            if (dir.toFile().list().length == 0) {
                                try {
                                    String folderUri = dir.toUri().toString();
                                    if (folderUri.endsWith("/")) {
                                        folderUri = folderUri.substring(0, folderUri.length() - 1);
                                    }
                                    Query query = new Query()
                                            .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                            .append(CatalogFileDBAdaptor.QueryParams.URI.key(), folderUri);

                                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                                    if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                                        logger.debug("Cannot remove " + dir.toString() + ". The directory could not be found in catalog.");
                                        return FileVisitResult.CONTINUE;
                                    }

                                    if (fileQueryResult.getNumResults() > 1) {
                                        logger.error("Internal error: More than one file was found in catalog for uri " + dir.toString());
                                        return FileVisitResult.CONTINUE;
                                    }

                                    File file = fileQueryResult.first();

                                    logger.debug("Removing empty directory '" + dir.toString() + "' from filesystem and catalog");

                                    ioManager.deleteDirectory(dir.toUri());

                                    ObjectMap update = new ObjectMap()
                                            .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);
//                                            .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), "");

                                    QueryResult<File> deleteQueryResult = fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                                    if (deleteQueryResult == null || deleteQueryResult.getNumResults() != 1) {
                                        // The file could not be removed from catalog. This should ONLY be happening when the file that
                                        // has been removed from the filesystem was not registered in catalog.
                                        logger.error("Internal error: The file {} could not be deleted from the database."
                                                + dir.toString());
                                    }

                                    logger.debug("REMOVE: {} successfully removed from the filesystem and catalog", dir.toString());
                                } catch (CatalogDBException e) {
                                    logger.error(e.getMessage());
                                    e.printStackTrace();
                                } catch (CatalogIOException e) {
                                    logger.error(e.getMessage());
                                    e.printStackTrace();
                                }
                            } else {
                                logger.warn("REMOVE: {} Could not remove the directory as it is not empty.", dir.toString());
                            }
                            return FileVisitResult.CONTINUE;
                        } else {
                            // directory iteration failed
                            throw exc;
                        }
                    }
                });
            }
            removedFileResult = fileDBAdaptor.getFile(fileOrDirectory.getId(), new QueryOptions());
        }
        return removedFileResult;
    }

    /**
     * Create the parent directories that are needed.
     *
     * @param studyId study id where they will be created.
     * @param userId user that is creating the parents.
     * @param studyURI Base URI where the created folders will be pointing to. (base physical location)
     * @param path Path used in catalog as a virtual location. (whole bunch of directories inside the virtual location in catalog)
     * @param checkPermissions Boolean indicating whether to check if the user has permissions to create a folder in the first directory
     *                         that is available in catalog.
     * @throws CatalogDBException
     */
    private void createParents(long studyId, String userId, URI studyURI, Path path, boolean checkPermissions) throws CatalogException {
        if (path == null) {
            if (checkPermissions) {
                authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_FILES);
            }
            return;
        }

        String stringPath = path.toString();
        logger.info("Path: {}", stringPath);

        if (stringPath.startsWith("/")) {
            stringPath = stringPath.substring(1);
        }

        if (!stringPath.endsWith("/")) {
            stringPath = stringPath + "/";
        }

        // Check if the folder exists
        Query query = new Query()
                .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), stringPath);

        if (fileDBAdaptor.count(query).first() == 0) {
            createParents(studyId, userId, studyURI, path.getParent(), checkPermissions);
        } else {
            if (checkPermissions) {
                long fileId = fileDBAdaptor.getFileId(studyId, stringPath);
                authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.CREATE);
            }
            return;
        }

        URI completeURI = Paths.get(studyURI).resolve(path).toUri();

        // Create the folder in catalog
        File folder = new File(-1, path.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, completeURI,
                stringPath, userId, TimeUtils.getTime(), TimeUtils.getTime(), "", new File.FileStatus(File.FileStatus.READY),
                false, 0, -1, Collections.emptyList(), -1, Collections.emptyList(), Collections.emptyList(), null, null, null);
        fileDBAdaptor.createFile(studyId, folder, new QueryOptions());
    }

    public QueryResult<File> link(URI uriOrigin, String pathDestiny, long studyId, ObjectMap params, String sessionId)
            throws CatalogException, IOException {

        CatalogIOManager ioManager = catalogIOManagerFactory.get(uriOrigin);
        if (!ioManager.exists(uriOrigin)) {
            throw new CatalogIOException("File " + uriOrigin + " does not exist");
        }

        studyDBAdaptor.checkStudyId(studyId);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        boolean parents = params.getBoolean("parents", false);
        // FIXME: Implement resync
        boolean resync  = params.getBoolean("resync", false);
        String description = params.getString("description", "");

        // Because pathDestiny can be null, we will use catalogPath as the virtual destiny where the files will be located in catalog.
        Path catalogPath;

        if (pathDestiny == null || pathDestiny.isEmpty()) {
            // If no destiny is given, everything will be linked to the root folder of the study.
            // FIXME use / as a empty
            catalogPath = Paths.get("");
            authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_FILES);
        } else {
            catalogPath = Paths.get(pathDestiny);

            // Check if the destiny is a directory
            if (catalogPath.toFile().isFile()) {
                throw new CatalogException("Error: The destiny catalog path must be a directory.");
            }

            // Check if the folder exists
            Query query = new Query()
                    .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), catalogPath.toString() + "/");
            if (fileDBAdaptor.count(query).first() == 0) {
                if (parents) {
                    // Get the base URI where the files are located in the study
                    URI studyURI = getStudyUri(studyId);

                    // Create the directories that are necessary in catalog except for the main folder that will be linked
                    createParents(studyId, userId, studyURI, catalogPath.getParent(), true);

                    // Create them in the disk
                    if (catalogPath.getParent() != null) {
                        URI directory = Paths.get(studyURI).resolve(catalogPath.getParent()).toUri();
                        catalogIOManagerFactory.get(directory).createDirectory(directory, true);
                    }

                    // Create the folder with external
                    File folder = new File(-1, catalogPath.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN,
                            File.Bioformat.NONE, uriOrigin, catalogPath.toString() + "/", userId, TimeUtils.getTime(), TimeUtils.getTime(),
                            description, new File.FileStatus(File.FileStatus.READY), true, 0, -1,
                            Collections.emptyList(), -1, Collections.emptyList(), Collections.emptyList(), null, Collections.emptyMap(),
                            Collections.emptyMap());
                    fileDBAdaptor.createFile(studyId, folder, new QueryOptions());

                } else {
                    throw new CatalogException("The path " + catalogPath + " does not exist in catalog.");
                }
            } else {
                // Check if the user has permissions to link files in the directory
                long fileId = fileDBAdaptor.getFileId(studyId, catalogPath.toString() + "/");
                authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.CREATE);
            }
        }

        Path pathOrigin = Paths.get(uriOrigin);
        if (Paths.get(uriOrigin).toFile().isFile()) {
            Path filePath = catalogPath.resolve(pathOrigin.getFileName());

            // Check if there is already a file in the same path
            Query query = new Query()
                    .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), filePath.toString());

            // Create the file
            if (fileDBAdaptor.count(query).first() == 0) {
                long diskUsage = Files.size(Paths.get(uriOrigin));
                File.Bioformat bioformat = BioformatDetector.detect(uriOrigin);
                File.Format format = FormatDetector.detect(uriOrigin);

                File subfile = new File(-1, filePath.getFileName().toString(), File.Type.FILE, format, bioformat, uriOrigin,
                        filePath.toString(), userId, TimeUtils.getTime(), TimeUtils.getTime(), description,
                        new File.FileStatus(File.FileStatus.READY), true, diskUsage, -1, Collections.emptyList(), -1,
                        Collections.emptyList(), Collections.emptyList(), null, Collections.emptyMap(), Collections.emptyMap());
                return fileDBAdaptor.createFile(studyId, subfile, new QueryOptions());
            } else {
                throw new CatalogException("Cannot link " + filePath.getFileName().toString() + ". A file with the same name was found"
                        + " in the same path.");
            }
        } else {
            // Link all the files and folders present in the uri
            Files.walkFileTree(pathOrigin, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

                    try {
                        String destinyPath = dir.toString().replace(Paths.get(uriOrigin).toString(), catalogPath.toString());

                        if (!destinyPath.isEmpty()) {
                            destinyPath += "/";
                        }

                        Query query = new Query()
                                .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                        if (fileDBAdaptor.count(query).first() == 0) {
                            // If the folder does not exist, we create it
                            File folder = new File(-1, dir.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN,
                                    File.Bioformat.NONE, dir.toUri(), destinyPath, userId, TimeUtils.getTime(), TimeUtils.getTime(),
                                    description, new File.FileStatus(File.FileStatus.READY), true, 0, -1,
                                    Collections.emptyList(), -1, Collections.emptyList(), Collections.emptyList(), null,
                                    Collections.emptyMap(), Collections.emptyMap());
                            fileDBAdaptor.createFile(studyId, folder, new QueryOptions());
                        }

                    } catch (CatalogDBException e) {
                        logger.error("An error occurred when trying to create folder {}", dir.toString());
//                        e.printStackTrace();
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) throws IOException {
                    try {
                        String destinyPath = filePath.toString().replace(Paths.get(uriOrigin).toString(), catalogPath.toString());

                        Query query = new Query()
                                .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                        if (fileDBAdaptor.count(query).first() == 0) {
                            long diskUsage = Files.size(filePath);
                            File.Bioformat bioformat = BioformatDetector.detect(filePath.toUri());
                            File.Format format = FormatDetector.detect(filePath.toUri());
                            // If the file does not exist, we create it
                            File subfile = new File(-1, filePath.getFileName().toString(), File.Type.FILE, format, bioformat,
                                    filePath.toUri(), destinyPath, userId, TimeUtils.getTime(), TimeUtils.getTime(), description,
                                    new File.FileStatus(File.FileStatus.READY), true, diskUsage, -1, Collections.emptyList(), -1,
                                    Collections.emptyList(), Collections.emptyList(), null, Collections.emptyMap(), Collections.emptyMap());
                            fileDBAdaptor.createFile(studyId, subfile, new QueryOptions());

                        } else {
                            throw new CatalogException("Cannot link the file " + filePath.getFileName().toString()
                                    + ". There is already a file in the path " + destinyPath + " with the same name.");
                        }

                    } catch (CatalogException e) {
                        logger.error(e.getMessage());
//                        e.printStackTrace();
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.SKIP_SUBTREE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    // We update the diskUsage of the folder
                    // TODO: Check this. Maybe we should not be doing this here.
//                    String destinyPath = dir.toString().replace(Paths.get(uriOrigin).toString(), catalogPath.toString());
//
//                    if (destinyPath.startsWith("/")) {
//                        destinyPath = destinyPath.substring(1, destinyPath.length());
//                    }
//
//                    if (!destinyPath.isEmpty()) {
//                        destinyPath += "/";
//                    }
//
//                    long diskUsage = Files.size(Paths.get(destinyPath));
//
//                    Query query = new Query()
//                            .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
//                            .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), destinyPath);
//
//                    ObjectMap objectMap = new ObjectMap(CatalogFileDBAdaptor.QueryParams.DISK_USAGE.key(), diskUsage);
//
//                    try {
//                        fileDBAdaptor.update(query, objectMap);
//                    } catch (CatalogDBException e) {
//                        logger.error("Link: There was an error when trying to update the diskUsage of the folder");
//                        e.printStackTrace();
//                    }

                    return FileVisitResult.CONTINUE;
                }
            });

            Query query = new Query()
                    .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), catalogPath.toString());

            return fileDBAdaptor.get(query, new QueryOptions());
        }

    }

    public QueryResult<File> unlink(String fileIdStr, QueryOptions options, String sessionId) throws CatalogException, IOException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        // FIXME use userManager instead of userDBAdaptor
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        // Check 1. No comma-separated values are valid, only one single File or Directory can be deleted.
        long fileId = getFileId(userId, fileIdStr);
        fileDBAdaptor.checkFileId(fileId);
        long studyId = getStudyId(fileId);

        // Check 2. User has the proper permissions to delete the file.
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DELETE);

        // Check if we can obtain the file from the dbAdaptor properly.
        QueryResult<File> fileQueryResult = fileDBAdaptor.getFile(fileId, options);
        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
            throw new CatalogException("Cannot delete file '" + fileIdStr + "'. There was an error with the database.");
        }

        File file = fileQueryResult.first();

        // Check 3.
        if (!file.isExternal()) {
            throw new CatalogException("Only previously linked files can be unlinked. Please, use delete instead.");
        }

        if (file.getType().equals(File.Type.FILE)) {
            logger.debug("Unlinking file {}", file.getUri().toString());

            ObjectMap update = new ObjectMap(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);

            return fileDBAdaptor.delete(file.getId(), update, new QueryOptions());
        } else {
            logger.debug("Unlinking folder {}", file.getUri().toString());
            String suffixName = ".REMOVED_" + TimeUtils.getTime();
            String basePath = Paths.get(file.getPath()).toString();
            String suffixedPath = basePath + suffixName;


            Files.walkFileTree(Paths.get(file.getUri()), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    try {
                        // Look for the file in catalog
                        Query query = new Query()
                                .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(CatalogFileDBAdaptor.QueryParams.URI.key(), path.toUri().toString())
                                .append(CatalogFileDBAdaptor.QueryParams.EXTERNAL.key(), true)
                                .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);

                        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                        if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                            logger.debug("Cannot unlink " + path.toString() + ". The file could not be found in catalog.");
                            return FileVisitResult.CONTINUE;
                        }

                        if (fileQueryResult.getNumResults() > 1) {
                            logger.error("Internal error: More than one file was found in catalog for uri " + path.toString());
                            return FileVisitResult.CONTINUE;
                        }

                        File file = fileQueryResult.first();

                        ObjectMap update = new ObjectMap()
                                .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                                .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), file.getPath().replaceFirst(basePath, suffixedPath));

                        fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                        logger.debug("{} unlinked", file.toString());
                    } catch (CatalogDBException e) {
                        e.printStackTrace();
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.SKIP_SUBTREE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (exc == null) {
                            try {
                                // Only empty folders can be deleted for safety reasons
                                Query query = new Query()
                                        .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                        .append(CatalogFileDBAdaptor.QueryParams.URI.key(), "~^" + dir.toUri().toString() + "/*")
                                        .append(CatalogFileDBAdaptor.QueryParams.EXTERNAL.key(), true)
                                        .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);

                                QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                                if (fileQueryResult == null || fileQueryResult.getNumResults() > 1) {
                                    // The only result should be the current directory
                                    logger.debug("Cannot unlink " + dir.toString()
                                            + ". There are files/folders inside the folder that have not been unlinked.");
                                    return FileVisitResult.CONTINUE;
                                }

                                // Look for the folder in catalog
                                query = new Query()
                                        .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                        .append(CatalogFileDBAdaptor.QueryParams.URI.key(), dir.toUri().toString())
                                        .append(CatalogFileDBAdaptor.QueryParams.EXTERNAL.key(), true)
                                        .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);

                                fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                                if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                                    logger.debug("Cannot unlink " + dir.toString() + ". The directory could not be found in catalog.");
                                    return FileVisitResult.CONTINUE;
                                }

                                if (fileQueryResult.getNumResults() > 1) {
                                    logger.error("Internal error: More than one file was found in catalog for uri " + dir.toString());
                                    return FileVisitResult.CONTINUE;
                                }

                                File file = fileQueryResult.first();

                                ObjectMap update = new ObjectMap()
                                        .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                                        .append(CatalogFileDBAdaptor.QueryParams.PATH.key(),
                                                file.getPath().replaceFirst(basePath, suffixedPath));

                                fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                                logger.debug("{} unlinked", dir.toString());
                            } catch (CatalogDBException e) {
                                e.printStackTrace();
                            }

                        return FileVisitResult.CONTINUE;
                    } else {
                        // directory iteration failed
                        throw exc;
                    }
                }
            });

            Query query = new Query()
                    .append(CatalogFileDBAdaptor.QueryParams.ID.key(), file.getId())
                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);
            return fileDBAdaptor.get(query, new QueryOptions());
        }
    }

    @Deprecated
    public QueryResult<File> unlink(long fileId, String sessionId) throws CatalogException {
        return null;
//        fileDBAdaptor.checkFileId(fileId);
//
//        QueryResult<File> fileQueryResult = fileDBAdaptor.getFile(fileId, new QueryOptions());
//
//        if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
//            throw new CatalogException("Internal error: Cannot find " + fileId);
//        }
//
//        File file = fileQueryResult.first();
//
//        if (isRootFolder(file)) {
//            throw new CatalogException("Can not delete root folder");
//        }
//
//        if (!file.isExternal()) {
//            throw new CatalogException("Cannot unlink a file that has not been linked.");
//        }
//
//        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        authorizationManager.checkFilePermission(fileId, userId, FileAcl.FilePermissions.DELETE);
//
//        List<File> filesToDelete;
//        if (file.getType().equals(File.Type.DIRECTORY)) {
//            filesToDelete = fileDBAdaptor.get(
//                    new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath()),
//                    new QueryOptions("include", "projects.studies.files.id")).getResult();
//        } else {
//            filesToDelete = Collections.singletonList(file);
//        }
//
//        for (File f : filesToDelete) {
//            fileDBAdaptor.delete(f.getId(), new QueryOptions());
//        }
//
//        return queryResult;
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Deprecated
    private QueryResult<File> checkCanDeleteFile(File file, String userId) throws CatalogException {
        authorizationManager.checkFilePermission(file.getId(), userId, FileAclEntry.FilePermissions.DELETE);

        switch (file.getStatus().getName()) {
            case File.FileStatus.TRASHED:
                //Send warning message
                String warningMsg = "File already deleted. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}";
                logger.warn(warningMsg);
                return new QueryResult<File>("Delete file", 0, 0, 0,
                        warningMsg,
                        null, Collections.emptyList());
            case File.FileStatus.READY:
                break;
            case File.FileStatus.STAGE:
            case File.FileStatus.MISSING:
            default:
                throw new CatalogException("File is not ready. {"
                        + "id: " + file.getId() + ", "
                        + "path:\"" + file.getPath() + "\","
                        + "status: '" + file.getStatus().getName() + "'}");
        }
        return null;
    }

    @Override
    public QueryResult<File> rename(long fileId, String newName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkFileName(newName, "name");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        long projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = projectDBAdaptor.getProjectOwnerId(projectId);

        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.UPDATE);
        QueryResult<File> fileResult = fileDBAdaptor.getFile(fileId, null);
        File file = fileResult.first();

        if (file.getName().equals(newName)) {
            fileResult.setId("rename");
            fileResult.setWarningMsg("File name '" + newName + "' is the original name. Do nothing.");
            return fileResult;
        }

        if (isRootFolder(file)) {
            throw new CatalogException("Can not rename root folder");
        }

        String oldPath = file.getPath();
        Path parent = Paths.get(oldPath).getParent();
        String newPath;
        if (parent == null) {
            newPath = newName;
        } else {
            newPath = parent.resolve(newName).toString();
        }

        userDBAdaptor.updateUserLastModified(ownerId);
        CatalogIOManager catalogIOManager;
        URI oldUri = file.getUri();
        URI newUri = Paths.get(oldUri).getParent().resolve(newName).toUri();
//        URI studyUri = file.getUri();
        boolean isExternal = isExternal(file); //If the file URI is not null, the file is external located.
        QueryResult<File> result;
        switch (file.getType()) {
            case DIRECTORY:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(oldUri); // TODO? check if something in the subtree is not READY?
                    catalogIOManager.rename(oldUri, newUri);   // io.move() 1
                }
                result = fileDBAdaptor.renameFile(fileId, newPath, newUri.toString(), null);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, new ObjectMap("path", newPath)
                        .append("name", newName), "rename", null);
                break;
            case FILE:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(oldUri);
                    catalogIOManager.rename(oldUri, newUri);
                }
                result = fileDBAdaptor.renameFile(fileId, newPath, newUri.toString(), null);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, new ObjectMap("path", newPath)
                        .append("name", newName), "rename", null);
                break;
            default:
                throw new CatalogException("Unknown file type " + file.getType());
        }

        return result;
    }

    @Deprecated
    @Override
    public QueryResult move(long fileId, String newPath, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
        //TODO https://github.com/opencb/opencga/issues/136
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
//        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
//        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);
//
//        if (!authorizationManager.getFileACL(userId, fileId).isWrite()) {
//            throw new CatalogManagerException("Permission denied. User can't rename this file");
//        }
//        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
//        if (fileResult.getResult().isEmpty()) {
//            return new QueryResult("Delete file", 0, 0, 0, "File not found", null, null);
//        }
//        File file = fileResult.getResult().get(0);
    }

    @Override
    public QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(files, "files");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_DATASETS);

        for (Long fileId : files) {
            if (fileDBAdaptor.getStudyIdByFileId(fileId) != studyId) {
                throw new CatalogException("Can't create a dataset with files from different studies.");
            }
            authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.VIEW);
        }

        Dataset dataset = new Dataset(-1, name, TimeUtils.getTime(), description, files, new Status(), attributes);
        QueryResult<Dataset> queryResult = datasetDBAdaptor.createDataset(studyId, dataset, options);
//        auditManager.recordCreation(AuditRecord.Resource.dataset, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.dataset, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Dataset> readDataset(long dataSetId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        QueryResult<Dataset> queryResult = datasetDBAdaptor.getDataset(dataSetId, options);

        for (Long fileId : queryResult.first().getFiles()) {
            authorizationManager.checkDatasetPermission(fileId, userId, DatasetAclEntry.DatasetPermissions.VIEW);
        }

        return queryResult;
    }

    @Override
    public Long getStudyIdByDataset(long datasetId) throws CatalogException {
        return datasetDBAdaptor.getStudyIdByDatasetId(datasetId);
    }

    @Override
    public Long getDatasetId(String userId, String datasetStr) throws CatalogException {
        if (StringUtils.isNumeric(datasetStr)) {
            return Long.parseLong(datasetStr);
        }

        // Resolve the studyIds and filter the datasetName
        ObjectMap parsedSampleStr = parseFeatureId(userId, datasetStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String datasetName = parsedSampleStr.getString("featureName");

        Query query = new Query(CatalogDatasetDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(CatalogDatasetDBAdaptor.QueryParams.NAME.key(), datasetName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.datasets.id");
        QueryResult<Dataset> queryResult = datasetDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one dataset id found based on " + datasetName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Override
    public QueryResult<DatasetAclEntry> getDatasetAcls(String datasetStr, List<String> members, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long datasetId = getDatasetId(userId, datasetStr);
        authorizationManager.checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);
        Long studyId = getStudyIdByDataset(datasetId);

        // Split and obtain the set of members (users + groups), users and groups
        Set<String> memberSet = new HashSet<>();
        Set<String> userIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();

        for (String member: members) {
            memberSet.add(member);
            if (!member.startsWith("@")) {
                userIds.add(member);
            } else {
                groupIds.add(member);
            }
        }


        // Obtain the groups the user might belong to in order to be able to get the permissions properly
        // (the permissions might be given to the group instead of the user)
        // Map of group -> users
        Map<String, List<String>> groupUsers = new HashMap<>();

        if (userIds.size() > 0) {
            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
            // We add the groups where the users might belong to to the memberSet
            if (groups.getNumResults() > 0) {
                for (Group group : groups.getResult()) {
                    for (String tmpUserId : group.getUserIds()) {
                        if (userIds.contains(tmpUserId)) {
                            memberSet.add(group.getName());

                            if (!groupUsers.containsKey(group.getName())) {
                                groupUsers.put(group.getName(), new ArrayList<>());
                            }
                            groupUsers.get(group.getName()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<DatasetAclEntry> datasetAclQueryResult = datasetDBAdaptor.getDatasetAcl(datasetId, memberList);

        if (members.size() == 0) {
            return datasetAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one sampleAcl per member
        Map<String, DatasetAclEntry> datasetAclHashMap = new HashMap<>();
        for (DatasetAclEntry datasetAcl : datasetAclQueryResult.getResult()) {
            if (memberList.contains(datasetAcl.getMember())) {
                if (datasetAcl.getMember().startsWith("@")) {
                    // Check if the user was demanding the group directly or a user belonging to the group
                    if (groupIds.contains(datasetAcl.getMember())) {
                        datasetAclHashMap.put(datasetAcl.getMember(),
                                new DatasetAclEntry(datasetAcl.getMember(), datasetAcl.getPermissions()));
                    } else {
                        // Obtain the user(s) belonging to that group whose permissions wanted the userId
                        if (groupUsers.containsKey(datasetAcl.getMember())) {
                            for (String tmpUserId : groupUsers.get(datasetAcl.getMember())) {
                                if (userIds.contains(tmpUserId)) {
                                    datasetAclHashMap.put(tmpUserId, new DatasetAclEntry(tmpUserId, datasetAcl.getPermissions()));
                                }
                            }
                        }
                    }
                } else {
                    // Add the user
                    datasetAclHashMap.put(datasetAcl.getMember(), new DatasetAclEntry(datasetAcl.getMember(), datasetAcl.getPermissions()));
                }
            }
        }

        // We recreate the output that is in DatasetAclHashMap but in the same order the members were queried.
        List<DatasetAclEntry> datasetAclList = new ArrayList<>(datasetAclHashMap.size());
        for (String member : members) {
            if (datasetAclHashMap.containsKey(member)) {
                datasetAclList.add(datasetAclHashMap.get(member));
            }
        }

        // Update queryResult info
        datasetAclQueryResult.setId(datasetStr);
        datasetAclQueryResult.setNumResults(datasetAclList.size());
        datasetAclQueryResult.setNumTotalResults(datasetAclList.size());
        datasetAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        datasetAclQueryResult.setResult(datasetAclList);

        return datasetAclQueryResult;
    }

    @Override
    public DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.VIEW);

        URI fileUri = getFileUri(read(fileId, null, sessionId).first());
        boolean ignoreCase = options.getBoolean("ignoreCase");
        boolean multi = options.getBoolean("multi");
        return catalogIOManagerFactory.get(fileUri).getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    @Override
    public DataInputStream download(long fileId, int start, int limit, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DOWNLOAD);

        URI fileUri = getFileUri(read(fileId, null, sessionId).first());

        return catalogIOManagerFactory.get(fileUri).getFileObject(fileUri, start, limit);
    }

    @Override
    public DataInputStream head(long fileId, int lines, QueryOptions options, String sessionId) throws CatalogException {
        return download(fileId, 0, lines, options, sessionId);
    }

    @Override
    public QueryResult<FileAclEntry> getFileAcls(String fileStr, List<String> members, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long fileId = getFileId(userId, fileStr);
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);
        Long studyId = getStudyId(fileId);

        // Split and obtain the set of members (users + groups), users and groups
        Set<String> memberSet = new HashSet<>();
        Set<String> userIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();

        for (String member: members) {
            memberSet.add(member);
            if (!member.startsWith("@")) {
                userIds.add(member);
            } else {
                groupIds.add(member);
            }
        }


        // Obtain the groups the user might belong to in order to be able to get the permissions properly
        // (the permissions might be given to the group instead of the user)
        // Map of group -> users
        Map<String, List<String>> groupUsers = new HashMap<>();

        if (userIds.size() > 0) {
            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
            // We add the groups where the users might belong to to the memberSet
            if (groups.getNumResults() > 0) {
                for (Group group : groups.getResult()) {
                    for (String tmpUserId : group.getUserIds()) {
                        if (userIds.contains(tmpUserId)) {
                            memberSet.add(group.getName());

                            if (!groupUsers.containsKey(group.getName())) {
                                groupUsers.put(group.getName(), new ArrayList<>());
                            }
                            groupUsers.get(group.getName()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<FileAclEntry> fileAclQueryResult = fileDBAdaptor.getFileAcl(fileId, memberList);

        if (members.size() == 0) {
            return fileAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one fileAcl per member
        Map<String, FileAclEntry> fileAclHashMap = new HashMap<>();
        for (FileAclEntry fileAcl : fileAclQueryResult.getResult()) {
            if (memberList.contains(fileAcl.getMember())) {
                if (fileAcl.getMember().startsWith("@")) {
                    // Check if the user was demanding the group directly or a user belonging to the group
                    if (groupIds.contains(fileAcl.getMember())) {
                        fileAclHashMap.put(fileAcl.getMember(), new FileAclEntry(fileAcl.getMember(), fileAcl.getPermissions()));
                    } else {
                        // Obtain the user(s) belonging to that group whose permissions wanted the userId
                        if (groupUsers.containsKey(fileAcl.getMember())) {
                            for (String tmpUserId : groupUsers.get(fileAcl.getMember())) {
                                if (userIds.contains(tmpUserId)) {
                                    fileAclHashMap.put(tmpUserId, new FileAclEntry(tmpUserId, fileAcl.getPermissions()));
                                }
                            }
                        }
                    }
                } else {
                    // Add the user
                    fileAclHashMap.put(fileAcl.getMember(), new FileAclEntry(fileAcl.getMember(), fileAcl.getPermissions()));
                }
            }
        }

        // We recreate the output that is in fileAclHashMap but in the same order the members were queried.
        List<FileAclEntry> fileAclList = new ArrayList<>(fileAclHashMap.size());
        for (String member : members) {
            if (fileAclHashMap.containsKey(member)) {
                fileAclList.add(fileAclHashMap.get(member));
            }
        }

        // Update queryResult info
        fileAclQueryResult.setId(fileStr);
        fileAclQueryResult.setNumResults(fileAclList.size());
        fileAclQueryResult.setNumTotalResults(fileAclList.size());
        fileAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        fileAclQueryResult.setResult(fileAclList);

        return fileAclQueryResult;
    }

//    private File.Type getType(URI uri, boolean exists) throws CatalogException {
//        ParamsUtils.checkObj(uri, "uri");
//        return uri.getPath().endsWith("/") ? File.Type.DIRECTORY : File.Type.FILE;
//    }

//    private File.Bioformat setBioformat(File file, String sessionId) throws CatalogException {
//        ParamsUtils.checkObj(file, "file");
//
//
//        File.Bioformat bioformat = null;
//        ObjectMap parameters = new ObjectMap();
//        for (Map.Entry<File.Bioformat, Pattern> entry : bioformatMap.entrySet()) {
//            if (entry.getValue().matcher(file.getPath()).matches()) {
//                bioformat = entry.getKey();
//                break;
//            }
//        }
//
//        if (bioformat == File.Bioformat.VARIANT) {
//
//        }
//
//
//        update(file.getId(), parameters, new QueryOptions(), sessionId);
//
//        return File.Bioformat.NONE;
//    }

}
