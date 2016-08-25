package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.CatalogPermission;
import org.opencb.opencga.catalog.authorization.StudyPermission;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileManager extends AbstractManager implements IFileManager {

    protected static Logger logger = LoggerFactory.getLogger(FileManager.class);

    private static final QueryOptions includeStudyUri = new QueryOptions("include", Collections.singletonList("projects.studies.uri"));
    private static final QueryOptions includeFileUriPath = new QueryOptions("include", Arrays.asList("projects.studies.files.uri", "projects.studies.files.path"));
    private static final Comparator<File> rootFirstComparator =
            (f1, f2) -> (f1.getPath() == null ? 0 : f1.getPath().length()) - (f2.getPath() == null ? 0 : f2.getPath().length());
    private static final Comparator<File> rootLastComparator =
            (f1, f2) -> (f2.getPath() == null ? 0 : f2.getPath().length()) - (f1.getPath() == null ? 0 : f1.getPath().length());


    public FileManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                       AuditManager auditManager,
                       CatalogDBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       Properties catalogProperties) {
        super(authorizationManager, authenticationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    @Override
    public URI getStudyUri(int studyId)
            throws CatalogException {
        return studyDBAdaptor.getStudy(studyId, includeStudyUri).first().getUri();
    }

    @Override
    public URI getFileUri(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            return getFileUri(studyDBAdaptor.getStudy(getStudyId(file.getId()), includeStudyUri).first(), file);
        }
    }

    @Override
    public URI getFileUri(Study study, File file) throws CatalogException {
        ParamUtils.checkObj(study, "Study");
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            QueryResult<File> parents = getParents(file, false, includeFileUriPath);
            for (File parent : parents.getResult()) {
                if (parent.getUri() != null) {
                    String relativePath = file.getPath().replaceFirst(parent.getPath(), "");
                    return parent.getUri().resolve(relativePath);
                }
            }
            URI studyUri = study.getUri() == null ? getStudyUri(study.getId()) : study.getUri();
            return file.getPath().isEmpty() ?
                    studyUri :
                    catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, file.getPath());
        }
    }

    @Deprecated
    @Override
    public URI getFileUri(URI studyUri, String relativeFilePath) throws CatalogException {
        ParamUtils.checkObj(studyUri, "studyUri");
        ParamUtils.checkObj(relativeFilePath, "relativeFilePath");

        return relativeFilePath.isEmpty() ?
                studyUri :
                catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
    }

    public URI getFileUri(int studyId, String filePath) throws CatalogException {
        ParamUtils.checkObj(filePath, "filePath");

        List<File> parents = getParents(false, new QueryOptions("include", "projects.studies.files.path,projects.studies.files.uri"), filePath, studyId).getResult();

        for (File parent : parents) {
            if (parent.getUri() != null) {
                String relativePath = filePath.replaceFirst(parent.getPath(), "");
                return parent.getUri().resolve(relativePath);
            }
        }
        URI studyUri = getStudyUri(studyId);
        return filePath.isEmpty() ?
                studyUri :
                catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, filePath);
    }

    @Override
    public String getUserId(int fileId) throws CatalogException {
        return fileDBAdaptor.getFileOwnerId(fileId);
    }

    @Override
    public Integer getStudyId(int fileId) throws CatalogException {
        return fileDBAdaptor.getStudyIdByFileId(fileId);
    }

    @Override
    public Integer getFileId(String id) throws CatalogException {
        try {
            return Integer.parseInt(id);
        } catch (NumberFormatException ignore) {
        }

        String[] split = id.split("@", 2);
        if (split.length != 2) {
            return -1;
        }
        String[] projectStudyPath = split[1].replace(':', '/').split("/", 3);
        if (projectStudyPath.length <= 2) {
            return -2;
        }
        int projectId = userDBAdaptor.getProjectId(split[0], projectStudyPath[0]);
        int studyId = studyDBAdaptor.getStudyId(projectId, projectStudyPath[1]);
        return fileDBAdaptor.getFileId(studyId, projectStudyPath[2]);
    }

    /**
     * Returns if a file is externally located.
     *
     * A file externally located is the one with a URI or a parent folder with an external URI.
     *
     * @throws CatalogException
     */
    @Override
    public boolean isExternal(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        return file.getUri() != null;
    }

    public boolean isRootFolder(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        return file.getPath().isEmpty();
    }

    @Override
    public QueryResult<File> getParents(int fileId, QueryOptions options, String sessionId) throws CatalogException {
        return getParents(true, options, read(fileId, new QueryOptions("include", "projects.studies.files.path"), sessionId).first().getPath(), getStudyId(fileId));
    }

    /**
     * Return all parent folders from a file.
     * @param file
     * @param options
     * @return
     * @throws CatalogException
     */
    private QueryResult<File> getParents(File file, boolean rootFirst, QueryOptions options) throws CatalogException {
        String filePath = file.getPath();
        return getParents(rootFirst, options, filePath, getStudyId(file.getId()));
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
            paths.add(path = path + f + "/");
        }
        paths.add(filePath); //Add the file path
        return paths;
    }

    private QueryResult<File> getParents(boolean rootFirst, QueryOptions options, String filePath, int studyId) throws CatalogException {
        List<String> paths = getParentPaths(filePath);

        QueryOptions query = new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.path.toString(), paths);
        query.put(CatalogFileDBAdaptor.FileFilterOption.studyId.toString(), studyId);
        QueryResult<File> result = fileDBAdaptor.getAllFiles(
                query,
                options);
        result.getResult().sort(rootFirst ? rootFirstComparator : rootLastComparator);
        return result;
    }

    @Override
    public QueryResult<File> create(QueryOptions params, String sessionId) throws CatalogException {
        return create(
                params.getInt("studyId"),
                File.Type.valueOf(params.getString("type", File.Type.FILE.toString())),
                File.Format.valueOf(params.getString("format", File.Format.PLAIN.toString())),
                File.Bioformat.valueOf(params.getString("type", File.Bioformat.NONE.toString())),
                params.getString("path", null),
                params.getString("ownerId", null),
                params.getString("creationDate", null),
                params.getString("description", null),
                File.Status.valueOf(params.getString("type", File.Status.STAGE.toString())),
                params.getLong("diskUsage", 0),
                params.getInt("experimentId", -1),
                params.getAsIntegerList("sampleIds"),
                params.getInt("jobId", -1),
                params.getMap("stats", null),
                params.getMap("attributes", null),
                params.getBoolean("parents"),
                params,
                sessionId
        );
    }

    @Override
    public QueryResult<File> createFolder(int studyId, String path, File.Status status, boolean parents, String description, QueryOptions options, String sessionId)
            throws CatalogException {
        return create(studyId, File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE,
                path, null, null, description, status, 0, -1, null, -1, null, null,
                parents, options, sessionId);
    }

    @Override
    public QueryResult<File> create(
            int studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path, String ownerId,
            String creationDate, String description, File.Status status, long diskUsage, int experimentId,
            List<Integer> sampleIds, int jobId, Map<String, Object> stats, Map<String, Object> attributes,
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
        status = type == File.Type.FILE ?
                ParamUtils.defaultObject(status, File.Status.STAGE) :   //By default, files are STAGED
                ParamUtils.defaultObject(status, File.Status.READY) ;   //By default, folders are READY

        if (diskUsage < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }
        if (experimentId > 0 && !jobDBAdaptor.experimentExists(experimentId)) {
            throw new CatalogException("Experiment { id: " + experimentId + "} does not exist.");
        }
        sampleIds = ParamUtils.defaultObject(sampleIds, LinkedList<Integer>::new);

        for (Integer sampleId : sampleIds) {
            if (!sampleDBAdaptor.sampleExists(sampleId)) {
                throw new CatalogException("Sample { id: " + sampleId + "} does not exist.");
            }
        }

        if (jobId > 0 && !jobDBAdaptor.jobExists(jobId)) {
            throw new CatalogException("Job { id: " + jobId + "} does not exist.");
        }
        stats = ParamUtils.defaultObject(stats, HashMap<String, Object>::new);
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (!studyDBAdaptor.studyExists(studyId)) {
            throw new CatalogException("Study { id: " + studyId + "} does not exist.");
        }

        if (!ownerId.equals(userId)) {
            if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
                throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with ownerId != userId");
            } else {
                if (!userDBAdaptor.userExists(ownerId)) {
                    throw new CatalogException("ERROR: ownerId does not exist.");
                }
            }
        }

        if (status != File.Status.STAGE && type == File.Type.FILE) {
            if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
                throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with status != STAGE and INDEXING");
            }
        }

        if (type == File.Type.FOLDER && !path.endsWith("/")) {
            path += "/";
        }
        if (type == File.Type.FILE && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }


        //Create file object
        File file = new File(-1, Paths.get(path).getFileName().toString(), type, format, bioformat,
                path, ownerId, creationDate, description, status, diskUsage, experimentId, sampleIds, jobId,
                new LinkedList<>(), stats, attributes);

        return create(studyId, file, parents, options, sessionId);
    }

    /**
     * Unchecked create file. Private only
     *
     * @throws CatalogException
     */
    private QueryResult<File> create(int studyId, File file, boolean parents, QueryOptions options, String sessionId) throws CatalogException {

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        /**
         * CHECK ALREADY EXISTS
         */
        if (fileDBAdaptor.getFileId(studyId, file.getPath()) >= 0) {
            if (file.getType() == File.Type.FOLDER && parents) {
                return read(fileDBAdaptor.getFileId(studyId, file.getPath()), options, sessionId);
            } else {
                throw new CatalogException("Cannot create file ‘" + file.getPath() + "’: File exists");
            }
        }

        //Find parent. If parents == true, create folders.
        Path parent = Paths.get(file.getPath()).getParent();
        String parentPath;
        boolean isRoot = false;
        if (parent == null) {   //If parent == null, the file is in the root of the study
            parentPath = "";
            isRoot = true;
        } else {
            parentPath = parent.toString() + "/";
        }

        int parentFileId = fileDBAdaptor.getFileId(studyId, parentPath);
        if (parentFileId < 0 && parent != null) {
            if (parents) {
                create(studyId, File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, parent.toString(),
                        file.getOwnerId(), file.getCreationDate(), "", File.Status.READY, 0, -1,
                        Collections.<Integer>emptyList(), -1, Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap(), true,
                        options, sessionId);
                parentFileId = fileDBAdaptor.getFileId(studyId, parent.toString() + "/");
            } else {
                throw new CatalogDBException("Directory not found " + parent.toString());
            }
        }

        //Check permissions
        if (parentFileId < 0) {
            throw new CatalogException("Unable to create file without a parent file");
        } else {
            authorizationManager.checkFilePermission(parentFileId, userId, CatalogPermission.WRITE);
        }


        //Check external file
        boolean isExternal = isExternal(file);

        if (file.getType() == File.Type.FOLDER && file.getStatus() == File.Status.READY && (!isExternal || isRoot)) {
            URI fileUri = getFileUri(studyId, file.getPath());
            CatalogIOManager ioManager = catalogIOManagerFactory.get(fileUri);
            ioManager.createDirectory(fileUri, parents);
        }


        QueryResult<File> queryResult = fileDBAdaptor.createFile(studyId, file, options);
        auditManager.recordCreation(AuditRecord.Resource.file, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<File> read(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(id, userId, CatalogPermission.READ);

        return fileDBAdaptor.getFile(id, options);
    }

    @Override
    public QueryResult<File> getParent(int fileId, QueryOptions options, String sessionId)
            throws CatalogException {

        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
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
    public QueryResult<File> readAll(QueryOptions query, QueryOptions options, String sessionId)
            throws CatalogException {
        return readAll(query.getInt("studyId", -1), query, options, sessionId);
    }

    @Override
    public QueryResult<File> readAll(int studyId, QueryOptions query, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, QueryOptions::new);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);


        if (studyId <= 0) {
            switch (authorizationManager.getUserRole(userId)) {
                case ADMIN:
                    break;
                default:
                    throw new CatalogDBException("Permission denied. StudyId or Admin role required");
            }
        } else {
            authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.READ_STUDY);
            query.put(CatalogFileDBAdaptor.FileFilterOption.studyId.toString(), studyId);
        }
        QueryResult<File> queryResult = fileDBAdaptor.getAllFiles(query, options);
        authorizationManager.filterFiles(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
    }

    @Override
    public QueryResult<File> update(Integer fileId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        File file = read(fileId, null, sessionId).first();

        if (isRootFolder(file)) {
            throw new CatalogException("Can not modify root folder");
        }

        switch (authorizationManager.getUserRole(userId)) {
            case ADMIN:
                logger.info("UserAdmin " + userId + " modifies file {id: " + fileId + "}");
                break;
            default:
                authorizationManager.checkFilePermission(fileId, userId, CatalogPermission.WRITE);
                for (String s : parameters.keySet()) {
                    switch (s) { //Special cases
                        //Can be modified anytime
                        case "format":
                        case "bioformat":
                        case "description":
                        case "status":
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
//                            if (!file.getStatus().equals(File.Status.STAGE)) {
//                                throw new CatalogException("Parameter '" + s + "' can't be changed when " +
//                                        "status == " + file.getStatus().name() + ". " +
//                                        "Required status STAGE or admin account");
//                            }
                            break;
                        //Path and Name must be changed with "raname" and/or "move" methods.
                        case "path":
                        case "name":
                            break;
                        case "type":
                        default:
                            throw new CatalogException("Parameter '" + s + "' can't be changed. " +
                                    "Requires admin account");
                    }
                }
                break;
        }
        //Path and Name must be changed with "raname" and/or "move" methods.
        if (parameters.containsKey("name")) {
            logger.info("Rename file using update method!");
            rename(fileId, parameters.getString("name"), sessionId);
        }
        if (parameters.containsKey("path") ) {
            logger.info("Move file using update method!");
            move(fileId, parameters.getString("path"), options, sessionId);
        }

        String ownerId = fileDBAdaptor.getFileOwnerId(fileId);
        QueryResult queryResult = fileDBAdaptor.modifyFile(fileId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
        userDBAdaptor.updateUserLastActivity(ownerId);
        return queryResult;
    }

    @Override
    public QueryResult<File> delete(Integer fileId, QueryOptions options, String sessionId)
            throws CatalogException {        //Safe delete: Don't delete. Just rename file and set {deleting:true}
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = userDBAdaptor.getProjectOwnerId(projectId);

        File file = fileDBAdaptor.getFile(fileId, null).first();

        if (isRootFolder(file)) {
            throw new CatalogException("Can not delete root folder");
        }

        QueryResult<File> result = checkCanDeleteFile(file, userId);
        if (result != null) {
            return result;
        }

        userDBAdaptor.updateUserLastActivity(ownerId);
        ObjectMap objectMap = new ObjectMap();
        objectMap.put("status", File.Status.TRASHED);
        objectMap.put("attributes", new ObjectMap(File.DELETE_DATE, System.currentTimeMillis()));

        switch (file.getType()) {
            case FOLDER: {
                QueryResult<File> allFilesInFolder = fileDBAdaptor.getAllFilesInFolder(fileId, null);
                // delete recursively. Walk tree depth first
                for (File subfolder : allFilesInFolder.getResult()) {
                    if (subfolder.getType() == File.Type.FOLDER) {
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

                fileDBAdaptor.modifyFile(fileId, objectMap);
                QueryResult<File> queryResult = rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, objectMap, null, null);
                return queryResult; //TODO: Return the modified file
            }
            case FILE: {
                rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
                QueryResult<File> queryResult = fileDBAdaptor.modifyFile(fileId, objectMap);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, objectMap, null, null);
                return queryResult; //TODO: Return the modified file
            }
        }
        return null;
    }

    private QueryResult<File> checkCanDeleteFile(File file, String userId) throws CatalogException {
        authorizationManager.checkFilePermission(file.getId(), userId, CatalogPermission.DELETE);

        switch (file.getStatus()) {
            case STAGE:
            case MISSING:
            default:
                throw new CatalogException("File is not ready. {" +
                        "id: " + file.getId() + ", " +
                        "path:\"" + file.getPath() + "\"," +
                        "status: '" + file.getStatus() + "'}");
            case TRASHED:
            case DELETED:
                //Send warning message
                String warningMsg = "File already deleted. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}";
                logger.warn(warningMsg);
                return new QueryResult<File>("Delete file", 0, 0, 0,
                        warningMsg,
                        null, Collections.emptyList());
            case READY:
                break;
        }
        return null;
    }

    @Override
    public QueryResult<File> rename(int fileId, String newName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkFileName(newName, "name");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = userDBAdaptor.getProjectOwnerId(projectId);

        authorizationManager.checkFilePermission(fileId, userId, CatalogPermission.WRITE);
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

        userDBAdaptor.updateUserLastActivity(ownerId);
        CatalogIOManager catalogIOManager;
        URI studyUri = getStudyUri(studyId);
        boolean isExternal = isExternal(file); //If the file URI is not null, the file is external located.
        QueryResult<File> result;
        switch (file.getType()) {
            case FOLDER:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(studyUri); // TODO? check if something in the subtree is not READY?
                    catalogIOManager.rename(getFileUri(studyId, oldPath), getFileUri(studyId, newPath));   // io.move() 1
                }
                result = fileDBAdaptor.renameFile(fileId, newPath, null);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, new ObjectMap("path", newPath).append("name", newName), "rename", null);
                break;
            case FILE:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(studyUri);
                    catalogIOManager.rename(getFileUri(studyId, file.getPath()), getFileUri(studyId, newPath));
                }
                result = fileDBAdaptor.renameFile(fileId, newPath, null);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, new ObjectMap("path", newPath).append("name", newName), "rename", null);
                break;
            default:
                throw new CatalogException("Unknown file type " + file.getType());
        }

        return result;
    }

    @Override
    public QueryResult move(int fileId, String newPath, QueryOptions options, String sessionId)
            throws CatalogException {
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
    public QueryResult<Dataset> createDataset(int studyId, String name, String description, List<Integer> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(files, "files");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        authorizationManager.checkStudyPermission(studyId, userId, StudyPermission.MANAGE_STUDY);

        for (Integer fileId : files) {
            if (fileDBAdaptor.getStudyIdByFileId(fileId) != studyId) {
                throw new CatalogException("Can't create a dataset with files from different files.");
            }
            authorizationManager.checkFilePermission(fileId, userId, CatalogPermission.READ);
        }

        Dataset dataset = new Dataset(-1, name, TimeUtils.getTime(), description, files, attributes);
        QueryResult<Dataset> queryResult = fileDBAdaptor.createDataset(studyId, dataset, options);
        auditManager.recordCreation(AuditRecord.Resource.dataset, queryResult.first().getId(), userId, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Dataset> readDataset(int dataSetId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        QueryResult<Dataset> queryResult = fileDBAdaptor.getDataset(dataSetId, options);

        for (Integer fileId : queryResult.first().getFiles()) {
            authorizationManager.checkFilePermission(fileId, userId, CatalogPermission.READ);
        }

        return queryResult;
    }

    @Override
    public DataInputStream grep(int fileId, String pattern, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(fileId, userId, CatalogPermission.READ);

        URI fileUri = getFileUri(read(fileId, null, sessionId).first());
        boolean ignoreCase = options.getBoolean("ignoreCase");
        boolean multi = options.getBoolean("multi");
        return catalogIOManagerFactory.get(fileUri).getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    @Override
    public DataInputStream download(int fileId, int start, int limit, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(fileId, userId, CatalogPermission.READ);

        URI fileUri = getFileUri(read(fileId, null, sessionId).first());

        return catalogIOManagerFactory.get(fileUri).getFileObject(fileUri, start, limit);
    }

    @Override
    public DataInputStream head(int fileId, int lines, QueryOptions options, String sessionId)
            throws CatalogException {
        return download(fileId, 0, lines, options, sessionId);
    }

//    private File.Type getType(URI uri, boolean exists) throws CatalogException {
//        ParamsUtils.checkObj(uri, "uri");
//        return uri.getPath().endsWith("/") ? File.Type.FOLDER : File.Type.FILE;
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
