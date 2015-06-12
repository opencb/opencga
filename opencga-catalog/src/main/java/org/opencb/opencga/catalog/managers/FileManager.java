package org.opencb.opencga.catalog.managers;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
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

    private static final QueryOptions includeStudyUri = new QueryOptions("include", Arrays.asList("projects.studies.uri"));

    protected static Logger logger = LoggerFactory.getLogger(FileManager.class);

    public FileManager(AuthorizationManager authorizationManager, AuthenticationManager authenticationManager,
                       CatalogDBAdaptor catalogDBAdaptor, CatalogIOManagerFactory ioManagerFactory,
                       Properties catalogProperties) {
        super(authorizationManager, authenticationManager, catalogDBAdaptor, ioManagerFactory, catalogProperties);
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
            URI studyUri = study.getUri() == null ? getStudyUri(study.getId()) : study.getUri();
            return file.getPath().isEmpty() ?
                    studyUri :
                    catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, file.getPath());
        }
    }

    @Override
    public URI getFileUri(URI studyUri, String relativeFilePath) throws CatalogException {
        ParamUtils.checkObj(studyUri, "studyUri");
        ParamUtils.checkObj(relativeFilePath, "relativeFilePath");

        return relativeFilePath.isEmpty() ?
                studyUri :
                catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
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

        if (file.getUri() != null) {
            return true;
        }
        List<String> paths = new LinkedList<>();
        String path = "";
        for (String f : file.getPath().split("/")) {
            paths.add(path = path + f + "/");
        }

        if (!paths.isEmpty()) {
            for (File folder : fileDBAdaptor.searchFile(
                    new QueryOptions(CatalogFileDBAdaptor.FileFilterOption.path.toString(), paths),
                    new QueryOptions("include", "projects.studies.files.uri")).getResult()) {
                if (folder.getUri() != null) {
                    return true;
                }
            }
        }
        return false;
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
    public QueryResult<File> createFolder(int studyId, String path, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        return create(studyId, File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE,
                path, null, null, null, File.Status.READY, 0, -1, null, -1, null, null,
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
        status = ParamUtils.defaultObject(status, File.Status.STAGE);

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
        int fileId = -1;
        if (parent != null) {   //If parent == null, the file is in the root of the study
            fileId = fileDBAdaptor.getFileId(studyId, parent.toString() + "/");
        }
        if (fileId < 0 && parent != null) {
            if (parents) {
                create(studyId, File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, parent.toString(),
                        file.getOwnerId(), file.getCreationDate(), "", File.Status.READY, 0, -1,
                        Collections.<Integer>emptyList(), -1, Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap(), true,
                        options, sessionId);
                fileId = fileDBAdaptor.getFileId(studyId, parent.toString() + "/");
            } else {
                throw new CatalogDBException("Directory not found " + parent.toString());
            }
        }

        //Check permissions
        Acl parentAcl;
        if (fileId < 0) {
            parentAcl = authorizationManager.getStudyACL(userId, studyId);
        } else {
            parentAcl = authorizationManager.getFileACL(userId, fileId);
        }

        if (!parentAcl.isWrite()) {
            throw new CatalogException("Permission denied, " + userId + " can not write in " +
                    (parent != null ? "directory " + parent.toString() : "study " + studyId));
        }

        //Check external file
        boolean isExternal = false;
        if (fileId > 0) {
            isExternal = isExternal(fileDBAdaptor.getFile(fileId, null).first());
        }

        if (file.getType() == File.Type.FOLDER && file.getStatus() == File.Status.READY && !isExternal) {
            URI studyUri = getStudyUri(studyId);
            CatalogIOManager ioManager = catalogIOManagerFactory.get(studyUri);
//            ioManager.createFolder(getStudyUri(studyId), folderPath.toString(), parents);
            ioManager.createFolder(studyUri, file.getPath(), parents);
        }


        return fileDBAdaptor.createFileToStudy(studyId, file, options);
    }

    @Override
    public QueryResult<File> read(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getFileACL(userId, id).isRead()) {
            throw new CatalogException("Permission denied. User can't read file");
        }

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
            if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
                throw new CatalogException("Permission denied. User " + userId + " can't read data from the study " + studyId);
            }
            query.put("studyId", studyId);
        }
        return fileDBAdaptor.searchFile(query, options);
    }

    @Override
    public QueryResult<File> update(Integer fileId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        File file = read(fileId, null, sessionId).first();
        switch (authorizationManager.getUserRole(userId)) {
            case ADMIN:
                logger.info("UserAdmin " + userId + " modifies file {id: " + fileId + "}");
                break;
            default:
                if (!authorizationManager.getFileACL(userId, fileId).isWrite()) {
                    throw new CatalogException("User " + userId + " can't modify the file " + fileId);
                }
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
                            throw new CatalogException("Parameter '" + s + "' can't be changed directly. " +
                                    "Use \"rename\" instead");
                        case "type":
                        default:
                            throw new CatalogException("Parameter '" + s + "' can't be changed. " +
                                    "Requires admin account");
                    }
                }
                break;
        }
        String ownerId = fileDBAdaptor.getFileOwnerId(fileId);
        QueryResult queryResult = fileDBAdaptor.modifyFile(fileId, parameters);
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

        if (!authorizationManager.getFileACL(userId, fileId).isDelete()) {
            throw new CatalogDBException("Permission denied. User can't delete this file");
        }
        QueryResult<File> fileResult = fileDBAdaptor.getFile(fileId, null);

        File file = fileResult.getResult().get(0);
        switch (file.getStatus()) {
            case STAGE:
                throw new CatalogException("File is not ready. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}");
            case TRASHED:
            case DELETED:
                //Send warning message
                return new QueryResult<File>("Delete file", 0, 0, 0,
                        "File already deleted. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}",
                        null, Collections.emptyList());
            case READY:
                break;
        }

        userDBAdaptor.updateUserLastActivity(ownerId);
        ObjectMap objectMap = new ObjectMap();
        objectMap.put("status", File.Status.TRASHED);
        objectMap.put("attributes", new ObjectMap(File.DELETE_DATE, System.currentTimeMillis()));

        switch (file.getType()) {
            case FOLDER:
                QueryResult<File> allFilesInFolder = fileDBAdaptor.getAllFilesInFolder(fileId, null);// delete recursively
                for (File subfile : allFilesInFolder.getResult()) {
                    delete(subfile.getId(), null, sessionId);
                }

                rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
                QueryResult<File> queryResult = fileDBAdaptor.modifyFile(fileId, objectMap);
                return queryResult; //TODO: Return the modified file
            case FILE:
                rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
                return fileDBAdaptor.modifyFile(fileId, objectMap); //TODO: Return the modified file
//            case INDEX:       //#62
//                throw new CatalogException("Can't delete INDEX file");
            //rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
            //return catalogDBAdaptor.modifyFile(fileId, objectMap);
        }
        return null;
    }


    @Override
    public QueryResult<File> rename(int fileId, String newName, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkPath(newName, "newName");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = userDBAdaptor.getProjectOwnerId(projectId);

        if (!authorizationManager.getFileACL(userId, fileId).isWrite()) {
            throw CatalogAuthorizationException.cantModify(userId, "File", fileId, null);
        }
        QueryResult<File> fileResult = fileDBAdaptor.getFile(fileId, null);
        if (fileResult.getResult().isEmpty()) {
            return new QueryResult<File>("Rename file", 0, 0, 0, "File not found", null, null);
        }
        File file = fileResult.getResult().get(0);
//        System.out.println("file = " + file);

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
        switch (file.getType()) {
            case FOLDER:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(studyUri); // TODO? check if something in the subtree is not READY?
                    catalogIOManager.rename(getFileUri(studyUri, oldPath), getFileUri(studyUri, newPath));   // io.move() 1
                }
                return fileDBAdaptor.renameFile(fileId, newPath); //TODO: Return the modified file
            case FILE:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(studyUri);
                    catalogIOManager.rename(getFileUri(studyUri, file.getPath()), getFileUri(studyUri, newPath));
                }
                return fileDBAdaptor.renameFile(fileId, newPath); //TODO: Return the modified file
        }

        return null;
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

        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw CatalogAuthorizationException.cantModify(userId, "Study", studyId, null);
        }
        for (Integer fileId : files) {
            if (fileDBAdaptor.getStudyIdByFileId(fileId) != studyId) {
                throw new CatalogException("Can't create a dataset with files from different files.");
            }
            if (!authorizationManager.getFileACL(userId, fileId).isRead()) {
                throw CatalogAuthorizationException.cantRead(userId, "File", fileId, null);
            }
        }

        Dataset dataset = new Dataset(-1, name, TimeUtils.getTime(), description, files, attributes);

        return fileDBAdaptor.createDataset(studyId, dataset, options);
    }

    @Override
    public QueryResult<Dataset> readDataset(int dataSetId, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = fileDBAdaptor.getStudyIdByDatasetId(dataSetId);

        if (!authorizationManager.getStudyACL(userId, studyId).isRead()) {
            throw CatalogAuthorizationException.cantRead(userId, "DataSet", dataSetId, null);
        }

        return fileDBAdaptor.getDataset(dataSetId, options);
    }

    @Override
    public DataInputStream grep(int fileId, String pattern, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getFileACL(userId, fileId).isRead()) {
            throw CatalogAuthorizationException.cantRead(userId, "File", fileId, null);
        }

        URI fileUri = getFileUri(read(fileId, null, sessionId).first());
        boolean ignoreCase = options.getBoolean("ignoreCase");
        boolean multi = options.getBoolean("multi");
        return catalogIOManagerFactory.get(fileUri).getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    @Override
    public DataInputStream download(int fileId, int start, int limit, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        if (!authorizationManager.getFileACL(userId, fileId).isRead()) {
            throw CatalogAuthorizationException.cantRead(userId, "File", fileId, null);
        }

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
