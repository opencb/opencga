package org.opencb.opencga.catalog;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.api.IFileManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by hpccoll1 on 28/04/15.
 */
public class FileManager implements IFileManager {

    final protected AuthorizationManager authorizationManager;
    final protected CatalogUserDBAdaptor userDBAdaptor;
    final protected CatalogStudyDBAdaptor studyDBAdaptor;
    final protected CatalogFileDBAdaptor fileDBAdaptor;
    final protected CatalogSamplesDBAdaptor sampleDBAdaptor;
    final protected CatalogJobDBAdaptor jobDBAdaptor;
    final protected CatalogIOManagerFactory catalogIOManagerFactory;

    protected static Logger logger = LoggerFactory.getLogger(FileManager.class);

    public FileManager(AuthorizationManager authorizationManager, CatalogDBAdaptor catalogDBAdaptor, CatalogIOManagerFactory ioManagerFactory) {
        this.authorizationManager = authorizationManager;
        this.userDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptor.getCatalogSamplesDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptor.getCatalogJobDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
    }

    @Override
    public URI getStudyUri(int studyId)
            throws CatalogException {
        return studyDBAdaptor.getStudy(studyId, new QueryOptions("include", Arrays.asList("projects.studies.uri"))).first().getUri();
    }

    @Override
    public URI getFileUri(int studyId, String relativeFilePath)
            throws CatalogException {
        URI studyUri = getStudyUri(studyId);
        return catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
    }

    @Override
    public URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogIOManagerException {
        return catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
    }

    @Override
    public String getUserId(int fileId) throws CatalogException {
        return fileDBAdaptor.getFileOwnerId(fileId);
    }

    @Override
    public Integer getProjectId(int fileId) throws CatalogException {
        throw new UnsupportedOperationException();
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
        return fileDBAdaptor.getFileId(studyId, projectStudyPath[2]);    }

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
                File.Status.valueOf(params.getString("type", File.Status.UPLOADING.toString())),
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
        checkParameter(sessionId, "sessionId");
        checkPath(path, "filePath");

        type = defaultObject(type, File.Type.FILE);
        format = defaultObject(format, File.Format.PLAIN);  //TODO: Inference from the file name
        bioformat = defaultObject(bioformat, File.Bioformat.NONE);
        ownerId = defaultString(ownerId, userId);
        creationDate = defaultString(creationDate, TimeUtils.getTime());
        description = defaultString(description, "");
        status = defaultObject(status, File.Status.UPLOADING);

        if (diskUsage < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }
        if (experimentId > 0 && !jobDBAdaptor.experimentExists(experimentId)) {
            throw new CatalogException("Experiment { id: " + experimentId + "} does not exist.");
        }
        sampleIds = defaultObject(sampleIds, new LinkedList<Integer>());

        for (Integer sampleId : sampleIds) {
            if (!sampleDBAdaptor.sampleExists(sampleId)) {
                throw new CatalogException("Sample { id: " + sampleId + "} does not exist.");
            }
        }

        if (jobId > 0 && !jobDBAdaptor.jobExists(jobId)) {
            throw new CatalogException("Job { id: " + jobId + "} does not exist.");
        }
        stats = defaultObject(stats, new HashMap<String, Object>());
        attributes = defaultObject(attributes, new HashMap<String, Object>());

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

        if (status != File.Status.UPLOADING && type == File.Type.FILE) {
            if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
                throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with status != UPLOADING and INDEXING");
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

        /**
         * CHECK ALREADY EXISTS
         */
        if (fileDBAdaptor.getFileId(studyId, path) >= 0) {
            throw new CatalogException("Cannot create file ‘" + path + "’: File exists");
        }

        //Find parent. If parents == true, create folders.
        Path parent = Paths.get(path).getParent();
        int fileId = -1;
        if (parent != null) {   //If parent == null, the file is in the root of the study
            fileId = fileDBAdaptor.getFileId(studyId, parent.toString() + "/");
        }
        if (fileId < 0 && parent != null) {
            if (parents) {
                create(studyId, File.Type.FOLDER, File.Format.PLAIN, File.Bioformat.NONE, parent.toString(),
                        ownerId, creationDate, "", File.Status.READY, 0, -1, Collections.<Integer>emptyList(),
                        -1, Collections.<String, Object>emptyMap(), Collections.<String, Object>emptyMap(), true,
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

        if (type == File.Type.FOLDER) {
            URI studyUri = getStudyUri(studyId);
            CatalogIOManager ioManager = catalogIOManagerFactory.get(studyUri);
//            ioManager.createFolder(getStudyUri(studyId), folderPath.toString(), parents);
            ioManager.createFolder(studyUri, path, parents);
        }


        return fileDBAdaptor.createFileToStudy(studyId, file, options);
    }

    @Override
    public QueryResult<File> read(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        checkParameter(sessionId, "sessionId");

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
        checkParameter(sessionId, "sessionId");
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
    public QueryResult<File> update(Integer fileId, ObjectMap parameters, String sessionId)
            throws CatalogException {
        checkObj(parameters, "Parameters");
        checkParameter(sessionId, "sessionId");
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

                        //Can only be modified when file.status == UPLOADING
                        case "creationDate":
                        case "diskUsage":
                            if (!file.getStatus().equals(File.Status.UPLOADING)) {
                                throw new CatalogException("Parameter '" + s + "' can't be changed when " +
                                        "status == " + file.getStatus().name() + ". " +
                                        "Required status UPLOADING or admin account");
                            }
                            break;
                        //Path and Name must be changed with "raname" and/or "move" methods.
                        case "path":
                        case "name":
                            throw new CatalogException("Parameter '" + s + "' can't be changed directly. " +
                                    "Use \"renameFile\" instead");
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
        checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = userDBAdaptor.getProjectOwnerId(projectId);

        if (!authorizationManager.getFileACL(userId, fileId).isDelete()) {
            throw new CatalogDBException("Permission denied. User can't delete this file");
        }
        QueryResult<File> fileResult = fileDBAdaptor.getFile(fileId, null);

        File file = fileResult.getResult().get(0);
        switch(file.getStatus()) {
            case UPLOADING:
            case UPLOADED:
                throw new CatalogException("File is not ready. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}");
            case DELETING:
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
        objectMap.put("status", File.Status.DELETING);
        objectMap.put("attributes", new ObjectMap(File.DELETE_DATE, System.currentTimeMillis()));

        switch (file.getType()) {
            case FOLDER:
                QueryResult<File> allFilesInFolder = fileDBAdaptor.getAllFilesInFolder(fileId, null);// delete recursively
                for (File subfile : allFilesInFolder.getResult()) {
                    delete(subfile.getId(), null, sessionId);
                }

                renameFile(fileId, ".deleted_" + TimeUtils.getTime() + "_" +  file.getName(), sessionId);
                QueryResult<File> queryResult = fileDBAdaptor.modifyFile(fileId, objectMap);
                return queryResult; //TODO: Return the modified file
            case FILE:
                renameFile(fileId, ".deleted_" + TimeUtils.getTime() + "_" +file.getName(), sessionId);
                return fileDBAdaptor.modifyFile(fileId, objectMap); //TODO: Return the modified file
//            case INDEX:       //#62
//                throw new CatalogException("Can't delete INDEX file");
            //renameFile(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
            //return catalogDBAdaptor.modifyFile(fileId, objectMap);
        }
        return null;
    }


    @Override
    public QueryResult<File> renameFile(int fileId, String newName, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkPath(newName, "newName");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        int projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = userDBAdaptor.getProjectOwnerId(projectId);

        if (!authorizationManager.getFileACL(userId, fileId).isWrite()) {
            throw new CatalogDBException("Permission denied. User can't rename this file");
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
        switch (file.getType()) {
            case FOLDER:
                catalogIOManager = catalogIOManagerFactory.get(studyUri); // TODO? check if something in the subtree is not READY?
                catalogIOManager.rename(getFileUri(studyUri, oldPath), getFileUri(studyUri, newPath));   // io.move() 1
                return fileDBAdaptor.renameFile(fileId, newPath); //TODO: Return the modified file
            case FILE:
                catalogIOManager = catalogIOManagerFactory.get(studyUri);
                catalogIOManager.rename(getFileUri(studyUri, file.getPath()), getFileUri(studyUri, newPath));
                return fileDBAdaptor.renameFile(fileId, newPath); //TODO: Return the modified file
        }

        return null;
    }


    @Override
    public QueryResult<Dataset> createDataset(int studyId, String name, String description, List<Integer> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        checkParameter(name, "name");
        checkObj(files, "files");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        description = defaultString(description, "");
        attributes = defaultObject(attributes, Collections.<String, Object>emptyMap());

        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify the study " + studyId);
        }
        for (Integer fileId : files) {
            if (fileDBAdaptor.getStudyIdByFileId(fileId) != studyId) {
                throw new CatalogException("Can't create a dataset with files from different files.");
            }
            if (!authorizationManager.getFileACL(userId, fileId).isRead()) {
                throw new CatalogException("Permission denied. User " + userId + " can't read the file " + fileId);
            }
        }

        Dataset dataset = new Dataset(-1, name, TimeUtils.getTime(), description, files, attributes);

        return fileDBAdaptor.createDataset(studyId, dataset, options);
    }

    @Override
    public QueryResult<Dataset> getDataset(int dataSetId, QueryOptions options, String sessionId)
            throws CatalogException {
        checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        int studyId = fileDBAdaptor.getStudyIdByDatasetId(dataSetId);

        if (!authorizationManager.getStudyACL(userId, studyId).isWrite()) {
            throw new CatalogException("Permission denied. User " + userId + " can't modify the study " + studyId);
        }

        return fileDBAdaptor.getDataset(dataSetId, options);
    }


}
