package org.opencb.opencga.catalog.core;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.opencb.opencga.catalog.io.CatalogIOManagerException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jacobo on 11/02/15.
 */
public interface ICatalogManager {

    //New Interface
    CatalogClient client();
    CatalogClient client(String sessionId);

    //Old interface
    CatalogIOManagerFactory getCatalogIOManagerFactory();

    URI getUserUri(String userId) throws CatalogException;

    URI getProjectUri(String userId, String projectId) throws CatalogException;

    URI getStudyUri(int studyId)
            throws CatalogException;

    URI getFileUri(int studyId, String relativeFilePath)
            throws CatalogException;

    URI getFileUri(URI studyUri, String relativeFilePath)
            throws CatalogException, IOException;

    URI getFileUri(File file) throws CatalogException;

    int getProjectIdByStudyId(int studyId) throws CatalogException;

    int getProjectId(String id) throws CatalogDBException;

    int getStudyId(String id) throws CatalogDBException;

    int getFileId(String id) throws CatalogDBException;

    int getToolId(String id) throws CatalogDBException;

    QueryResult<User> createUser(String id, String name, String email, String password, String organization, QueryOptions options)
            throws CatalogException;

    QueryResult<User> createUser(String id, String name, String email, String password, String organization, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<ObjectMap> loginAsAnonymous(String sessionIp)
            throws CatalogException, IOException;

    QueryResult<ObjectMap> login(String userId, String password, String sessionIp)
            throws CatalogException, IOException;

    QueryResult logout(String userId, String sessionId) throws CatalogException;

    QueryResult logoutAnonymous(String sessionId) throws CatalogException;

    QueryResult changePassword(String userId, String oldPassword, String newPassword, String sessionId)
            throws CatalogException;

    QueryResult changeEmail(String userId, String nEmail, String sessionId) throws CatalogException;

    QueryResult resetPassword(String userId, String email) throws CatalogException;

    QueryResult<User> getUser(String userId, String lastActivity, String sessionId) throws CatalogException;

    QueryResult<User> getUser(String userId, String lastActivity, QueryOptions options, String sessionId)
            throws CatalogException;

    String getUserIdBySessionId(String sessionId);

    QueryResult modifyUser(String userId, ObjectMap parameters, String sessionId)
            throws CatalogException;

    void deleteUser(String userId, String sessionId) throws CatalogException;

    QueryResult<Project> createProject(String ownerId, String name, String alias, String description,
                                       String organization, QueryOptions options, String sessionId)
            throws CatalogException,
            CatalogIOManagerException;

    QueryResult<Project> getProject(int projectId, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<Project> getAllProjects(String ownerId, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult renameProject(int projectId, String newProjectAlias, String sessionId)
            throws CatalogException;

    QueryResult modifyProject(int projectId, ObjectMap parameters, String sessionId)
            throws CatalogException;

    QueryResult shareProject(int projectId, Acl acl, String sessionId) throws CatalogException;

    QueryResult<Study> createStudy(int projectId, String name, String alias, Study.Type type, String description,
                                   String sessionId)
            throws CatalogException, IOException;

    QueryResult<Study> createStudy(int projectId, String name, String alias, Study.Type type,
                                   String creatorId, String creationDate, String description, String status,
                                   String cipher, String uriScheme, URI uri,
                                   Map<File.Bioformat, DataStore> datastores, Map<String, Object> stats,
                                   Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException, IOException;

    QueryResult<Study> getStudy(int studyId, String sessionId)
            throws CatalogException;

    QueryResult<Study> getStudy(int studyId, String sessionId, QueryOptions options)
            throws CatalogException;

    QueryResult<Study> getAllStudies(int projectId, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult renameStudy(int studyId, String newStudyAlias, String sessionId)
            throws CatalogException;

    QueryResult modifyStudy(int studyId, ObjectMap parameters, String sessionId)
            throws CatalogException;

    QueryResult shareStudy(int studyId, Acl acl, String sessionId) throws CatalogException;

    String getFileOwner(int fileId) throws CatalogDBException;

    int getStudyIdByFileId(int fileId) throws CatalogDBException;

    @Deprecated
    QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                 boolean parents, String sessionId)
            throws CatalogException, CatalogIOManagerException;

    //create file with byte[]
    QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, byte[] bytes, String description,
                                 boolean parents, String sessionId)
            throws CatalogException, IOException;

    QueryResult<File> createFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                 boolean parents, int jobId, String sessionId)
            throws CatalogException, CatalogIOManagerException;

    QueryResult<File> createFile(int studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                 String ownerId, String creationDate, String description, File.Status status,
                                 long diskUsage, int experimentId, List<Integer> sampleIds, int jobId,
                                 Map<String, Object> stats, Map<String, Object> attributes,
                                 boolean parents, QueryOptions options, String sessionId)
            throws CatalogException;

    @Deprecated
    QueryResult<File> uploadFile(int studyId, File.Format format, File.Bioformat bioformat, String path, String description,
                                 boolean parents, InputStream fileIs, String sessionId)
            throws IOException, CatalogException;

    @Deprecated
    QueryResult<File> uploadFile(int fileId, InputStream fileIs, String sessionId) throws CatalogException,
            CatalogIOManagerException, IOException, InterruptedException;

    QueryResult<File> createFolder(int studyId, Path folderPath, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult deleteFolder(int folderId, String sessionId)
            throws CatalogException, IOException;

    QueryResult deleteFile(int fileId, String sessionId)
            throws CatalogException, IOException;

    QueryResult moveFile(int fileId, int folderId, String sessionId) throws CatalogException;

    QueryResult renameFile(int fileId, String newName, String sessionId)
            throws CatalogException, IOException, CatalogIOManagerException;

    QueryResult modifyFile(int fileId, ObjectMap parameters, String sessionId)
            throws CatalogException;

    QueryResult<File> getFileParent(int fileId, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<File> getFile(int fileId, String sessionId)
            throws CatalogException;

    QueryResult<File> getFile(int fileId, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<File> getAllFiles(int studyId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream downloadFile(int fileId, String sessionId)
            throws IOException, CatalogException;

    DataInputStream downloadFile(int fileId, int start, int limit, String sessionId)    //TODO: start & limit does not work
            throws IOException, CatalogException;

    DataInputStream grepFile(int fileId, String pattern, boolean ignoreCase, boolean multi, String sessionId)
            throws IOException, CatalogException;

    /*Require role admin*/
    QueryResult<File> searchFile(QueryOptions query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> searchFile(int studyId, QueryOptions query, String sessionId) throws CatalogException;

    QueryResult<File> searchFile(int studyId, QueryOptions query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Dataset> createDataset(int studyId, String name, String description, List<Integer> files,
                                       Map<String, Object> attributes, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Dataset> getDataset(int dataSetId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult refreshFolder(int folderId, String sessionId)
            throws CatalogDBException, IOException;

    //    public QueryResult<Analysis> createAnalysis(int studyId, String name, String alias, String description, String sessionId) throws CatalogManagerException {
    //        checkParameter(name, "name");
    //        checkAlias(alias, "alias");
    //        checkParameter(sessionId, "sessionId");
    //
    //        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
    //        if (!getStudyAcl(userId, studyId).isWrite()) {
    //            throw new CatalogManagerException("Permission denied. Can't write in this study");
    //        }
    //        Analysis analysis = new Analysis(name, alias, TimeUtils.getTime(), userId, description);
    //
    //        String ownerId = catalogDBAdaptor.getStudyOwnerId(studyId);
    //        catalogDBAdaptor.updateUserLastActivity(ownerId);
    //        return catalogDBAdaptor.createAnalysis(studyId, analysis);
    //    }
    //
    //
    //    public QueryResult<Analysis> getAnalysis(int analysisId, String sessionId) throws CatalogManagerException {
    //        checkParameter(sessionId, "sessionId");
    //        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
    //        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
    //
    //        if (!getStudyAcl(userId, studyId).isRead()) {
    //            throw new CatalogManagerException("Permission denied. User can't read from this study"); //TODO: Should Analysis have ACL?
    //        }
    //
    //        return catalogDBAdaptor.getAnalysis(analysisId);
    //
    //    }
    //
    //    public QueryResult<Analysis> getAllAnalysis(int studyId, String sessionId) throws CatalogManagerException {
    //        checkParameter(sessionId, "sessionId");
    //        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
    //
    //        if (!getStudyAcl(userId, studyId).isRead()) {
    //            throw new CatalogManagerException("Permission denied. Can't read from this study"); //TODO: Should Analysis have ACL?
    //        }
    //
    //        return catalogDBAdaptor.getAllAnalysis(studyId);
    //    }
    //
    //    public QueryResult modifyAnalysis(int analysisId, ObjectMap parameters, String sessionId) throws CatalogManagerException {
    //        checkParameter(sessionId, "sessionId");
    //        checkObj(parameters, "Parameters");
    //        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
    //        int studyId = catalogDBAdaptor.getStudyIdByAnalysisId(analysisId);
    //
    //        for (String s : parameters.keySet()) {
    //            if (!s.matches("name|date|description|attributes")) {
    //                throw new CatalogManagerException("Parameter '" + s + "' can't be changed");
    //            }
    //        }
    //
    //        if (!getStudyAcl(userId, studyId).isWrite()) {
    //            throw new CatalogManagerException("Permission denied. Can't modify this analysis"); //TODO: Should Analysis have ACL?
    //        }
    //
    //        String ownerId = catalogDBAdaptor.getAnalysisOwner(analysisId);
    //        catalogDBAdaptor.updateUserLastActivity(ownerId);
    //        return catalogDBAdaptor.modifyAnalysis(analysisId, parameters);
    //    }
    int getStudyIdByJobId(int jobId) throws CatalogDBException;

    //    @Deprecated
    //    public QueryResult<Job> createJob(int studyId, String name, String toolName, String description, String commandLine,
    //                                      int outDirId, int tmpOutDirId, List<Integer> inputFiles, String sessionId)
    //            throws CatalogManagerException, CatalogIOManagerException {
    //        QueryOptions options = new QueryOptions("include", Arrays.asList("id", "type", "path"));
    //        File tmpOutDir = catalogDBAdaptor.getFile(tmpOutDirId, options).getResult().get(0);     //TODO: Create tmpOutDir outside
    //        return createJob(studyId, name, toolName, description, commandLine, outDirId, getFileUri(tmpOutDir), inputFiles, sessionId);
    //    }
    QueryResult<Job> createJob(int studyId, String name, String toolName, String description, String commandLine,
                               URI tmpOutDirUri, int outDirId, List<Integer> inputFiles,
                               Map<String, Object> attributes, Map<String, Object> resourceManagerAttributes,
                               Job.Status status, QueryOptions options, String sessionId)
            throws CatalogException;

    URI createJobOutDir(int studyId, String dirName, String sessionId)
            throws CatalogException;

    QueryResult<ObjectMap> incJobVisites(int jobId, String sessionId) throws CatalogException;

    QueryResult deleteJob(int jobId, String sessionId)
            throws CatalogException;

    QueryResult<Job> getJob(int jobId, QueryOptions options, String sessionId) throws IOException, CatalogIOManagerException, CatalogException;

    QueryResult<Job> getUnfinishedJobs(String sessionId) throws CatalogException;

    QueryResult<Job> getAllJobs(int studyId, String sessionId) throws CatalogException;

    QueryResult modifyJob(int jobId, ObjectMap parameters, String sessionId) throws CatalogException;

    QueryResult<Sample> createSample(int studyId, String name, String source, String description,
                                     Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<Sample> getSample(int sampleId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Sample> getAllSamples(int studyId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                               String description, Map<String, Object> attributes,
                                               List<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> createVariableSet(int studyId, String name, Boolean unique,
                                               String description, Map<String, Object> attributes,
                                               Set<Variable> variables, String sessionId)
            throws CatalogException;

    QueryResult<VariableSet> getVariableSet(int variableSet, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<AnnotationSet> annotateSample(int sampleId, String id, int variableSetId,
                                              Map<String, Object> annotations,
                                              Map<String, Object> attributes,
                                              String sessionId) throws CatalogException;

    QueryResult<Tool> createTool(String alias, String description, Object manifest, Object result,
                                 String path, boolean openTool, String sessionId) throws CatalogException;

    QueryResult<Tool> getTool(int id, String sessionId) throws CatalogException;
}
