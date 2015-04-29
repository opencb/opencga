/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.catalog.db.api;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.beans.*;
import org.opencb.opencga.catalog.db.CatalogDBException;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public abstract class CatalogDBAdaptor
        implements CatalogUserDBAdaptor, CatalogStudyDBAdaptor, CatalogFileDBAdaptor, CatalogJobDBAdaptor, CatalogSamplesDBAdaptor {

    protected Logger logger;

    protected long startQuery(){
        return System.currentTimeMillis();
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result) throws CatalogDBException {
        return endQuery(queryId, startTime, result, null, null);
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime) throws CatalogDBException {
        return endQuery(queryId, startTime, Collections.<T>emptyList(), null, null);
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime, QueryResult<T> result)
            throws CatalogDBException {
        long end = System.currentTimeMillis();
        result.setId(queryId);
        result.setDbTime((int)(end-startTime));
        if(result.getErrorMsg() != null && !result.getErrorMsg().isEmpty()){
            throw new CatalogDBException(result.getErrorMsg());
        }
        return result;
    }

    protected <T> QueryResult<T> endQuery(String queryId, long startTime, List<T> result,
                                          String errorMessage, String warnMessage) throws CatalogDBException {
        long end = System.currentTimeMillis();
        if(result == null){
            result = new LinkedList<>();
        }
        int numResults = result.size();
        QueryResult<T> queryResult = new QueryResult<>(queryId, (int) (end - startTime), numResults, numResults,
                warnMessage, errorMessage, result);
        if(errorMessage != null && !errorMessage.isEmpty()){
            throw new CatalogDBException(queryResult.getErrorMsg());
        }
        return queryResult;
    }


    public abstract void disconnect();

    public abstract CatalogUserDBAdaptor getCatalogUserDBAdaptor();
    public abstract CatalogStudyDBAdaptor getCatalogStudyDBAdaptor();
    public abstract CatalogFileDBAdaptor getCatalogFileDBAdaptor();
    public abstract CatalogSamplesDBAdaptor getCatalogSamplesDBAdaptor();
    public abstract CatalogJobDBAdaptor getCatalogJobDBAdaptor();
//
//    /**
//     * User methods
//     * ***************************
//     */
//    public abstract boolean checkUserCredentials(String userId, String sessionId);
//
//    public abstract boolean userExists(String userId);
//
//    public abstract QueryResult<User> createUser(String userId, String userName, String email, String password, String organization, QueryOptions options)
//            throws CatalogDBException;
//
//    public abstract QueryResult<User> insertUser(User user, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Integer> deleteUser(String userId) throws CatalogDBException;
//
//    public abstract QueryResult<ObjectMap> login(String userId, String password, Session session) throws CatalogDBException;
//
//    public abstract QueryResult logout(String userId, String sessionId) throws CatalogDBException;
//
//    public abstract QueryResult<ObjectMap> loginAsAnonymous(Session session) throws CatalogDBException;
//
//    public abstract QueryResult logoutAnonymous(String sessionId) throws CatalogDBException;
//
//    public abstract QueryResult<User> getUser(String userId, QueryOptions options, String lastActivity) throws CatalogDBException;
//
//    public abstract QueryResult changePassword(String userId, String oldPassword, String newPassword) throws CatalogDBException;
//
//    public abstract QueryResult changeEmail(String userId, String newEmail) throws CatalogDBException;
//
//    public abstract void updateUserLastActivity(String userId) throws CatalogDBException;
//
//    public abstract QueryResult modifyUser(String userId, ObjectMap parameters) throws CatalogDBException;
//
//    public abstract QueryResult resetPassword(String userId, String email, String newCryptPass) throws CatalogDBException;
//
//    public abstract QueryResult getSession(String userId, String sessionId) throws CatalogDBException;
//
//    public abstract String getUserIdBySessionId(String sessionId);
//
//    /**
//     * Project methods
//     * ***************************
//     */
//
//    public abstract QueryResult<Project> createProject(String userId, Project project, QueryOptions options) throws CatalogDBException;
//
//    public abstract boolean projectExists(int projectId);
//
//    public abstract QueryResult<Project> getAllProjects(String userId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Project> getProject(int project, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Integer> deleteProject(int projectId) throws CatalogDBException;
//
//    public abstract QueryResult renameProjectAlias(int projectId, String newProjectName) throws CatalogDBException;
//
//    public abstract QueryResult modifyProject(int projectId, ObjectMap parameters) throws CatalogDBException;
//
//    public abstract int getProjectId(String userId, String projectAlias) throws CatalogDBException;
//
//    public abstract String getProjectOwnerId(int projectId) throws CatalogDBException;
//
//    public abstract QueryResult<Acl> getProjectAcl(int projectId, String userId) throws CatalogDBException;
//
//    public abstract QueryResult setProjectAcl(int projectId, Acl newAcl) throws CatalogDBException;
//
//    /**
//     * Study methods
//     * ***************************
//     */
//
//    public abstract QueryResult<Study> createStudy(int projectId, Study study, QueryOptions options) throws CatalogDBException;
//
//    public abstract boolean studyExists(int studyId);
//
//    public abstract QueryResult<Study> getAllStudies(int projectId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Study> getStudy(int studyId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult renameStudy(int studyId, String newStudyName) throws CatalogDBException;
//
////    public abstract QueryResult modifyStudy(int studyId, Map<String, String> parameters, Map<String, Object> attributes, Map<String, Object> stats) throws CatalogManagerException;
//
//    public abstract void updateStudyLastActivity(int studyId) throws CatalogDBException;
//
//    public abstract QueryResult<ObjectMap> modifyStudy(int studyId, ObjectMap params) throws CatalogDBException;
//
//    public abstract QueryResult<Integer> deleteStudy(int studyId) throws CatalogDBException;
//
//    public abstract int getStudyId(int projectId, String studyAlias) throws CatalogDBException;
//
//    public abstract int getProjectIdByStudyId(int studyId) throws CatalogDBException;
//
//    public abstract String getStudyOwnerId(int studyId) throws CatalogDBException;
//
//    public abstract QueryResult<Acl> getStudyAcl(int projectId, String userId) throws CatalogDBException;
//
//    public abstract QueryResult setStudyAcl(int projectId, Acl newAcl) throws CatalogDBException;
//
//
//    /**
//     * File methods
//     * ***************************
//     */
//
//    // add file to study
//    public abstract QueryResult<File> createFileToStudy(int studyId, File file, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Integer> deleteFile(int fileId) throws CatalogDBException;
//
//    public abstract int getFileId(int studyId, String path) throws CatalogDBException;
//
//    public abstract QueryResult<File> getAllFiles(int studyId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<File> getAllFilesInFolder(int folderId, QueryOptions options) throws CatalogDBException;
//
//    public QueryResult<File> getFile(int fileId) throws CatalogDBException { return getFile(fileId, null); }
//    public abstract QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult setFileStatus(int fileId, File.Status status) throws CatalogDBException;
//
//    public abstract QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogDBException;
//
//    public abstract QueryResult<Object> renameFile(int fileId, String name) throws CatalogDBException;
//
//    public abstract int getStudyIdByFileId(int fileId) throws CatalogDBException;
//
//    public abstract String getFileOwnerId(int fileId) throws CatalogDBException;
//
//    public abstract QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogDBException;
//
//    public abstract QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogDBException;
//
//    public abstract QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Dataset> createDataset(int studyId, Dataset dataset, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Dataset> getDataset(int datasetId, QueryOptions options) throws CatalogDBException;
//
//    public abstract int getStudyIdByDatasetId(int datasetId) throws CatalogDBException;
//
//    /**
//     * Analysis methods
//     * ***************************
//     */
//
////    public abstract QueryResult<Analysis> getAllAnalysis(String userId, String projectAlias, String studyAlias) throws CatalogManagerException;
////    public abstract QueryResult<Analysis> getAllAnalysis(int studyId) throws CatalogManagerException;
////    int getAnalysisId(int studyId, String analysisAlias) throws CatalogManagerException;
////    public abstract QueryResult<Analysis> getAnalysis(int analysisId) throws CatalogManagerException;
////
////    public abstract QueryResult<Analysis> createAnalysis(String userId, String projectAlias, String studyAlias, Analysis analysis) throws CatalogManagerException;
////    public abstract QueryResult<Analysis> createAnalysis(int studyId, Analysis analysis) throws CatalogManagerException;
////
////    public abstract QueryResult modifyAnalysis(int analysisId, ObjectMap parameters) throws CatalogManagerException;
////
////    int getStudyIdByAnalysisId(int analysisId) throws CatalogManagerException;
////    String getAnalysisOwner(int analysisId) throws CatalogManagerException;
//
//    /**
//     * Job methods
//     * ***************************
//     */
//
//    public abstract boolean jobExists(int jobId);
//
//    public abstract QueryResult<Job> createJob(int studyId, Job job, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Integer> deleteJob(int jobId) throws CatalogDBException;
//
//    public abstract QueryResult<Job> getJob(int jobId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Job> getAllJobs(int studyId, QueryOptions options) throws CatalogDBException;
//
//    public abstract String getJobStatus(int jobId, String sessionId) throws CatalogDBException;
//
//    public abstract QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException;
//
//    public abstract QueryResult modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException;
//
//    public abstract int getStudyIdByJobId(int jobId) throws CatalogDBException;
//
//    public abstract QueryResult<Job> searchJob(QueryOptions options) throws CatalogDBException;
//
//    /**
//     * Tool methods
//     * ***************************
//     */
//
//    public abstract QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException;
//
//    public abstract QueryResult<Tool> getTool(int id) throws CatalogDBException;
//
//    public abstract int getToolId(String userId, String toolAlias) throws CatalogDBException;
//
//
////    public abstract QueryResult<Tool> searchTool(QueryOptions options);
//
//    /**
//     * Experiments methods
//     * ***************************
//     */
//
//    public abstract boolean experimentExists(int experimentId);
//
////    QueryResult<Tool> searchTool(QueryOptions options);
//    /**
//     * Samples methods
//     * ***************************
//     */
//
//    public abstract boolean sampleExists(int sampleId);
//
//    public abstract QueryResult<Sample> createSample(int studyId, Sample sample, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Sample> getSample(int sampleId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Sample> getAllSamples(int studyId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<Sample> modifySample(int sampleId, QueryOptions parameters) throws CatalogDBException;
//
//    public abstract QueryResult<Integer> deleteSample(int sampleId) throws CatalogDBException;
//
//    public abstract int getStudyIdBySampleId(int sampleId) throws CatalogDBException;
//
//    public abstract QueryResult<Cohort> createCohort(int studyId, Cohort cohort) throws CatalogDBException;
//
//    public abstract QueryResult<Cohort> getCohort(int cohortId) throws CatalogDBException;
//
//    public abstract int getStudyIdByCohortId(int cohortId) throws CatalogDBException;
//
//    /**
//     * Annotation Methods
//     * ***************************
//     */
//
//    public abstract QueryResult<VariableSet> createVariableSet(int studyId, VariableSet variableSet) throws CatalogDBException;
//
//    public abstract QueryResult<VariableSet> getVariableSet(int variableSetId, QueryOptions options) throws CatalogDBException;
//
//    public abstract QueryResult<AnnotationSet> annotateSample(int sampleId, AnnotationSet annotationSet) throws CatalogDBException;
//
//    public abstract int getStudyIdByVariableSetId(int sampleId) throws CatalogDBException;
//
//    /**
//     * Util methods
//     * ***************************
//     */
////    List<AnalysisPlugin> getUserAnalysis(String sessionId) throws CatalogManagerException, IOException;
////
////    List<Bucket> jsonToBucketList(String json) throws IOException;
////
////    ObjectItem getObjectFromBucket(String userId, String bucketId, Path objectId, String sessionId) throws CatalogManagerException, IOException;
//
//

}
