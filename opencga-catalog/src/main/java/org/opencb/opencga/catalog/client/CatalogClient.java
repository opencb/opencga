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

package org.opencb.opencga.catalog.client;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;

import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 10/02/15.
 */
public interface CatalogClient {

    String getSessionId();

    void setSessionId(String sessionId);

    String getUserId();

    void setUserId(String userId);

    CatalogUserClient users();

    CatalogUserClient users(String userId);

    CatalogProjectClient projects();

    CatalogProjectClient projects(int projectId);

    CatalogStudyClient studies();

    CatalogStudyClient studies(int studyId);

    CatalogFileClient files();

    CatalogFileClient files(int fileId);

    CatalogJobClient jobs();

    CatalogJobClient jobs(int jobId);

    void close() throws CatalogException;

    interface CatalogResourceClient<R> {
        QueryResult<R> create(QueryOptions options) throws CatalogException;

        QueryResult<R> read(QueryOptions options) throws CatalogException;

        QueryResult<R> readAll(QueryOptions options) throws CatalogException;

        QueryResult<R> update(QueryOptions options) throws CatalogException;

        QueryResult<R> delete() throws CatalogException;
    }

    interface CatalogUserClient extends CatalogResourceClient<User> {
        String getUserId(String sessionId);

        QueryResult<User> update(QueryOptions options) throws CatalogException;

        QueryResult<User> read(QueryOptions options) throws CatalogException;

        QueryResult<User> create(String id, String name, String email, String password, String organization,
                                 QueryOptions options) throws CatalogException;

        QueryResult<User> changePassword(String userId, String oldPassword, String newPassword) throws CatalogException;
    }

    interface CatalogProjectClient extends CatalogResourceClient<Project> {
        String getUserId(int projectId) throws CatalogException;

        Integer getProjectId(String projectId) throws CatalogException;

        QueryResult<Project> create(String ownerId, String name, String alias, String description,
                                           String organization, QueryOptions options) throws CatalogException;
    }

    interface CatalogStudyClient extends CatalogResourceClient<Study> {
        String getUserId(int studyId) throws CatalogException;

        Integer getProjectId(int studyId) throws CatalogException;

        Integer getStudyId(String studyId) throws CatalogException;

        QueryResult<Study> create(int projectId, String name, String alias, Study.Type type,
                                  String creatorId, String creationDate, String description, String status,
                                  String cipher, String uriScheme, Map<String, Object> stats,
                                  Map<String, Object> attributes) throws CatalogException;

        QueryResult<Study> share(int studyId, AclEntry acl) throws CatalogException;
    }

    interface CatalogFileClient extends CatalogResourceClient<File> {
        String getUserId(int fileId) throws CatalogException;

        Integer getProjectId(int fileId) throws CatalogException;

        Integer getStudyId(int fileId) throws CatalogException;

        Integer getFileId(String fileId) throws CatalogException;

        QueryResult<File> create(int studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                        String ownerId, String creationDate, String description, File.Status status,
                                        long diskUsage, int experimentId, List<Integer> sampleIds, int jobId,
                                        Map<String, Object> stats, Map<String, Object> attributes,
                                        boolean parents) throws CatalogException;
    }

    interface CatalogJobClient extends CatalogResourceClient<Job> {
        Integer getStudyId(int jobId);

        QueryResult<Job> visit(int jobId);
    }

    interface CatalogSampleClient extends CatalogResourceClient<Project> {
        Integer getProjectId(String projectId);

        QueryResult<Annotation> annotate(int sampleId);
    }

    interface CatalogExperimentClient extends CatalogResourceClient<Project> {
        Integer getProjectId(String projectId);
    }

}
