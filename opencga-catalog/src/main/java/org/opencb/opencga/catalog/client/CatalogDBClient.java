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
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Project;
import org.opencb.opencga.catalog.models.User;

import java.io.IOException;

/**
 * Created by jacobo on 10/02/15.
 */
@Deprecated
public class CatalogDBClient implements org.opencb.opencga.catalog.client.CatalogClient {

    private final CatalogManager catalogManager;
    private final UserManager catalogUserClient;
    private final ProjectManager catalogProjectClient;
    private String sessionId;
    private String userId;
    private long projectId;
    private long studyId;

    public CatalogDBClient(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        this.catalogUserClient = new UserManager();
        this.catalogProjectClient = new ProjectManager();
    }

    public CatalogDBClient(CatalogManager catalogManager, String sessionId) {
        this(catalogManager);
        this.sessionId = sessionId;
        this.userId = catalogManager.getUserIdBySessionId(sessionId);
    }

    public CatalogDBClient(CatalogManager catalogManager, String userId, String password, String sessionIp)
            throws CatalogException, IOException {
        this(catalogManager);
        this.userId = userId;
        this.sessionId = catalogManager.login(userId, password, sessionIp).getResult().get(0).getString("sessionId");
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public String getUserId() {
        return userId;
    }

    @Override
    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public org.opencb.opencga.catalog.client.CatalogClient.CatalogUserClient users() {
        return catalogUserClient;
    }

    @Override
    public CatalogUserClient users(String userId) {
        this.userId = userId;
        return catalogUserClient;
    }

    @Override
    public org.opencb.opencga.catalog.client.CatalogClient.CatalogProjectClient projects() {
        return catalogProjectClient;
    }

    @Override
    public CatalogProjectClient projects(long projectId) {
        this.projectId = projectId;
        return null;
    }

    @Override
    public org.opencb.opencga.catalog.client.CatalogClient.CatalogStudyClient studies() {
        return null;
    }

    @Override
    public CatalogStudyClient studies(long studyId) {
        return null;
    }

    @Override
    public org.opencb.opencga.catalog.client.CatalogClient.CatalogFileClient files() {
        return null;
    }

    @Override
    public CatalogFileClient files(long fileId) {
        return null;
    }

    @Override
    public org.opencb.opencga.catalog.client.CatalogClient.CatalogJobClient jobs() {
        return null;
    }

    @Override
    public CatalogJobClient jobs(long jobId) {
        return null;
    }

    @Override
    public void close() throws CatalogException {
        catalogManager.logout(userId, sessionId);
//        catalogManager.close();
    }

    public class UserManager implements org.opencb.opencga.catalog.client.CatalogClient.CatalogUserClient {

        @Override
        public String getUserId(String sessionId) {
            return catalogManager.getUserIdBySessionId(sessionId);
        }

        @Override
        public QueryResult<User> create(QueryOptions options) throws CatalogException {
            return create(
                    options.getString("id"),
                    options.getString("name"),
                    options.getString("email"),
                    options.getString("password"),
                    options.getString("organization"),
                    options
            );
        }

        @Override
        public QueryResult<User> create(String id, String name, String email, String password, String organization, QueryOptions options)
                throws CatalogException {
            return catalogManager.createUser(id, name, email, password, organization, 0L, options, getSessionId());
        }

        @Override
        public QueryResult<User> read(QueryOptions options) throws CatalogException {
            return catalogManager.getUser(userId, (options != null) ? options.getString("lastActivity") : null, options, getSessionId());
        }

        @Override
        public QueryResult<User> readAll(QueryOptions options) {
            return null;
        }

        @Override
        public QueryResult<User> update(QueryOptions params) throws CatalogException {
            return catalogManager.modifyUser(userId, params, sessionId);
        }

        @Override
        public QueryResult<User> delete() throws CatalogException {
            catalogManager.deleteUser(userId, null, sessionId);
            return new QueryResult<>("deleteUser");
        }

        @Override
        public QueryResult<User> changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
            catalogManager.deleteUser(userId, null, sessionId);
            return new QueryResult<>("changePassword");
        }
    }

    class ProjectManager implements org.opencb.opencga.catalog.client.CatalogClient.CatalogProjectClient {

        @Override
        public String getUserId(long projectId) throws CatalogException {
            return null;
        }

        @Override
        public Long getProjectId(String projectId) throws CatalogException {
            return catalogManager.getProjectId(projectId);
        }

        @Override
        public QueryResult<Project> create(QueryOptions options) throws CatalogException {
            return create(options.getString("ownerId", userId),
                    options.getString("name"),
                    options.getString("alias"),
                    options.getString("description"),
                    options.getString("organization"),
                    options);
        }

        @Override
        public QueryResult<Project> create(String ownerId, String name, String alias, String description, String organization,
                                           QueryOptions options) throws CatalogException {
            return catalogManager.createProject(name, alias, description, organization, options, sessionId);
        }

        @Override
        public QueryResult<Project> read(QueryOptions options) throws CatalogException {
            return null;
        }

        @Override
        public QueryResult<Project> readAll(QueryOptions options) throws CatalogException {
            return null;
        }

        @Override
        public QueryResult<Project> update(QueryOptions params) throws CatalogException {
            return null;
        }

        @Override
        public QueryResult<Project> delete() throws CatalogException {
            return null;
        }
    }
}
