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

package org.opencb.opencga.catalog.core;

import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.core.CatalogClient;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.CatalogManager;
import org.opencb.opencga.catalog.beans.Project;
import org.opencb.opencga.catalog.beans.User;

import java.io.IOException;

/**
 * Created by jacobo on 10/02/15.
 */
public class CatalogDBClient implements CatalogClient {

    private final CatalogManager catalogManager;
    private final CatalogUserClient catalogUserClient;
    private final CatalogProjectClient catalogProjectClient;
    private String sessionId;
    private String userId;
    private int projectId;
    private int studyId;

    public CatalogDBClient(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
        this.catalogUserClient = new CatalogUserClient();
        this.catalogProjectClient = new CatalogProjectClient();
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

    public class CatalogUserClient implements CatalogClient.CatalogUserClient {

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
        public QueryResult<User> create(String id, String name, String email, String password, String organization, QueryOptions options) throws CatalogException {
            return catalogManager.createUser(id, name, email, password, organization, options, getSessionId());
        }

        @Override
        public QueryResult<User> read(QueryOptions options) throws CatalogException {
            return catalogManager.getUser(userId, (options != null)? options.getString("lastActivity") : null, options, getSessionId());
        }

        @Override
        public QueryResult<User> readAll(QueryOptions options) {
            return null;
        }

        @Override
        public QueryResult<User> update(QueryOptions options) throws CatalogException {
            return catalogManager.modifyUser(userId, options, sessionId);
        }

        @Override
        public QueryResult<User> delete() throws CatalogException {
            catalogManager.deleteUser(userId, sessionId);
            return new QueryResult<>("deleteUser");
        }

        @Override
        public QueryResult<User> changePassword(String userId, String oldPassword, String newPassword) throws CatalogException {
            catalogManager.deleteUser(userId, sessionId);
            return new QueryResult<>("changePassword");
        }
    }

    class CatalogProjectClient implements CatalogClient.CatalogProjectClient {

        @Override
        public String getUserId(int projectId) throws CatalogException {
            return null;
        }

        @Override
        public Integer getProjectId(String projectId) throws CatalogException {
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
        public QueryResult<Project> create(String ownerId, String name, String alias, String description, String organization, QueryOptions options) throws CatalogException {
            return catalogManager.createProject(ownerId, name, alias, description, organization, options, sessionId);
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
        public QueryResult<Project> update(QueryOptions options) throws CatalogException {
            return null;
        }

        @Override
        public QueryResult<Project> delete() throws CatalogException {
            return null;
        }
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
    public CatalogUserClient users() {
        return catalogUserClient;
    }

    @Override
    public CatalogClient.CatalogUserClient users(String userId) {
        this.userId = userId;
        return catalogUserClient;
    }

    @Override
    public CatalogProjectClient projects() {
        return catalogProjectClient;
    }

    @Override
    public CatalogClient.CatalogProjectClient projects(int projectId) {
        this.projectId = projectId;
        return null;
    }

    @Override
    public CatalogStudyClient studies() {
        return null;
    }

    @Override
    public CatalogStudyClient studies(int studyId) {
        this.studyId = studyId;
        return null;
    }

    @Override
    public CatalogFileClient files() {
        return null;
    }

    @Override
    public CatalogFileClient files(int fileId) {
        return null;
    }

    @Override
    public CatalogJobClient jobs() {
        return null;
    }

    @Override
    public CatalogJobClient jobs(int jobId) {
        return null;
    }

    @Override
    public void close() throws CatalogException {
        catalogManager.logout(userId, sessionId);
//        catalogManager.close();
    }
}
