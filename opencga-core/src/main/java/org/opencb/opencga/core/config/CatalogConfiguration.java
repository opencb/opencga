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

package org.opencb.opencga.core.config;

/**
 * Created by imedina on 28/08/15.
 */
public class CatalogConfiguration {

//    OPENCGA.CATALOG.DB.HOSTS        = ${OPENCGA.CATALOG.DB.HOSTS}
//    OPENCGA.CATALOG.DB.DATABASE    = ${OPENCGA.CATALOG.DB.DATABASE}
//    OPENCGA.CATALOG.DB.USER        = ${OPENCGA.CATALOG.DB.USER}
//    OPENCGA.CATALOG.DB.PASSWORD    = ${OPENCGA.CATALOG.DB.PASSWORD}
//    OPENCGA.CATALOG.DB.AUTHENTICATION.DB =  ${OPENCGA.CATALOG.DB.AUTHENTICATION_DATABASE}
//
//    OPENCGA.CATALOG.MAIN.ROOTDIR        = ${OPENCGA.CATALOG.ROOTDIR}
//    OPENCGA.CATALOG.JOBS.ROOTDIR        = ${OPENCGA.CATALOG.JOBS.ROOTDIR}
//
//    CATALOG.MAIL.USER    = ${OPENCGA.CATALOG.MAIL.USER}
//    CATALOG.MAIL.PASSWORD = ${OPENCGA.CATALOG.MAIL.PASSWORD}
//    CATALOG.MAIL.HOST    = ${OPENCGA.CATALOG.MAIL.HOST}
//    CATALOG.MAIL.PORT    = ${OPENCGA.CATALOG.MAIL.PORT}

    /**
     * Credentials for the Catalog database
     */
    private DatabaseCredentials database;

    private String usersData;

    private String jobsData;

    public CatalogConfiguration() {
    }

    public CatalogConfiguration(DatabaseCredentials database, String usersData, String jobsData) {
        this.database = database;
        this.usersData = usersData;
        this.jobsData = jobsData;
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public void setDatabase(DatabaseCredentials database) {
        this.database = database;
    }

    public String getUsersData() {
        return usersData;
    }

    public void setUsersData(String usersData) {
        this.usersData = usersData;
    }

    public String getJobsData() {
        return jobsData;
    }

    public void setJobsData(String jobsData) {
        this.jobsData = jobsData;
    }
}
