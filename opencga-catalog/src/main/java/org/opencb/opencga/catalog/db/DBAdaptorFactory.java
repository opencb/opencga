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

package org.opencb.opencga.catalog.db;

import org.opencb.opencga.catalog.config.Admin;
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface DBAdaptorFactory {

    /**
     * Says if the catalog database is ready to be used. If false, needs to be initialized.
     * @return boolean
     */
    boolean isCatalogDBReady();

    /**
     * Initializes de Database with the initial structure.
     *
     * @param admin Administrator object containing the encrypted password and the email.
     * @throws CatalogDBException if there was any problem, or it was already initialized.
     */
    void initializeCatalogDB(Admin admin) throws CatalogDBException;

    /**
     * Installs the catalog database with their corresponding indexes.
     *
     * @param catalogConfiguration Configuration of catalog.
     * @throws CatalogException if there is any problem with the installation.
     */
    void installCatalogDB(CatalogConfiguration catalogConfiguration) throws CatalogException;

    /**
     * Creates the indexes needed to make queries faster.
     *
      * @throws CatalogDBException if there is any problem creating the indexes.
     */
    void createIndexes() throws CatalogDBException;

    /**
     * Removes the catalog database.
     *
     * @throws CatalogDBException if there is a problem during the removal.
     */
    void deleteCatalogDB() throws CatalogDBException;

    void close();

    MetaDBAdaptor getCatalogMetaDBAdaptor();

    UserDBAdaptor getCatalogUserDBAdaptor();

    ProjectDBAdaptor getCatalogProjectDbAdaptor();

    StudyDBAdaptor getCatalogStudyDBAdaptor();

    FileDBAdaptor getCatalogFileDBAdaptor();

    SampleDBAdaptor getCatalogSampleDBAdaptor();

    IndividualDBAdaptor getCatalogIndividualDBAdaptor();

    JobDBAdaptor getCatalogJobDBAdaptor();

    AuditDBAdaptor getCatalogAuditDbAdaptor();

    CohortDBAdaptor getCatalogCohortDBAdaptor();

    DatasetDBAdaptor getCatalogDatasetDBAdaptor();

    PanelDBAdaptor getCatalogPanelDBAdaptor();
}
