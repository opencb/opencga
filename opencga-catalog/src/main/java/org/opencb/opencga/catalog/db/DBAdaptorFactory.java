/*
 * Copyright 2015-2020 OpenCB
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

import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Configuration;

import java.util.Map;

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
     * Installs the catalog database with their corresponding indexes.
     *
     * @param configuration Configuration of catalog.
     * @throws CatalogException if there is any problem with the installation.
     */
    void installCatalogDB(Configuration configuration) throws CatalogException;

    /**
     * Creates the indexes needed to make queries faster.
     *
     * @param uniqueIndexesOnly boolean indicating whether to index unique indexes only.
     * @throws CatalogDBException if there is any problem creating the indexes.
     */
    void createIndexes(boolean uniqueIndexesOnly) throws CatalogDBException;

    String getCatalogDatabase(String prefix);

    boolean getDatabaseStatus();

    /**
     * Removes the catalog database.
     *
     * @throws CatalogDBException if there is a problem during the removal.
     */
    void deleteCatalogDB() throws CatalogDBException;

    void close();

    MigrationDBAdaptor getMigrationDBAdaptor();

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

    PanelDBAdaptor getCatalogPanelDBAdaptor();

    FamilyDBAdaptor getCatalogFamilyDBAdaptor();

    ClinicalAnalysisDBAdaptor getClinicalAnalysisDBAdaptor();

    InterpretationDBAdaptor getInterpretationDBAdaptor();

    Map<String, MongoDBCollection> getMongoDBCollectionMap();
}
