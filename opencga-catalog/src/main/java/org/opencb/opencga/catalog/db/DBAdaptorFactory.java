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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Admin;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.organizations.Organization;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface DBAdaptorFactory extends AutoCloseable {

    /**
     * Says if the catalog database is ready to be used. If false, needs to be initialized.
     * @return boolean
     * @throws CatalogDBException CatalogDBException.
     */
    boolean isCatalogDBReady() throws CatalogDBException;

    /**
     * Create all collections for the database.
     *
     * @param configuration Configuration of catalog.
     * @throws CatalogException if there is any problem with the installation.
     */
    void createAllCollections(Configuration configuration) throws CatalogException;

    /**
     * Initialise meta collection.
     *
     * @param admin Admin information.
     * @throws CatalogException if there is any problem with the installation.
     */
    void initialiseMetaCollection(Admin admin) throws CatalogException;

    default String getCatalogDatabase(String prefix, String organization) {
        String dbPrefix = StringUtils.isEmpty(prefix) ? "opencga" : prefix;
        dbPrefix = dbPrefix.endsWith("_") ? dbPrefix : dbPrefix + "_";
        return (dbPrefix + "catalog_" + organization).toLowerCase();
    }

    boolean getDatabaseStatus() throws CatalogDBException;

    /**
     * Removes the catalog database.
     *
     * @throws CatalogDBException if there is a problem during the removal.
     */
    void deleteCatalogDB() throws CatalogDBException;

    void close();

    void createIndexes(String organization) throws CatalogDBException;

    List<String> getOrganizationIds() throws CatalogDBException;

    MigrationDBAdaptor getMigrationDBAdaptor(String organization) throws CatalogDBException;

    OpenCGAResult<Organization> createOrganization(Organization organization, QueryOptions options, String userId) throws CatalogException;

    void deleteOrganization(Organization organization) throws CatalogDBException;

    NoteDBAdaptor getCatalogNoteDBAdaptor(String organization) throws CatalogDBException;

    OrganizationDBAdaptor getCatalogOrganizationDBAdaptor(String organization) throws CatalogDBException;

    UserDBAdaptor getCatalogUserDBAdaptor(String organization) throws CatalogDBException;

    ProjectDBAdaptor getCatalogProjectDbAdaptor(String organization) throws CatalogDBException;

    StudyDBAdaptor getCatalogStudyDBAdaptor(String organization) throws CatalogDBException;

    FileDBAdaptor getCatalogFileDBAdaptor(String organization) throws CatalogDBException;

    SampleDBAdaptor getCatalogSampleDBAdaptor(String organization) throws CatalogDBException;

    IndividualDBAdaptor getCatalogIndividualDBAdaptor(String organization) throws CatalogDBException;

    JobDBAdaptor getCatalogJobDBAdaptor(String organization) throws CatalogDBException;

    AuditDBAdaptor getCatalogAuditDbAdaptor(String organization) throws CatalogDBException;

    CohortDBAdaptor getCatalogCohortDBAdaptor(String organization) throws CatalogDBException;

    PanelDBAdaptor getCatalogPanelDBAdaptor(String organization) throws CatalogDBException;

    FamilyDBAdaptor getCatalogFamilyDBAdaptor(String organization) throws CatalogDBException;

    ClinicalAnalysisDBAdaptor getClinicalAnalysisDBAdaptor(String organization) throws CatalogDBException;

    InterpretationDBAdaptor getInterpretationDBAdaptor(String organization) throws CatalogDBException;

    WorkflowDBAdaptor getWorkflowDBAdaptor(String organization) throws CatalogDBException;
}
