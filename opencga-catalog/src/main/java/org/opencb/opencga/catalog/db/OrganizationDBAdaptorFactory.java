package org.opencb.opencga.catalog.db;

import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.config.Admin;

import java.util.Map;

public interface OrganizationDBAdaptorFactory extends DBAdaptorFactory {

    /**
     * Initialise meta collection.
     *
     * @param admin Admin information.
     * @throws CatalogException if there is any problem with the installation.
     */
    void initialiseMetaCollection(Admin admin) throws CatalogException;

    /**
     * Creates the indexes needed to make queries faster.
     *
     * @throws CatalogDBException if there is any problem creating the indexes.
     */
    void createIndexes() throws CatalogDBException;

    Map<String, MongoDBCollection> getMongoDBCollectionMap();

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

}
