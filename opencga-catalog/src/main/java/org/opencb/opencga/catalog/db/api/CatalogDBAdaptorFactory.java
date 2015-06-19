package org.opencb.opencga.catalog.db.api;

import org.opencb.opencga.catalog.exceptions.CatalogDBException;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public interface CatalogDBAdaptorFactory {

    /**
     * Says if the catalog database is ready to be used. If false, needs to be initialized
     */
    boolean isCatalogDBReady();

    /**
     * Initializes de Database with the initial structure.
     * @throws CatalogDBException   if there was any problem, or it was already initialized.
     */
    void initializeCatalogDB() throws CatalogDBException;

    void disconnect();

    CatalogUserDBAdaptor getCatalogUserDBAdaptor();

    CatalogStudyDBAdaptor getCatalogStudyDBAdaptor();

    CatalogFileDBAdaptor getCatalogFileDBAdaptor();

    CatalogSampleDBAdaptor getCatalogSampleDBAdaptor();

    CatalogIndividualDBAdaptor getCatalogIndividualDBAdaptor();

    CatalogJobDBAdaptor getCatalogJobDBAdaptor();
}
