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

import org.opencb.opencga.catalog.db.api2.*;
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
     *
     * @throws CatalogDBException if there was any problem, or it was already initialized.
     */
    void initializeCatalogDB() throws CatalogDBException;

    void close();

    CatalogUserDBAdaptor getCatalogUserDBAdaptor();

    CatalogProjectDBAdaptor getCatalogProjectDbAdaptor();

    CatalogStudyDBAdaptor getCatalogStudyDBAdaptor();

    CatalogFileDBAdaptor getCatalogFileDBAdaptor();

    CatalogSampleDBAdaptor getCatalogSampleDBAdaptor();

    CatalogIndividualDBAdaptor getCatalogIndividualDBAdaptor();

    CatalogJobDBAdaptor getCatalogJobDBAdaptor();

    CatalogAuditDBAdaptor getCatalogAuditDbAdaptor();
}
