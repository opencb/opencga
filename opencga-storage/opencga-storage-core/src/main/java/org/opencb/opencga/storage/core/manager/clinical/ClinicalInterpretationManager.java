/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.manager.clinical;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ReportedVariant;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.StorageManager;

import java.io.IOException;

public class ClinicalInterpretationManager extends StorageManager {

    private ClinicalVariantEngine clinicalVariantEngine;

    public ClinicalInterpretationManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);
    }

    @Override
    public void testConnection() throws StorageEngineException {
    }

    public QueryResult<ReportedVariant> index(String token) throws IOException, ClinicalVariantException {
        return null;
    }

    public QueryResult<ReportedVariant> index(String study, String token) throws IOException, ClinicalVariantException, CatalogException {
        ClinicalAnalysisManager clinicalAnalysisManager = catalogManager.getClinicalAnalysisManager();
        DBIterator<ClinicalAnalysis> iterator = clinicalAnalysisManager.iterator(study, new Query(), QueryOptions.empty(), token);
        return null;
    }

    public QueryResult<ReportedVariant> query(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException {

        return clinicalVariantEngine.query(query, options, "");
    }

}
