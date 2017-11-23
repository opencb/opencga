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
import org.opencb.opencga.core.models.Project;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.core.models.clinical.Comment;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.ReportedVariant;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ReportedVariantIterator;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.StorageManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClinicalInterpretationManager extends StorageManager {

    private ClinicalAnalysisManager clinicalAnalysisManager;
    private ClinicalVariantEngine clinicalVariantEngine;

    public ClinicalInterpretationManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);

        clinicalAnalysisManager = catalogManager.getClinicalAnalysisManager();
    }

    @Override
    public void testConnection() throws StorageEngineException {
    }

    public QueryResult<ReportedVariant> index(String token) throws IOException, ClinicalVariantException {
        return null;
    }

    public QueryResult<ReportedVariant> index(String study, String token) throws IOException, ClinicalVariantException, CatalogException {
        DBIterator<ClinicalAnalysis> iterator = clinicalAnalysisManager.iterator(study, new Query(), QueryOptions.empty(), token);
        return null;
    }

    public QueryResult<ReportedVariant> query(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException, CatalogException {

        QueryResult<Project> projectQueryResult = catalogManager.getProjectManager().get(new Query(), QueryOptions.empty(), token);
        List<String> studyIds = new ArrayList<>();
        for (Project project : projectQueryResult.getResult()) {
            for (Study study : project.getStudies()) {
                studyIds.add(String.valueOf(study.getId()));
            }
        }

        for (String studyId : studyIds) {

        }

        return clinicalVariantEngine.query(query, options, "");
    }

    public QueryResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException {

        return clinicalVariantEngine.interpretationQuery(query, options, "");
    }

    public ReportedVariantIterator iterator(Query query, QueryOptions options, String toten) throws IOException, ClinicalVariantException {
        return clinicalVariantEngine.iterator(query, options, "");
    }

    public void addInterpretationComment(long interpretationId, Comment comment, String toten)
            throws IOException, ClinicalVariantException {
        clinicalVariantEngine.addInterpretationComment(interpretationId, comment, "");
    }

    public void addReportedVariantComment(long interpretationId, String variantId, Comment comment, String toten)
            throws IOException, ClinicalVariantException {
        clinicalVariantEngine.addReportedVariantComment(interpretationId, variantId, comment, "");
    }
}
