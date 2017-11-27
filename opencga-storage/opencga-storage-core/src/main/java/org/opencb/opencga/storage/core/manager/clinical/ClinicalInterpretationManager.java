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

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.clinical.Comment;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.clinical.ReportedVariant;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.clinical.ClinicalInterpretationQueryParam;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ReportedVariantIterator;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.StorageManager;

import java.io.IOException;
import java.util.*;

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
        // Check permissions
        if (!isAdmin(token)) {
            checkQuery(query, token);
        }

        return clinicalVariantEngine.query(query, options, "");
    }

    public QueryResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        if (!isAdmin(token)) {
            checkQuery(query, token);
        }

        return clinicalVariantEngine.interpretationQuery(query, options, "");
    }

    public ReportedVariantIterator iterator(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        if (!isAdmin(token)) {
            checkQuery(query, token);
        }

        return clinicalVariantEngine.iterator(query, options, "");
    }

    public void addInterpretationComment(long interpretationId, Comment comment, String token)
            throws IOException, ClinicalVariantException {
        clinicalVariantEngine.addInterpretationComment(interpretationId, comment, "");
    }

    public void addReportedVariantComment(long interpretationId, String variantId, Comment comment, String token)
            throws IOException, ClinicalVariantException {
        clinicalVariantEngine.addReportedVariantComment(interpretationId, variantId, comment, "");
    }

    /*--------------------------------------------------------------------------*/
    /*                    P R I V A T E     M E T H O D S                       */
    /*--------------------------------------------------------------------------*/

    private List<String> getAllowedClinicalAnalysisIdList(Query query, String token) throws CatalogException {
        List<String> caIds = getClinicalAnalysisIdList(token);
        if (ListUtils.isEmpty(caIds)) {
            return Collections.emptyList();
        }

        if (StringUtils.isNotEmpty(query.getString(ClinicalInterpretationQueryParam.CLINICAL_ANALYSIS_ID.key(), ""))) {
            return caIds;
        } else {
            List<String> inputCaIds = Arrays.asList(StringUtils.split(
                    query.getString(ClinicalInterpretationQueryParam.CLINICAL_ANALYSIS_ID.key()), ","));
            Set<String> outputCaIds = new HashSet<>();

            for (String userCaId : caIds) {
                for (String inputCaId : inputCaIds) {
                    if (userCaId.equals(inputCaId)) {
                        outputCaIds.add(userCaId);
                    }
                }
            }
            return Arrays.asList((String[]) outputCaIds.toArray());
        }
    }

    private List<String> getClinicalAnalysisIdList(String token) throws CatalogException {
        List<String> ids = new ArrayList<>();
        QueryResult<Project> projectQueryResult = catalogManager.getProjectManager()
                .get(new Query(), QueryOptions.empty(), token);

        for (Project project: projectQueryResult.getResult()) {
            for (Study study: project.getStudies()) {
                QueryResult<ClinicalAnalysis> caQueryResult = catalogManager.getClinicalAnalysisManager()
                        .get(study.getAlias(), new Query(), QueryOptions.empty(), token);
                for (ClinicalAnalysis ca: caQueryResult.getResult()) {
                    ids.add(String.valueOf(ca.getId()));
                }
            }
        }
        return ids;
    }

    private List<ClinicalAnalysis> getAllowedClinicalAnalysisList(String token) throws CatalogException {
        List<ClinicalAnalysis> cas = new ArrayList<>();
        QueryResult<Project> projectQueryResult = catalogManager.getProjectManager().get(new Query(), QueryOptions.empty(), token);

        for (Project project: projectQueryResult.getResult()) {
            for (Study study: project.getStudies()) {
                QueryResult<ClinicalAnalysis> caQueryResult = catalogManager.getClinicalAnalysisManager()
                        .get(study.getAlias(), new Query(), QueryOptions.empty(), token);
                cas.addAll(caQueryResult.getResult());
            }
        }
        return cas;
    }

    private boolean isAdmin(String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);

        boolean isAdmin = false;
        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(new Query(), QueryOptions.empty(), "token");
        for (Study study: studyQueryResult.getResult()) {
            for (Group group: study.getGroups()) {
                if ("@admins".equals(group.getName())) {
                    if (group.getUserIds().contains(userId)) {
                        isAdmin = true;
                    } else {
                        isAdmin = false;
                    }
                    break;
                }
            }
        }

        return isAdmin;
    }

    private void checkQuery(Query query, String token) throws CatalogException {
        // Get all allowed clinical analysis for this user (token)
        List<ClinicalAnalysis> allowedCas = getAllowedClinicalAnalysisList(token);

        // Check Clinical Analysis ID
        boolean allowed;
        String value = query.getString(ClinicalInterpretationQueryParam.CLINICAL_ANALYSIS_ID.key());
        if (StringUtils.isNotEmpty(value)) {
            List<String> caIds = Arrays.asList((String[]) StringUtils.split(value));
            for (String caId: caIds) {
                allowed = false;
                for (ClinicalAnalysis ca : allowedCas) {
                    if (ca.getId() == Long.parseLong(caId)) {
                        allowed = true;
                        break;
                    }
                }
                if (!allowed) {
                    throw new CatalogAuthorizationException("Access not authorized to clinical analysis '" + caId + "'");
                }
            }
        }

        // Check Family names
        value = query.getString(ClinicalInterpretationQueryParam.FAMIY_NAME.key());
        if (StringUtils.isNotEmpty(value)) {
            List<String> familyNames = Arrays.asList((String[]) StringUtils.split(value));
            for (String familyName: familyNames) {
                allowed = false;
                for (ClinicalAnalysis ca : allowedCas) {
                    if (ca.getFamily() != null && ca.getFamily().getName() != null
                            && ca.getFamily().getName().equals(familyName)) {
                        allowed = true;
                        break;
                    }
                }
                if (!allowed) {
                    throw new CatalogAuthorizationException("Access not authorized to family '" + familyName + "'");
                }
            }
        }

        // Check Subject names
        value = query.getString(ClinicalInterpretationQueryParam.SUBJECT_NAME.key());
        if (StringUtils.isNotEmpty(value)) {
            List<String> subjectNames = Arrays.asList((String[]) StringUtils.split(value));
            for (String subjectName: subjectNames) {
                allowed = false;
                for (ClinicalAnalysis ca : allowedCas) {
                    if (ca.getSubjects() != null && ListUtils.isNotEmpty(ca.getSubjects())) {
                        for (Individual individual: ca.getSubjects()) {
                            if (individual.getName() != null && individual.getName().equals(subjectName)) {
                                allowed = true;
                                break;
                            }
                        }
                    }
                    if (allowed) {
                        break;
                    }
                }
                if (!allowed) {
                    throw new CatalogAuthorizationException("Access not authorized to subject '" + subjectName + "'");
                }
            }
        }
    }
}
