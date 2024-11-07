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

package com.zettagenomics.opencga.enterprise.cvdb;

import com.zettagenomics.opencga.enterprise.cvdb.exceptions.CvdbException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.biodata.models.clinical.interpretation.stats.InterpretationSummaryStats;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.catalog.utils.FqnUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CI_STATUS_ID_NAME;

/**
 * Created by jtarraga on 11/11/17.
 */
public class CvdbUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CvdbUtils.class);

    private CvdbUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static DataResult<ClinicalVariant> getClinicalVariant(Query query, QueryOptions queryOptions,
                                                                 ClinicalInterpretationManager clinicalInterpretationManager,
                                                                 CvdbSolrEngine cvdbEngine, String token)
            throws StorageEngineException, CatalogException, IOException, CvdbException {
        String studyId = query.getString(ParamConstants.STUDY_PARAM);
        if (StringUtils.isEmpty(studyId)) {
            throw new CvdbException("Missing study");
        }

        // Get project from study
        JwtPayload jwtPayload = cvdbEngine.getCatalogManager().getUserManager().validateToken(token);
        CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, jwtPayload);
        String organizationId = studyFqn.getOrganizationId();

//        Project project = cvdbEngine.getCatalogManager().getProjectManager().search(organizationId,
//                new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyId),
//                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key()), token).first();

        // Check the filter interpretation status ID
        String interpretationStatusId = null;
        if (query.containsKey(CI_STATUS_ID_NAME)) {
            interpretationStatusId = query.getString(CI_STATUS_ID_NAME);
            query.remove(CI_STATUS_ID_NAME);
        }
        // First, get clinical variants
        OpenCGAResult<ClinicalVariant> result = clinicalInterpretationManager.get(query, queryOptions, token);

        // Then, set summary for those clinical variants
        OpenCGAResult<Project> allProjects = clinicalInterpretationManager.getCatalogManager().getProjectManager()
                .search(organizationId, new Query(), new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ID.key()), token);
        for (Project project : allProjects.getResults()) {
            String projectId = FqnUtils.getProject(project.getFqn());
            if (!cvdbEngine.existCollections(projectId)) {
                LOGGER.info("No CVDB collections were found for project ID '{}'", projectId);
            } else {
                LOGGER.info("Getting summary stats from project ID '{}'", projectId);
                for (ClinicalVariant cv : result.getResults()) {
                    DataResult<ClinicalVariantSummaryStats> summaryStatsResult = cvdbEngine.getClinicalVariantSummaryStats(cv.getId(),
                            interpretationStatusId, project.getFqn(), null, token);
                    if (cv.getStats() == null) {
                        cv.setStats(new ClinicalVariantSummaryStats());
                    }
                    if (summaryStatsResult.getNumResults() > 0) {
                        updateSummaryStats(summaryStatsResult.first(), cv.getStats());
                    }
                }
            }
        }

        return result;
    }

    public static void updateSummaryStats(ClinicalVariantSummaryStats srcStats, ClinicalVariantSummaryStats destStats) {
        // Num. cases
        destStats.setNumCases(destStats.getNumCases() + srcStats.getNumCases());

        // Variant status counts
        updateStatsMap(srcStats.getVariantStatusCounts(), destStats.getVariantStatusCounts());

        // Variant confidence counts
        updateStatsMap(srcStats.getVariantConfidenceCounts(), destStats.getVariantConfidenceCounts());

        // Num. primary interpretations
        destStats.setNumPrimaryInterpretations(destStats.getNumPrimaryInterpretations() + srcStats.getNumPrimaryInterpretations());

        // Num. secondary interpretations
        destStats.setNumSecondaryInterpretations(destStats.getNumSecondaryInterpretations() + srcStats.getNumSecondaryInterpretations());

        // Interpretation summary stats
        updateInterpretationSummaryStats(srcStats.getInterpretationSummaryStats(), destStats.getInterpretationSummaryStats());

        // Clinical analysis disorder counts
        updateStatsMap(srcStats.getClinicalAnalysisDisorderCounts(), destStats.getClinicalAnalysisDisorderCounts());
    }

    private static void updateInterpretationSummaryStats(InterpretationSummaryStats srcStats, InterpretationSummaryStats destStats) {
        updateStatsMap(srcStats.getEvidenceTranscriptCounts(), destStats.getEvidenceTranscriptCounts());
        updateStatsMap(srcStats.getEvidenceGeneNameCounts(), destStats.getEvidenceGeneNameCounts());
        updateStatsMap(srcStats.getEvidenceModeOfInheritanceCounts(), destStats.getEvidenceModeOfInheritanceCounts());
        updateStatsMap(srcStats.getEvidencePanelCounts(), destStats.getEvidencePanelCounts());
        updateStatsMap(srcStats.getEvidenceReviewTierCounts(), destStats.getEvidenceReviewTierCounts());
        updateStatsMap(srcStats.getEvidenceReviewAcmgCounts(), destStats.getEvidenceReviewAcmgCounts());
        updateStatsMap(srcStats.getEvidenceReviewClinicalSignificanceCounts(), destStats.getEvidenceReviewClinicalSignificanceCounts());
        updateStatsMap(srcStats.getEvidenceClassificationAcmgCounts(), destStats.getEvidenceClassificationAcmgCounts());
    }

    private static void updateStatsMap(Map<String, Integer> srcMap, Map<String, Integer> destMap) {
        for (Map.Entry<String, Integer> entry : srcMap.entrySet()) {
            if (destMap.containsKey(entry.getKey())) {
                destMap.put(entry.getKey(), destMap.get(entry.getKey()) + entry.getValue());
            } else {
                destMap.put(entry.getKey(), entry.getValue());
            }
        }
    }
}

