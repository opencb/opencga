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
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.commons.datastore.core.QueryOptions.EXCLUDE;

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

        // First, get clinical variants
        OpenCGAResult<ClinicalVariant> result = clinicalInterpretationManager.get(query, queryOptions, token);

        // Compute summary stats if exclude stats is not set
        if (!getSkipStats(queryOptions)) {
            for (ClinicalVariant cv : result.getResults()) {
                DataResult<ClinicalVariantSummaryStats> summaryStatsResult = cvdbEngine.getClinicalVariantSummaryStats(cv.getId(),
                        null, token);
                cv.setStats(summaryStatsResult.getResults());
            }
        }

        return result;
    }

    public static boolean getSkipStats(QueryOptions queryOptions) {
        boolean skipStats = false;
        if (queryOptions != null && queryOptions.containsKey(EXCLUDE) && queryOptions.getAsStringList(EXCLUDE).contains("stats")) {
            skipStats = true;
        }
        return skipStats;
    }
}

