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
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantSummary;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;

import java.io.IOException;

/**
 * Created by jtarraga on 11/11/17.
 */
public class CvdbUtils {

    private CvdbUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static DataResult<ClinicalVariant> getClinicalVariant(Query query, QueryOptions queryOptions,
                                                                 ClinicalInterpretationManager clinicalInterpretationManager,
                                                                 CvdbSolrEngine cvdbEngine, String token)
            throws StorageEngineException, CatalogException, IOException, CvdbException {
        String projectId = query.getString(ParamConstants.PROJECT_PARAM);
        String studyId = query.getString(ParamConstants.STUDY_PARAM);

        // First, get clinical variants
        OpenCGAResult<ClinicalVariant> result = clinicalInterpretationManager.get(query, queryOptions, token);

        // Then, set summary for those clinical variants
        for (ClinicalVariant cv : result.getResults()) {
            DataResult<ClinicalVariantSummary> summaryResult = cvdbEngine.getClinicalVariantSummary(cv.getId(), projectId, studyId, token);
            cv.setSummary(summaryResult.first());
        }
        return result;
    }
}

