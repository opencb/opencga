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

package com.zettagenomics.opencga.enterprise.core.api;

import static org.opencb.commons.datastore.core.QueryOptions.*;

public class ParamConstants {

    public static final String CLINICAL_ANALYSES_INDEX_DESCRIPTION = "Index clinical analyses into CVDB";
    public static final String CLINICAL_ANALYSES_QUERY_DESCRIPTION = "Filter and fetch clinical analyses from CVDB";
    public static final String CLINICAL_INTERPRETATION_QUERY_DESCRIPTION = "Filter and fetch clinical interpretations from CVDB";
    public static final String CLINICAL_VARIANT_QUERY_DESCRIPTION = "Filter and fetch clinical variants from CVDB";
    public static final String CLINICAL_VARIANT_EVIDENCE_QUERY_DESCRIPTION = "Filter and fetch clinical variant evidences from CVDB";

    public static final String CLINICAL_VARIANT_SUMMARY_DESCRIPTION = "Get clinical variant summary from CVDB";

    public static final String INDEX_OVERWRITE_PARAM_NAME = "overwrite";
    public static final String INDEX_OVERWRITE_PARAM_DESCRIPTION = "Overwrite clinical analysis when CVDB indexing";

    public static final int DEFAULT_LIMIT = 100;

    public static final String STATS_LIMIT_DESCR = "Maximum number of results (i.e., buckets) to return for each aggregation";
    public static final String STATS_LIMIT_NAME = "statsLimit";
    public static final int STATS_DEFAULT_LIMIT = 10;

    public static final String STATS_ORDER_DESCR = "The sorting order of the results (i.e., buckets) based on their counts. For ascending"
            + " order use, '" + ASC + "' or '" + ASCENDING + "'; for descending order, '" + DESC + "' or '" + DESCENDING + "'";
    public static final String STATS_ORDER_NAME = "statsOrder";
    public static final String STATS_DEFAULT_ORDER = DESC;

    public static final String PROJECT_PARAM_DESCRIPTION = "Project ID";
    public static final String PROJECT_PARAM_NAME = "project";

    public static final String STUDY_PARAM_DESCRIPTION = "Study ID (or list of study IDs separated by commas)";
    public static final String STUDY_PARAM_NAME = "study";

    public static final String OPENCGA_STUDY_FQN = "OPENCGA_STUDY_FQN";
    public static final String OPENCGA_CLINICAL_ANALYSIS_ID = "OPENCGA_CLINICAL_ANALYSIS_ID";
    public static final String OPENCGA_INTERPRETATION_ID = "OPENCGA_INTERPRETATION_ID";
    public static final String OPENCGA_PRIMARY_INTERPRETATION = "OPENCGA_PRIMARY_INTERPRETATION";
    public static final String OPENCGA_VARIANT_ID = "OPENCGA_VARIANT_ID";
    public static final String OPENCGA_PRIMARY_FINDING = "OPENCGA_PRIMARY_FINDING";
}
