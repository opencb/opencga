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

public class ParamConstants {


    public static final String CLINICAL_ANALYSES_QUERY_DESCRIPTION = "Filter and fetch clinical analysis from CVDB";

    public static final String PROJECT_QUERY_DESCRIPTION = "Project ID";
    public static final String PROJECT_QUERY_PARAM = "projectId";

    public static final String VARIANT_QUERY_DESCRIPTION = "Variant ID (or list of variant IDs separated by commas),"
            + " e.g.: 6:31356248:G:C,X:53196017:G:A";
    public static final String VARIANT_QUERY_PARAM = "variantId";

    public static final int DEFAULT_LIMIT = 100;
}
