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

package org.opencb.opencga.core.models.variant.qc;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.variant.AnnotationVariantQueryParams;
import org.opencb.opencga.core.tools.ToolParams;

public class SampleQcSignatureAnalysisParams extends ToolParams {

    @DataField(id = "id", description = FieldConstants.SAMPLE_QC_SIGNATURE_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", description = FieldConstants.SAMPLE_QC_SIGNATURE_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "variantQuery", description = FieldConstants.SAMPLE_QC_SIGNATURE_VARIANT_QUERY_DESCRIPTION)
    private AnnotationVariantQueryParams variantQuery;

    @DataField(id = "fitting", description = FieldConstants.SAMPLE_QC_SIGNATURE_FIT_PARAMS_DESCRIPTION)
    private SampleQcSignatureFittingAnalysisParams fitting;

    public SampleQcSignatureAnalysisParams() {
    }

    public SampleQcSignatureAnalysisParams(String id, String description, AnnotationVariantQueryParams variantQuery, SampleQcSignatureFittingAnalysisParams fitting) {
        this.id = id;
        this.description = description;
        this.variantQuery = variantQuery;
        this.fitting = fitting;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SampleQcSignatureAnalysisParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", variantQuery=").append(variantQuery);
        sb.append(", fitting=").append(fitting);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public SampleQcSignatureAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public SampleQcSignatureAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public AnnotationVariantQueryParams getVariantQuery() {
        return variantQuery;
    }

    public SampleQcSignatureAnalysisParams setVariantQuery(AnnotationVariantQueryParams variantQuery) {
        this.variantQuery = variantQuery;
        return this;
    }

    public SampleQcSignatureFittingAnalysisParams getFitting() {
        return fitting;
    }

    public SampleQcSignatureAnalysisParams setFitting(SampleQcSignatureFittingAnalysisParams fitting) {
        this.fitting = fitting;
        return this;
    }
}