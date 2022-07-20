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

package org.opencb.opencga.core.models.variant;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.ToolParams;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class MutationalSignatureAnalysisParams extends ToolParams {
    public static final String DESCRIPTION = "Mutational signature analysis params";

    @DataField(description = ParamConstants.MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_SAMPLE_DESCRIPTION)
    private String sample;
    @DataField(description = ParamConstants.MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;
    @DataField(description = ParamConstants.MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_QUERY_DESCRIPTION)
    private ObjectMap query;
    @DataField(description = ParamConstants.MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_RELEASE_DESCRIPTION)
    private String release;
    @DataField(description = ParamConstants.MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_FITTING_DESCRIPTION)
    private boolean fitting;

    @DataField(description = ParamConstants.MUTATIONAL_SIGNATURE_ANALYSIS_PARAMS_OUTDIR_DESCRIPTION)
    private String outdir;

    public MutationalSignatureAnalysisParams() {
    }

    public MutationalSignatureAnalysisParams(String sample, String id, String description, ObjectMap query, String release,
                                             boolean fitting, String outdir) {
        this.sample = sample;
        this.id = id;
        this.description = description;
        this.query = query;
        this.release = release;
        this.fitting = fitting;
        this.outdir = outdir;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MutationalSignatureAnalysisParams{");
        sb.append("sample='").append(sample).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", query=").append(query);
        sb.append(", release='").append(release).append('\'');
        sb.append(", fitting=").append(fitting);
        sb.append(", outdir='").append(outdir).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSample() {
        return sample;
    }

    public MutationalSignatureAnalysisParams setSample(String sample) {
        this.sample = sample;
        return this;
    }

    public String getId() {
        return id;
    }

    public MutationalSignatureAnalysisParams setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MutationalSignatureAnalysisParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public ObjectMap getQuery() {
        return query;
    }

    public MutationalSignatureAnalysisParams setQuery(ObjectMap query) {
        this.query = query;
        return this;
    }

    public String getRelease() {
        return release;
    }

    public MutationalSignatureAnalysisParams setRelease(String release) {
        this.release = release;
        return this;
    }

    public boolean isFitting() {
        return fitting;
    }

    public MutationalSignatureAnalysisParams setFitting(boolean fitting) {
        this.fitting = fitting;
        return this;
    }

    public String getOutdir() {
        return outdir;
    }

    public MutationalSignatureAnalysisParams setOutdir(String outdir) {
        this.outdir = outdir;
        return this;
    }
}
