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

package org.opencb.opencga.analysis.variant.relatedness;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.GwasAnalysisExecutor;
import org.opencb.opencga.core.tools.variant.RelatednessAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.StudyResourceMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Tool(id = RelatednessAnalysis.ID, resource = Enums.Resource.VARIANT, description = RelatednessAnalysis.DESCRIPTION)
public class RelatednessAnalysis extends OpenCgaTool {

    public static final String ID = "relatedness";
    public static final String DESCRIPTION = "Compute a score to quantify relatedness between individuals.";

    private String study;
    private List<String> samples;

    public RelatednessAnalysis() {
    }

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public RelatednessAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    public List<String> getSamples() {
        return samples;
    }

    public RelatednessAnalysis setSamples(List<String> samples) {
        this.samples = samples;
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        if (StringUtils.isEmpty(study)) {
            throw new ToolException("Missing study!");
        }

        try {
            study = catalogManager.getStudyManager().get(study, null, token).first().getFqn();
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        // check read permission
        try {
            List<String> allSamples = new ArrayList<>();
            allSamples.addAll(samples);
            variantStorageManager.checkQueryPermissions(
                    new Query()
                            .append(VariantQueryParam.STUDY.key(), study)
                            .append(VariantQueryParam.INCLUDE_SAMPLE.key(), allSamples),
                    new QueryOptions(),
                    token);
        } catch (CatalogException | StorageEngineException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected void run() throws ToolException {

        step("relatedness", () -> {
            RelatednessAnalysisExecutor relatednessExecutor = getToolExecutor(RelatednessAnalysisExecutor.class);

            relatednessExecutor.setStudy(study)
                    .setSamples(samples)
                    .execute();
        });
    }
}
