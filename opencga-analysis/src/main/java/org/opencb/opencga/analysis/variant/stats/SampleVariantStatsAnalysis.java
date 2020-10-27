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

package org.opencb.opencga.analysis.variant.stats;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.SampleVariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tool(id = SampleVariantStatsAnalysis.ID, resource = Enums.Resource.VARIANT, description = SampleVariantStatsAnalysis.DESCRIPTION)
public class SampleVariantStatsAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-variant-stats";
    public static final String DESCRIPTION = "Compute sample variant stats for the selected list of samples.";

    @ToolParams
    protected SampleVariantStatsAnalysisParams toolParams;
    private ArrayList<String> checkedSamplesList;
    private Path outputFile;

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);
        Set<String> allSamples = new HashSet<>();

        Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, token);
        if (CollectionUtils.isNotEmpty(toolParams.getSample())) {
            catalogManager.getSampleManager().get(study, toolParams.getSample(), new QueryOptions(), token)
                    .getResults()
                    .stream()
                    .map(Sample::getId)
                    .forEach(allSamples::add);
        }
        if (CollectionUtils.isNotEmpty(toolParams.getIndividual())) {
            Query query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), toolParams.getIndividual());
            catalogManager.getSampleManager().search(study, query, new QueryOptions(), token)
                    .getResults()
                    .stream()
                    .map(Sample::getId)
                    .forEach(allSamples::add);
        }

        List<String> nonIndexedSamples = new ArrayList<>();
        // Remove non-indexed samples
        for (String sample : allSamples) {
            if (!indexedSamples.contains(sample)) {
                nonIndexedSamples.add(sample);
            }
        }
        if (!nonIndexedSamples.isEmpty()) {
            throw new IllegalArgumentException("Samples " + nonIndexedSamples + " are not indexed into the Variant Storage");
        }

        checkedSamplesList = new ArrayList<>(allSamples);
        checkedSamplesList.sort(String::compareTo);

        if (allSamples.isEmpty()) {
            throw new ToolException("Missing samples!");
        }

        // check read permission
        variantStorageManager.checkQueryPermissions(
                new Query()
                        .append(VariantQueryParam.STUDY.key(), study)
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), checkedSamplesList),
                new QueryOptions(),
                token);

        outputFile = getOutDir().resolve(getId() + ".json");
    }

    @Override
    protected List<String> getSteps() {
        return super.getSteps();
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            getToolExecutor(SampleVariantStatsAnalysisExecutor.class)
                    .setOutputFile(outputFile)
                    .setStudy(study)
                    .setSampleNames(checkedSamplesList)
                    .setVariantQuery(toolParams.getVariantQuery() == null ? new Query() : toolParams.getVariantQuery().toQuery())
                    .execute();
        });
    }

}
