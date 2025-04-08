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

package org.opencb.opencga.analysis.variant.operations;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.operations.variant.VariantAggregateFamilyParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

import java.util.ArrayList;
import java.util.List;

@Tool(id = VariantAggregateFamilyOperationTool.ID, description = VariantAggregateFamilyOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION,
        resource = Enums.Resource.VARIANT,
        priority = Enums.Priority.HIGH)
public class VariantAggregateFamilyOperationTool extends OperationTool {
    public static final String ID = "variant-aggregate-family";
    public static final String DESCRIPTION = "Find variants where not all the samples are present, and fill the empty values.";
    private String study;

    @ToolParams
    protected VariantAggregateFamilyParams toolParams;


    @Override
    protected void check() throws Exception {
        super.check();

        study = getStudyFqn();

        // Check exclusivity between family and samples
        if (StringUtils.isNotEmpty(toolParams.getFamily()) && CollectionUtils.isNotEmpty(toolParams.getSamples())) {
            throw new IllegalArgumentException("You can not use both 'family' and 'samples' parameters at the same time. "
                    + "Please, use only one of them.");
        }

        List<String> samples;
        if (StringUtils.isNotEmpty(toolParams.getFamily())) {
            Family family = getCatalogManager().getFamilyManager().get(study, toolParams.getFamily(), new QueryOptions(), getToken())
                    .first();
            samples = new ArrayList<>(family.getMembers().size());
            for (Individual member : family.getMembers()) {
                for (Sample sample : member.getSamples()) {
                    if (sample.getInternal().getVariant().getIndex().getStatus().isReady()) {
                        samples.add(sample.getId());
                    }
                }
            }
            if (samples.size() < 2) {
                throw new IllegalArgumentException("Family '" + family.getId() + "' contains " + family.getMembers().size()
                        + " members, but only " + samples.size() + " of them have a sample indexed in the variant storage."
                        + " Aggregate family operation requires at least two samples.");
            }
            addInfo("Family '" + family.getId() + "' contains " + family.getMembers().size() + " members. "
                    + samples.size() + " of them have a sample indexed in the variant storage. "
                    + "The following samples will be used for the aggregation: [" + String.join(", ", samples) + "].");

            toolParams.setSamples(samples); // Set the samples to be used for the aggregation
        } else {
            samples = toolParams.getSamples();
            if (samples == null || samples.size() < 2) {
                throw new IllegalArgumentException("Aggregate family operation requires at least two samples.");
            }
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> {
            variantStorageManager.aggregateFamily(study, toolParams, token, getOutDir().toUri());
        });
    }
}
