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

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleQualityControl;
import org.opencb.opencga.core.models.sample.SampleUpdateParams;
import org.opencb.opencga.core.models.sample.SampleVariantQualityControlMetrics;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.SampleVariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;

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

        if (toolParams.isIndex() && StringUtils.isBlank(toolParams.getIndexId())) {
            throw new ToolException("Param 'indexId' is required when indexing.");
        }
        if (CollectionUtils.isEmpty(toolParams.getSample()) && CollectionUtils.isEmpty(toolParams.getIndividual())) {
            throw new ToolException("At least one param from 'sample' or 'individual' is required."
                    + " Use 'sample=" + ParamConstants.ALL + "' to get stats for all samples indexed"
                    + " in the Variant Storage");
        }
        if ("ALL".equals(toolParams.getIndexId())) {
            if (!toolParams.getVariantQuery().toQuery().isEmpty()) {
                throw new ToolException("Unable to index with indexId='ALL' if query is not empty. Current query is "
                        + toolParams.getVariantQuery().toQuery().toJson());
            }
        }
        if (toolParams.isIndex()) {
            String userId = getCatalogManager().getUserManager().getUserId(getToken());
            Study study = getCatalogManager().getStudyManager().get(this.study, new QueryOptions(), getToken()).first();
            boolean isOwner = study.getFqn().startsWith(userId + "@");
            if (!isOwner) {
                Group admins = study.getGroups().stream()
                        .filter(g->g.getId().equals(ParamConstants.ADMINS_GROUP))
                        .findFirst()
                        .orElseThrow(() -> new ToolException("Missing group " + ParamConstants.ADMINS_GROUP + " in study " + study));
                if (!admins.getUserIds().contains(userId)) {
                    throw new ToolException("Unable to run " + getId() + " with index=true. "
                            + "User '" + userId + "' is neither owner nor part of the group " + ParamConstants.ADMINS_GROUP + "'");
                }
            }
        }

        boolean forAllSamples = false;
        Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, token);
        if (CollectionUtils.isNotEmpty(toolParams.getSample())) {
            if (toolParams.getSample().get(0).equals(ParamConstants.ALL)) {
                forAllSamples = true;
                allSamples.addAll(indexedSamples);
            } else {
                catalogManager.getSampleManager().get(study, toolParams.getSample(), new QueryOptions(), token)
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
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

        if (allSamples.isEmpty()) {
            throw new ToolException("Missing samples!");
        }

        if (toolParams.isIndex()) {
            if (!toolParams.isIndexOverwrite()) {
                List<String> alreadyIndexedSamples = getCatalogManager().getSampleManager().search(study,
                        new Query(SampleDBAdaptor.QueryParams.ID.key(), allSamples)
                                .append(SampleDBAdaptor.STATS_ID, toolParams.getIndexId())
                                .append(SampleDBAdaptor.STATS_VARIANT_COUNT, ">=0"),
                        new QueryOptions(INCLUDE, SampleDBAdaptor.QueryParams.ID.key()), getToken())
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toList());
                if (!alreadyIndexedSamples.isEmpty()) {
                    if (forAllSamples) {
                        // Remove already indexed
                        allSamples.removeAll(alreadyIndexedSamples);
                    } else {
                        // Collect already indexed. Fail if any
                        addAttribute("alreadyIndexedSamples", alreadyIndexedSamples);
                        throw new ToolException("Sample variant stats already computed and indexed for samples " + alreadyIndexedSamples
                                + ". Remove those samples of add indexOverwrite=true to continue.");
                    }
                }
            }
        }

        checkedSamplesList = new ArrayList<>(allSamples);
        checkedSamplesList.sort(String::compareTo);
        if (checkedSamplesList.isEmpty()) {
            logger.info("All samples stats indexed. Nothing to do!");
            addInfo("All samples stats indexed. Nothing to do!");
        } else {
            // check read permission
            variantStorageManager.checkQueryPermissions(
                    new Query()
                            .append(VariantQueryParam.STUDY.key(), study)
                            .append(VariantQueryParam.INCLUDE_SAMPLE.key(), checkedSamplesList),
                    new QueryOptions(),
                    token);
            logger.info("Calculating " + SampleVariantStats.class.getSimpleName() + " for {} samples", checkedSamplesList.size());
            addAttribute("numSamples", checkedSamplesList.size());
        }
        if (toolParams.isIndex()) {
            logger.info("Indexing stats with ID=" + toolParams.getIndexId());
        }

        outputFile = getOutDir().resolve(getId() + ".json");
    }

    @Override
    protected List<String> getSteps() {
        if (toolParams.isIndex()) {
            return Arrays.asList(getId(), "index");
        } else {
            return Collections.singletonList(getId());
        }
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            if (checkedSamplesList.isEmpty()) {
                return;
            }
            getToolExecutor(SampleVariantStatsAnalysisExecutor.class)
                    .setOutputFile(outputFile)
                    .setStudy(study)
                    .setSampleNames(checkedSamplesList)
                    .setVariantQuery(toolParams.getVariantQuery() == null ? new Query() : toolParams.getVariantQuery().toQuery())
                    .execute();
        });
        if (toolParams.isIndex()) {
            if (checkedSamplesList.isEmpty()) {
                return;
            }
            step("index", () -> {
                Map<String, String> queryMap = toolParams.getVariantQuery().toQuery().entrySet()
                        .stream()
                        .filter(e -> e.getValue() != null)
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));

                ObjectReader reader = JacksonUtils.getDefaultObjectMapper().readerFor(SampleVariantStats.class);
                try (MappingIterator<SampleVariantStats> it = reader.readValues(outputFile.toFile())) {
                    while (it.hasNext()) {
                        SampleVariantStats sampleVariantStats = it.next();
                        SampleQualityControl qualityControl = getCatalogManager()
                                .getSampleManager()
                                .get(getStudy(), sampleVariantStats.getId(),
                                        new QueryOptions(INCLUDE, SampleDBAdaptor.QueryParams.QUALITY_CONTORL.key()), getToken())
                                .first()
                                .getQualityControl();
//                        SampleUpdateParams updateParams = new SampleUpdateParams()
//                                .setQualityControl(new SampleQualityControl()
//                                        .setVariantMetrics(new SampleVariantQualityControlMetrics()
////                                                .setVcfFileIds() // TODO
//                                                .setVariantStats(Collections.singletonList(new SampleQcVariantStats(
//                                                                toolParams.getIndexId(),
//                                                                toolParams.getIndexDescription(),
//                                                                queryMap,
//                                                                sampleVariantStats)
//                                                        )
//                                                )
//                                        )
//                                );
                        SampleQcVariantStats o = new SampleQcVariantStats(
                                toolParams.getIndexId(),
                                toolParams.getIndexDescription(),
                                queryMap,
                                sampleVariantStats);
                        if (qualityControl == null) {
                            qualityControl = new SampleQualityControl();
                        }
                        if (qualityControl.getVariantMetrics() == null) {
                            qualityControl.setVariantMetrics(new SampleVariantQualityControlMetrics());
                        }
                        if (CollectionUtils.isEmpty(qualityControl.getVariantMetrics().getVariantStats())) {
                            qualityControl.getVariantMetrics().setVariantStats(Collections.singletonList(o));
                        } else {
                            boolean hasItem = false;
                            for (int i = 0; i < qualityControl.getVariantMetrics().getVariantStats().size(); i++) {
                                if (qualityControl.getVariantMetrics().getVariantStats().get(i).getId().equals(o.getId())) {
                                    qualityControl.getVariantMetrics().getVariantStats().set(i, o);
                                    hasItem = true;
                                    break;
                                }
                            }
                            if (!hasItem) {
                                qualityControl.getVariantMetrics().getVariantStats().add(o);
                            }
                        }
                        SampleUpdateParams updateParams = new SampleUpdateParams().setQualityControl(qualityControl);
                        getCatalogManager().getSampleManager()
                                .update(study, sampleVariantStats.getId(), updateParams, new QueryOptions(), getToken());

                    }
                }
            });
        }
    }

}
