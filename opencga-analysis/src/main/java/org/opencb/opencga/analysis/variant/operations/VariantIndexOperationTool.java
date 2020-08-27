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

import io.jsonwebtoken.lang.Collections;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.ResourceReference;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.opencb.opencga.analysis.variant.manager.operations.VariantFileIndexerOperationManager.LOAD;
import static org.opencb.opencga.analysis.variant.manager.operations.VariantFileIndexerOperationManager.TRANSFORM;

@Tool(id = VariantIndexOperationTool.ID, description = VariantIndexOperationTool.DESCRIPTION,
        type = Tool.Type.OPERATION, resource = Enums.Resource.VARIANT)
public class VariantIndexOperationTool extends OperationTool {
    public static final String ID = "variant-index";
    public static final String DESCRIPTION = "Index variant files into the variant storage";

    private VariantIndexParams indexParams = new VariantIndexParams();
    private String study;

    public void setFile(String file) {
        indexParams.setFile(file);
    }

    public void setTransform(boolean transform) {
        indexParams.setTransform(transform);
    }

    public void setLoad(boolean load) {
        indexParams.setLoad(load);
    }

    @Override
    protected void check() throws Exception {
        super.check();

        indexParams.updateParams(params);
        study = getStudyFqn();

        params.put(LOAD, indexParams.isLoad());
        params.put(TRANSFORM, indexParams.isTransform());
        if (indexParams.isTransform() && !indexParams.isLoad()) {
            // Ensure keeping intermediate files if only transforming
            keepIntermediateFiles = true;
        }

        params.put(VariantStorageOptions.MERGE_MODE.key(), indexParams.getMerge());

        params.put(VariantStorageOptions.STATS_CALCULATE.key(), indexParams.isCalculateStats());
        params.put(VariantStorageOptions.EXTRA_FORMAT_FIELDS.key(), indexParams.getIncludeSampleData());
        params.put(VariantStorageOptions.EXCLUDE_GENOTYPES.key(), indexParams.isExcludeGenotypes());
        params.put(VariantStorageOptions.STATS_AGGREGATION.key(), indexParams.getAggregated());
        params.put(VariantStorageOptions.STATS_AGGREGATION_MAPPING_FILE.key(), indexParams.getAggregationMappingFile());
        params.put(VariantStorageOptions.GVCF.key(), indexParams.isGvcf());

//        queryOptions.putIfNotNull(VariantFileIndexerStorageOperation.TRANSFORMED_FILES, indexParams.transformedPaths);

        params.put(VariantStorageOptions.ANNOTATE.key(), indexParams.isAnnotate());
        if (indexParams.getAnnotator() != null) {
            params.put(VariantStorageOptions.ANNOTATOR.key(),
                    indexParams.getAnnotator());
        }
        params.put(VariantStorageOptions.ANNOTATION_OVERWEITE.key(), indexParams.isOverwriteAnnotations());
        params.put(VariantStorageOptions.RESUME.key(), indexParams.isResume());
        params.put(VariantStorageOptions.NORMALIZATION_SKIP.key(), indexParams.getNormalizationSkip());
        params.put(VariantStorageOptions.NORMALIZATION_REFERENCE_GENOME.key(), indexParams.getReferenceGenome());
        params.put(VariantStorageOptions.FAMILY.key(), indexParams.isFamily());
        params.put(VariantStorageOptions.SOMATIC.key(), indexParams.isSomatic());
        params.put(VariantStorageOptions.LOAD_SPLIT_DATA.key(), indexParams.getLoadSplitData());
        params.put(VariantStorageOptions.LOAD_MULTI_FILE_DATA.key(), indexParams.isLoadMultiFileData());
        params.put(VariantStorageOptions.LOAD_SAMPLE_INDEX.key(), indexParams.getLoadSampleIndex());
        params.put(VariantStorageOptions.LOAD_ARCHIVE.key(), indexParams.getLoadArchive());
        params.put(VariantStorageOptions.LOAD_HOM_REF.key(), indexParams.getLoadHomRef());
        params.put(VariantStorageOptions.POST_LOAD_CHECK.key(), indexParams.getPostLoadCheck());
        params.put(VariantStorageOptions.INDEX_SEARCH.key(), indexParams.isIndexSearch());
        params.put(VariantStorageOptions.DEDUPLICATION_POLICY.key(), indexParams.getDeduplicationPolicy());
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>();
        steps.add(getId());
        if (indexParams.isFamily()) {
            steps.add("family-index");
        }
        return steps;
    }

    @Override
    protected void run() throws Exception {
        List<URI> inputFiles = new ArrayList<>();
        step(() -> {
            List<StoragePipelineResult> results =
                    variantStorageManager.index(study, indexParams.getFile(), getOutDir(keepIntermediateFiles).toString(), params, token);
            addAttribute("indexedFiles", Collections.size(results));
            if (Collections.isEmpty(results)) {
                addWarning("Nothing to do!");
            }
            addAttribute("StoragePipelineResult", results);
            for (StoragePipelineResult result : results) {
                inputFiles.add(result.getInput());
            }

            File[] files = getOutDir(keepIntermediateFiles).toFile().listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().endsWith(VariantReaderUtils.MALFORMED_FILE + ".txt")) {
                        if (!keepIntermediateFiles) {
                            Files.move(file.toPath(), getOutDir().resolve(file.getName()));
                        }
                        addWarning("Found malformed variants. Check file " + file.getName());
                    }
                    if (file.getName().endsWith(VariantReaderUtils.DUPLICATED_FILE + ".tsv")) {
                        if (!keepIntermediateFiles) {
                            Files.move(file.toPath(), getOutDir().resolve(file.getName()));
                        }
                        addWarning("Found duplicated variants. Check file " + file.getName());
                    }
                }
            }
        });

        if (indexParams.isFamily()) {
            step("family-index", () -> {
                OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult = getCatalogManager().getFileManager()
                        .search(study,
                                new Query(FileDBAdaptor.QueryParams.URI.key(), inputFiles),
                                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.SAMPLES.key()), getToken());

                Set<String> samples = new HashSet<>();
                for (org.opencb.opencga.core.models.file.File file : fileResult.getResults()) {
                    for (ResourceReference sample : file.getSamples()) {
                        samples.add(sample.getId());
                    }
                }

                variantStorageManager.familyIndexBySamples(study, samples, params, getToken());
            });
        }
    }
}
