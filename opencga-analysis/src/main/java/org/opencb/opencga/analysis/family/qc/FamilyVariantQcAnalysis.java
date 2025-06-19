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

package org.opencb.opencga.analysis.family.qc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.Relatedness;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.ResourceException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.ResourceManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.AnalysisTool;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyInternal;
import org.opencb.opencga.core.models.family.FamilyQualityControl;
import org.opencb.opencga.core.models.variant.qc.FamilyQcAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.FamilyVariantQcAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.models.common.InternalStatus.READY;
import static org.opencb.opencga.core.models.common.QualityControlStatus.ERROR;
import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_FAMILIES;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.JSON;
import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF_GZ;

@Tool(id = FamilyVariantQcAnalysis.ID, resource = Enums.Resource.FAMILY, description = FamilyVariantQcAnalysis.DESCRIPTION)
public class FamilyVariantQcAnalysis extends VariantQcAnalysis {

    public static final String ID = "family-variant-qc";
    public static final String DESCRIPTION = "Run quality control (QC) for a given family. It computes the relatedness scores among the"
            + " family members";

    @ToolParams
    protected final FamilyQcAnalysisParams analysisParams = new FamilyQcAnalysisParams();

    private Family family;

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        logger.info("Checking {}", analysisParams);

        // Check parameters
        family = checkParameters(analysisParams, study, catalogManager, token);

        // Check custom relatedness resources: prune-in, frq and thresholds files
        userResourcesPath = checkUserResourcesDir(analysisParams.getResourcesDir(), study, catalogManager, token);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>(Arrays.asList(PREPARE_RESOURCES_STEP, PREPARE_QC_STEP, ID));
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            steps.add(INDEX_QC_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        // Main steps
        step(PREPARE_RESOURCES_STEP, this::prepareResources);
        step(PREPARE_QC_STEP, this::prepareQualityControl);
        step(ID, this::runFamilyQc);
        if (getSteps().contains(INDEX_QC_STEP)) {
            step(INDEX_QC_STEP, this::indexQualityControl);
        }

        // Clean execution
        clean();
    }

    protected void prepareResources() throws IOException, ResourceException, ToolException {
        ResourceManager resourceManager = new ResourceManager(getOpencgaHome());

        // Prepare relatedness resources
        prepareResources(RELATEDNESS_ANALYSIS_ID, null, resourceManager);
    }

    protected void prepareQualityControl() throws ToolException {
        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Family.class);
            String basename = getOutDir().resolve(family.getId()).toAbsolutePath().toString();

            // Write family (JSON format)
            Path jsonPath = Paths.get(basename + "_info." + JSON.getExtension());
            objectWriter.writeValue(jsonPath.toFile(), family);

            // Check JSON file
            if (!Files.exists(jsonPath)) {
                throw new ToolException("Something wrong happened when saving family " + family.getId() + " in JSON file "
                        + jsonPath);
            }
            jsonPaths.add(jsonPath);

            // Export family variants (VCF format)
            // Create variant query
            String gt = getIndexedAndNoSomaticSampleIds(family, study, catalogManager, token).stream().map(s -> s + ":0/0,0/1,1/1")
                    .collect(Collectors.joining(";"));
            Query query = new Query()
                    .append(VariantQueryParam.STUDY.key(), study)
                    .append(VariantQueryParam.TYPE.key(), VariantType.SNV)
                    .append(VariantQueryParam.GENOTYPE.key(), gt)
                    .append(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
                            .split(",")));

            // Create query options
            QueryOptions queryOptions = new QueryOptions().append(QueryOptions.INCLUDE, "id,studies.samples");

            // Export variants (VCF.GZ format)
            getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

            // Check VCF file
            Path vcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
            if (!Files.exists(vcfPath)) {
                throw new ToolException("Something wrong happened when exporting VCF file for family ID " + family.getId() + ". VCF file "
                        + vcfPath + " was not created. Export query = " + query.toJson() + "; export query options = "
                        + queryOptions.toJson());
            }
            vcfPaths.add(vcfPath);
        } catch (CatalogException | IOException | StorageEngineException e) {
            // Clean execution
            clean();
            throw new ToolException(e);
        }
    }

    protected void runFamilyQc() throws ToolException {
        // Get executor to execute Python script that computes the family QC
        FamilyVariantQcAnalysisExecutor executor = getToolExecutor(FamilyVariantQcAnalysisExecutor.class);

        executor.setVcfPaths(vcfPaths)
                .setJsonPaths(jsonPaths)
                .setQcParams(analysisParams)
                .execute();
    }

    protected void indexQualityControl() throws ToolException {
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectReader familyQcReader = JacksonUtils.getDefaultObjectMapper().readerFor(FamilyQualityControl.class);

        QualityControlStatus qcStatus;
        FamilyQualityControl familyQc;

        // Check and parse the relatedness output file
        Path qcPath = getOutDir().resolve(family.getId()).resolve(QC_RESULTS_FILENAME);
        if (!Files.exists(qcPath)) {
            qcStatus = new QualityControlStatus(ERROR, FAILURE_FILE + qcPath.getFileName() + NOT_FOUND);
            clean();
            updateFamilyQualityControl(new FamilyQualityControl(), qcStatus);
            throw new ToolException(qcStatus.getDescription() + getIdLogMessage(family.getId(), FAMILY_QC_TYPE));
        } else {
            try {
                familyQc = familyQcReader.readValue(qcPath.toFile());

                // Set common attributes
                if (familyQc.getAttributes() == null) {
                    familyQc.setAttributes(new ObjectMap());
                }
                addCommonAttributes(familyQc.getAttributes());

                qcStatus = new QualityControlStatus(READY, SUCCESS);
            } catch (IOException e) {
                qcStatus = new QualityControlStatus(ERROR, FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'");
                // Clean family QC
                clean();
                updateFamilyQualityControl(new FamilyQualityControl(), qcStatus);
                throw new ToolException(qcStatus.getDescription() + getIdLogMessage(family.getId(), FAMILY_QC_TYPE));
            }
        }

        // Update family quality control in catalog
        updateFamilyQualityControl(familyQc, qcStatus);
    }

    public static Family checkParameters(FamilyQcAnalysisParams params, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        // Check study
        checkStudy(studyId, catalogManager, token);

        // Check permissions
        checkPermissions(WRITE_FAMILIES, params.getSkipIndex(), studyId, catalogManager, token);

        // Sanity check
        if (CollectionUtils.isEmpty(params.getFamilies())) {
            throw new ToolException("Missing list of family IDs.");
        }
        if (params.getFamilies().size() > 1) {
            throw new ToolException("Only one family ID is supported.");
        }

        // Check family in catalog
        Family checkedFamily;
        String familyId = params.getFamilies().get(0);
        OpenCGAResult<Family> familyResult;
        try {
            familyResult = catalogManager.getFamilyManager().get(studyId, familyId, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error getting family " + familyId + " from OpenCGA catalog.", e);
        }
        if (familyResult.getNumResults() == 0) {
            throw new ToolException("Family ID '" + familyId + "' not found in OpenCGA catalog for study '" + studyId + "'");
        } else if (familyResult.getNumResults() > 1) {
            throw new ToolException("More than one family found for ID '" + familyId + "' in OpenCGA catalog for study '"
                    + studyId + "'");
        }
        checkedFamily = familyResult.first();

        // Check number of samples
        List<String> sampleIds = getIndexedAndNoSomaticSampleIds(checkedFamily, studyId, catalogManager, token);
        if (sampleIds.size() < 2) {
            throw new ToolException("Too few samples found (" + StringUtils.join(sampleIds, ",") +  ") for that family members;"
                    + " minimum is 2 member samples");
        }

        // Check compatibility between QC status (READY) and overwrite
        if (!Boolean.TRUE.equals(params.getSkipIndex()) && !Boolean.TRUE.equals(params.getOverwrite())
                && Optional.ofNullable(checkedFamily)
                .map(Family::getInternal)
                .map(FamilyInternal::getQualityControlStatus)
                .map(QualityControlStatus::getId)
                .filter(READY::equals)
                .isPresent()) {
            throw new ToolException("Family QC " + HAS_ALREADY_BEEN_COMPUTED_WITH_A_READY_STATUS + " for family '" + familyId
                    + "'. To recompute QC, you must set the 'overwrite' flag to TRUE.");
        }

        return checkedFamily;
    }

    private void updateFamilyQualityControl(FamilyQualityControl familyQc, QualityControlStatus qcStatus)
            throws ToolException {
        try {
            catalogManager.getFamilyManager().updateQualityControl(getStudy(), family.getId(), familyQc, qcStatus, token);
        } catch (CatalogException e) {
            throw new ToolException(FAILURE_COULD_NOT_UPDATE_QUALITY_CONTROL_IN_OPEN_CGA_CATALOG
                    + getIdLogMessage(family.getId(), FAMILY_QC_TYPE), e);
        }
    }
}
