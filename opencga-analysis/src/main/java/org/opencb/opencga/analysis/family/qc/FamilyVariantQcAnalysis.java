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
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.qc.Relatedness;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyInternal;
import org.opencb.opencga.core.models.family.FamilyQualityControl;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.variant.qc.FamilyQcAnalysisParams;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.FamilyVariantQcAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

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

    private List<Family> families = new ArrayList<>();

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        logger.info("Checking {}", analysisParams);

        // Check parameters
        checkParameters(analysisParams, study, catalogManager, token);

        // Check custom resources path
        userResourcesPath = checkResourcesDir(analysisParams.getResourcesDir(), study, catalogManager, token);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = new ArrayList<>(Arrays.asList(PREPARE_QC_STEP, ID));
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            steps.add(INDEX_QC_STEP);
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        // Main steps
        step(PREPARE_QC_STEP, this::prepareQualityControl);
        step(ID, this::runFamilyQc);
        if (getSteps().contains(INDEX_QC_STEP)) {
            step(INDEX_QC_STEP, this::indexQualityControl);
        }

        // Clean execution
        clean();
    }

    protected void prepareQualityControl() throws ToolException {
        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Family.class);
            for (String familyId : analysisParams.getFamilies()) {
                // Get family
                OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(study, familyId, QueryOptions.empty(), token);
                Family family = familyResult.first();

                // Add family to the list
                families.add(family);

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
                String basename = getOutDir().resolve(familyId).toAbsolutePath().toString();
                getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

                // Check VCF file
                Path vcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
                if (!Files.exists(vcfPath)) {
                    throw new ToolException("Something wrong happened when exporting VCF file for family ID " + familyId + ". VCF file "
                            + vcfPath + " was not created. Export query = " + query.toJson() + "; export query options = "
                            + queryOptions.toJson());
                }
                vcfPaths.add(vcfPath);

                // Write family (JSON format)
                Path jsonPath = Paths.get(basename + "_info." + JSON.getExtension());
                objectWriter.writeValue(jsonPath.toFile(), family);

                // Check JSON file
                if (!Files.exists(jsonPath)) {
                    throw new ToolException("Something wrong happened when saving JSON file for family ID " + familyId + ". JSON file "
                            + jsonPath + " was not created.");
                }
                jsonPaths.add(jsonPath);
            }
        } catch (CatalogException | IOException | StorageEngineException e) {
            // Clean execution
            clean();
            throw new ToolException(e);
        }

        // Prepare resource files
        prepareResources();
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
        ObjectReader relatednessReader = JacksonUtils.getDefaultObjectMapper().readerFor(Relatedness.class);
        ObjectReader relatednessListReader = JacksonUtils.getDefaultObjectMapper().readerFor(new TypeReference<List<Relatedness>>() {});

        FamilyQualityControl familyQc;
        QualityControlStatus qcStatus;
        String logMsg;

        failedQcSet = new HashSet<>();
        for (Family family : families) {
            familyQc = new FamilyQualityControl();

            // Check and parse the relatedness output file
            Path qcPath = getOutDir().resolve(family.getId()).resolve(RELATEDNESS_ANALYSIS_ID)
                    .resolve(RELATEDNESS_ANALYSIS_ID + QC_JSON_EXTENSION);
            if (!Files.exists(qcPath)) {
                failedQcSet.add(family.getId());
                qcStatus = new QualityControlStatus(ERROR, FAILURE_FILE + qcPath.getFileName() + NOT_FOUND);

                logMsg = qcStatus.getDescription() + getIdLogMessage(family.getId(), FAMILY_QC_TYPE);
                addError(new ToolException(logMsg));
                logger.error(logMsg);
            } else {
                try {
                    List<Relatedness> relatednessList = isQcArray(qcPath)
                            ? relatednessListReader.readValue(qcPath.toFile())
                            : Collections.singletonList(relatednessReader.readValue(qcPath.toFile()));

                    // Set common attributes
                    for (Relatedness relatedness : relatednessList) {
                        addCommonAttributes(relatedness.getAttributes());
                    }

                    familyQc.setRelatedness(relatednessList);
                    qcStatus = new QualityControlStatus(READY, SUCCESS);
                } catch (IOException e) {
                    failedQcSet.add(family.getId());
                    qcStatus = new QualityControlStatus(ERROR, FAILURE_ERROR_PARSING_QC_JSON_FILE + qcPath.getFileName() + "'");
                    familyQc = new FamilyQualityControl();

                    logMsg = qcStatus.getDescription() + getIdLogMessage(family.getId(), FAMILY_QC_TYPE);
                    addError(new ToolException(logMsg));
                    logger.error(logMsg);
                }
            }

            try {
                catalogManager.getFamilyManager().updateQualityControl(getStudy(), family.getId(), familyQc, qcStatus, token);
            } catch (CatalogException e) {
                failedQcSet.add(family.getId());
                logMsg = FAILURE_COULD_NOT_UPDATE_QUALITY_CONTROL_IN_OPEN_CGA_CATALOG + getIdLogMessage(family.getId(), FAMILY_QC_TYPE);
                addError(e);
                logger.error(logMsg, e);
            }
        }

        checkFailedQcCounter(families.size(), FAMILY_QC_TYPE);
    }

    public static void checkParameters(FamilyQcAnalysisParams params, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        // Check study
        checkStudy(studyId, catalogManager, token);

        // Check permissions
        checkPermissions(WRITE_FAMILIES, params.getSkipIndex(), studyId, catalogManager, token);

        // Sanity check
        if (CollectionUtils.isEmpty(params.getFamilies())) {
            throw new ToolException("Missing list of family IDs.");
        }

        // Check families in catalog
        // Collect possible errors in a map where key is the family ID and value is the error
        Map<String, String> errors = new HashMap<>();
        for (String familyId : params.getFamilies()) {
            // Get family from catalog
            Family family = null;
            try {
                OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(studyId, familyId, QueryOptions.empty(), token);
                if (familyResult.getNumResults() == 0) {
                    errors.put(familyId, "Family not found");
                } else if (familyResult.getNumResults() > 1) {
                    errors.put(familyId, "More than one family found");
                } else {
                    family = familyResult.first();

                    // Check number of samples
                    List<String> sampleIds = getIndexedAndNoSomaticSampleIds(family, studyId, catalogManager, token);
                    if (sampleIds.size() < 2) {
                        errors.put(familyId, "Too few samples found (" + sampleIds.size() + ") for that family members; minimum is 2 member"
                                + " samples");
                    }
                }

                // Check compatibility between QC status (READY) and overwrite
                if (!Boolean.TRUE.equals(params.getSkipIndex()) && !Boolean.TRUE.equals(params.getOverwrite())
                        && Optional.ofNullable(family)
                        .map(Family::getInternal)
                        .map(FamilyInternal::getQualityControlStatus)
                        .map(QualityControlStatus::getId)
                        .filter(READY::equals)
                        .isPresent()) {
                    errors.put(familyId, "It is mandatory to set 'overwrite' to TRUE when QC has already been computed"
                            + " (status: READY)");
                }
            } catch (CatalogException e) {
                errors.put(familyId, Arrays.toString(e.getStackTrace()));
            }
        }

        // Check error
        if (MapUtils.isNotEmpty(errors)) {
            throw new ToolException("Found the following errors: " + StringUtils.join(errors.entrySet().stream().map(
                    e -> "Family ID '" + e.getKey() + "': " + e.getValue()).collect(Collectors.toList()), ","));
        }

        // Check resources dir
        checkResourcesDir(params.getResourcesDir(), studyId, catalogManager, token);
    }
}
