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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.variant.qc.VariantQcAnalysis;
import org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyQualityControl;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.variant.FamilyQcAnalysisParams;
import org.opencb.opencga.core.models.variant.QcRelatednessAnalysisParams;
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
import static org.opencb.opencga.core.models.common.QualityControlStatus.COMPUTING;
import static org.opencb.opencga.core.models.common.QualityControlStatus.NONE;
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

    @Override
    protected void check() throws Exception {
        super.check();
        checkParameters(analysisParams, getStudy(), catalogManager, token);

        // Update paths from relatedness external files
        updateRelatednessFilePaths(analysisParams.getRelatednessParams());
    }

    @Override
    protected void run() throws ToolException {
        setUpStorageEngineExecutor(study);

        List<Family> families = new ArrayList<>();
        LinkedList<Path> familyVcfPaths = new LinkedList<>();
        LinkedList<Path> familyJsonPaths = new LinkedList<>();

        try {
            ObjectWriter objectWriter = JacksonUtils.getDefaultObjectMapper().writerFor(Family.class);
            for (String familyId : analysisParams.getFamilies()) {
                // Get family
                OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(study, familyId, QueryOptions.empty(), token);
                Family family = familyResult.first();

                // Decide if quality control has to be performed
                //   - by checking the quality control status, if it is READY means it has been computed previously, and
                //   - by checking the flag overwrite
                if (family.getInternal() != null
                        && !performQualityControl(family.getInternal().getQualityControlStatus(), analysisParams.getOverwrite())) {
                    // Quality control does not have to be performed for this family
                    continue;
                }

                // Set quality control status to COMPUTING to prevent multiple family QCs from running simultaneously
                // for the same family
                if (!setComputingStatus(family)) {
                    continue;
                }

                // Create directory to save variants and family
                Path famOutPath = Files.createDirectories(getOutDir().resolve(familyId));
                if (!Files.exists(famOutPath)) {
                    throw new ToolException("Error creating directory: " + famOutPath);
                }

                // Export family variants (VCF format)
                // Create variant query
                String gt = getNoSomaticSampleIds(family, study, catalogManager, token).stream().map(s -> s + ":0/0,0/1,1/1")
                        .collect(Collectors.joining(";"));
                Query query = new Query()
                        .append(VariantQueryParam.STUDY.key(), study)
                        .append(VariantQueryParam.TYPE.key(), VariantType.SNV)
                        .append(VariantQueryParam.GENOTYPE.key(), gt)
                        .append(VariantQueryParam.REGION.key(), Arrays.asList("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22"
                                .split(",")));

                // Create query options
                QueryOptions queryOptions = new QueryOptions().append(QueryOptions.INCLUDE, "id,studies.samples");

                // Export to VCF.GZ format
                String basename = famOutPath.resolve(familyId).toAbsolutePath().toString();
                getVariantStorageManager().exportData(basename, VCF_GZ, null, query, queryOptions, token);

                // Check VCF file
                Path familyVcfPath = Paths.get(basename + "." + VCF_GZ.getExtension());
                if (!Files.exists(familyVcfPath)) {
                    throw new ToolException("Something wrong happened when exporting VCF file for family ID " + familyId + ". VCF file "
                            + familyVcfPath + " was not created. Export query = " + query.toJson() + "; export query options = "
                            + queryOptions.toJson());
                }
                familyVcfPaths.add(familyVcfPath);

                // Export family (JSON format)
                Path familyJsonPath = Paths.get(basename + "." + JSON.getExtension());
                objectWriter.writeValue(familyJsonPath.toFile(), family);

                // Check VCF file
                if (!Files.exists(familyJsonPath)) {
                    throw new ToolException("Something wrong happened when saving JSON file for family ID " + familyId + ". JSON file "
                            + familyJsonPath + " was not created.");
                }
                familyJsonPaths.add(familyJsonPath);

                // Add family to the list
                families.add(family);
            }
        } catch (CatalogException | IOException | StorageEngineException e) {
            throw new ToolException(e);
        }

        // Get executor to execute Python script that computes the family QC
        FamilyVariantQcAnalysisExecutor executor = getToolExecutor(FamilyVariantQcAnalysisExecutor.class);
        executor.setVcfPaths(familyVcfPaths)
                .setJsonPaths(familyJsonPaths)
                .setQcParams(analysisParams);

        // Step by step
        step(executor::execute);

        // Parse Python script results
        if (!Boolean.TRUE.equals(analysisParams.getSkipIndex())) {
            updateFamilyQualityControl(families);
        }
    }

    private boolean setComputingStatus(Family family) throws ToolException {
        try {
            QualityControlStatus qcStatus = new QualityControlStatus(COMPUTING, "Performing family QC");
            FamilyUpdateParams updateParams = new FamilyUpdateParams().setQualityControlStatus(qcStatus);
            catalogManager.getFamilyManager().update(getStudy(), family.getId(), updateParams, null, token);
        } catch (CatalogException e) {
            String msg = "Could not set status to COMPUTING before performing the QC for the family " + family.getId() + ": "
                    + e.getMessage();
            logger.error(msg);
            addError(new ToolException(msg, e));
            return false;
        }
        return true;
    }

    public static void checkParameters(FamilyQcAnalysisParams params, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        // Check study
        checkStudy(studyId, catalogManager, token);

        // Check permissions
        checkPermissions(WRITE_FAMILIES, studyId, catalogManager, token);

        // Sanity check
        if (CollectionUtils.isEmpty(params.getFamilies())) {
            throw new ToolException("Missing list of family IDs.");
        }

        // Check families in catalog
        // Collect possible errors in a map where key is the family ID and value is the error
        Map<String, String> errors = new HashMap<>();
        for (String familyId : params.getFamilies()) {
            // Get family from catalog
            Family family;
            try {
                OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(studyId, familyId, QueryOptions.empty(), token);
                if (familyResult.getNumResults() == 0) {
                    errors.put(familyId, "Family not found");
                } else if (familyResult.getNumResults() > 1) {
                    errors.put(familyId, "More than one family found");
                } else {
                    family = familyResult.first();

                    // Check number of samples
                    List<String> sampleIds = getNoSomaticSampleIds(family, studyId, catalogManager, token);
                    if (sampleIds.size() < 2) {
                        errors.put(familyId, "Too few samples found (" + sampleIds.size() + ") for that family members; minimum is 2 member"
                                + " samples");
                    }
                }
            } catch (CatalogException e) {
                errors.put(familyId, Arrays.toString(e.getStackTrace()));
            }
        }

        // Check error
        if (MapUtils.isNotEmpty(errors)) {
            throw new ToolException("Found the following error for family IDs: " + StringUtils.join(errors.entrySet().stream().map(
                    e -> "Family ID " + e.getKey() + ": " + e.getValue()).collect(Collectors.toList()), ","));
        }

        // Check relatedness files: pop. freq. file, pop. exclude var. file and threshold file
        checkRelatednessParameters(params.getRelatednessParams(), studyId, catalogManager, token);
    }

    private void updateFamilyQualityControl(List<Family> families) throws ToolException {
        final String extension = ".qc.json";
        ObjectMapper objectMapper = JacksonUtils.getDefaultObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        ObjectReader objectReader = JacksonUtils.getDefaultObjectMapper().readerFor(FamilyQualityControl.class);

        for (Family family : families) {
            FamilyQualityControl familyQc;
            QualityControlStatus qcStatus;

            // Check output file
            String msg;
            Path qcPath = getOutDir().resolve(family.getId()).resolve(family.getId() + extension);
            if (!Files.exists(qcPath)) {
                msg = "Quality control error for family " + family.getId() + ": file " + qcPath.getFileName() + " not found";
                familyQc = new FamilyQualityControl();
                qcStatus = new QualityControlStatus(NONE, msg);
                addError(new ToolException(msg));
                logger.error(msg);
            } else {
                try {
                    msg = "Computed successfully for family " + family.getId();
                    familyQc = objectReader.readValue(qcPath.toFile());
                    qcStatus = new QualityControlStatus(READY, msg);
                    logger.info(msg);
                } catch (IOException e) {
                    msg = "Quality control error for family " + family.getId() + ": error parsing JSON file " + qcPath.getFileName();
                    familyQc = new FamilyQualityControl();
                    qcStatus = new QualityControlStatus(NONE, msg);
                    addError(e);
                    logger.error(msg);
                }
            }

            try {
                // Update catalog: quality control and status
                FamilyUpdateParams updateParams = new FamilyUpdateParams()
                        .setQualityControl(familyQc)
                        .setQualityControlStatus(qcStatus);
                catalogManager.getFamilyManager().update(getStudy(), family.getId(), updateParams, null, token);
            } catch (CatalogException e) {
                logger.error("Could not update quality control in OpenCGA catalog for family {}: {}", family.getId(), e.getMessage());
                addError(e);
            }
        }
    }
}
