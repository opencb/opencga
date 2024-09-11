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

package org.opencb.opencga.analysis.variant.qc;

import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ResourceUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.QualityControlStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.family.FamilyUpdateParams;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualQualityControlStatus;
import org.opencb.opencga.core.models.individual.IndividualUpdateParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.analysis.AnalysisUtils.ANALYSIS_FOLDER;
import static org.opencb.opencga.analysis.AnalysisUtils.ANALYSIS_RESOURCES_FOLDER;
import static org.opencb.opencga.core.models.common.InternalStatus.READY;
import static org.opencb.opencga.core.models.common.QualityControlStatus.COMPUTING;
import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_SAMPLES;

public class VariantQcAnalysis extends OpenCgaToolScopeStudy {

    // QC folders
    public static final String QC_FOLDER = "qc/";
    public static final String RESOURCES_FOLDER = "resources/";
    public static final String QC_RESOURCES_FOLDER = QC_FOLDER + RESOURCES_FOLDER;

    public static final String QC_JSON_EXTENSION = ".qc.json";

    // Data type
    public static final String FAMILY_QC_TYPE = "family";
    public static final String INDIVIDUAL_QC_TYPE = "individual";
    public static final String SAMPLE_QC_TYPE = "sample";

    // For relatedness analysis
    public static final String RELATEDNESS_ANALYSIS_ID = "relatedness";
    protected static final String RELATEDNESS_POP_FREQ_FILENAME = "autosomes_1000G_QC_prune_in.frq";
    protected static final String RELATEDNESS_POP_FREQ_FILE_MSG = "Population frequency file";
    protected static final String RELATEDNESS_POP_EXCLUDE_VAR_FILENAME = "autosomes_1000G_QC.prune.out";
    protected static final String RELATEDNESS_POP_EXCLUDE_VAR_FILE_MSG = "Population exclude variant file";
    protected static final String RELATEDNESS_THRESHOLDS_FILENAME = "relatedness_thresholds.tsv";
    protected static final String RELATEDNESS_THRESHOLDS_FILE_MSG = "Relatedness thresholds file";

    // For inferred sex analysis
    public static final String INFERRED_SEX_ANALYSIS_ID = "inferred-sex";
    protected static final String INFERRED_SEX_THRESHOLDS_FILENAME = "karyotypic_sex_thresholds.json";
    protected static final String INFERRED_SEX_THRESHOLDS_FILE_MSG = "Karyotypic sex thresholds file";

    // For mendelian errors sex analysis
    public static final String MENDELIAN_ERROR_ANALYSIS_ID = "mendelian-errors";

    @Override
    protected void check() throws Exception {
        super.check();

        if (StringUtils.isEmpty(getStudy())) {
            throw new ToolException("Missing study");
        }

        // Check permissions
        try {
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(study, jwtPayload);
            String organizationId = studyFqn.getOrganizationId();
            String userId = jwtPayload.getUserId(organizationId);

            long studyUid = catalogManager.getStudyManager().get(getStudy(), QueryOptions.empty(), token).first().getUid();
            catalogManager.getAuthorizationManager().checkStudyPermission(organizationId, studyUid, userId, WRITE_SAMPLES);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected void run() throws Exception {
        // Nothing to do
    }

    protected static void checkStudy(String studyId, CatalogManager catalogManager, String token) throws ToolException {
        if (StringUtils.isEmpty(studyId)) {
            throw new ToolException("Missing study");
        }

        try {
            catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), token).first();
        } catch (CatalogException e) {
            throw new ToolException("Error accessing study ID '" + studyId + "'", e);
        }
    }

    protected static void checkPermissions(StudyPermissions.Permissions permissions, String studyId, CatalogManager catalogManager,
                                           String token) throws ToolException {
        checkStudy(studyId, catalogManager, token);

        try {
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, jwtPayload);
            String organizationId = studyFqn.getOrganizationId();
            String userId = jwtPayload.getUserId(organizationId);

            Study study = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), token).first();
            catalogManager.getAuthorizationManager().checkStudyPermission(organizationId, study.getUid(), userId, permissions);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    protected static Path checkResourcesDir(String resourcesDir, String studyId, CatalogManager catalogManager, String token)
            throws ToolException {
        Path path = null;
        if (StringUtils.isNotEmpty(resourcesDir)) {
            try {
                Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), resourcesDir);
                OpenCGAResult<File> fileResult = catalogManager.getFileManager().search(studyId, query, QueryOptions.empty(), token);
                if (fileResult.getNumResults() == 0) {
                    throw new ToolException("Could not find the resources path '" + resourcesDir + "' in OpenCGA catalog");
                }
                if (fileResult.getNumResults() > 1) {
                    throw new ToolException("Multiple results found (" + fileResult.getNumResults() + ") for resources path '"
                            + resourcesDir + "' in OpenCGA catalog");
                }
                path = Paths.get(fileResult.first().getUri());
                if (!Files.exists(path)) {
                    throw new ToolException("Resources path '" + path + "' does not exist (OpenCGA path: " + resourcesDir + ")");
                }
                return path;
            } catch (CatalogException e) {
                throw new ToolException("Error searching the OpenCGA catalog path '" + resourcesDir + "'", e);
            }
        }
        return path;
    }

    protected void prepareRelatednessResources(String resourcesDir) throws ToolException {
        Path path = checkResourcesDir(resourcesDir, getStudy(), getCatalogManager(), getToken());

        // Copy relatedness population frequency file
        copyQcResourceFile(path, RELATEDNESS_POP_FREQ_FILENAME);

        // Copy relatedness population exclude variant file
        copyQcResourceFile(path, RELATEDNESS_POP_FREQ_FILENAME);

        // Copy relatedness thresholds file
        copyQcResourceFile(path, RELATEDNESS_THRESHOLDS_FILENAME);
    }

    protected void prepareInferredSexResources(String resourcesDir) throws ToolException {
        Path path = checkResourcesDir(resourcesDir, getStudy(), getCatalogManager(), getToken());

        // Copy inferred sex thresholds file
        copyQcResourceFile(path, INFERRED_SEX_THRESHOLDS_FILENAME);
    }

    protected boolean setQualityControlStatus(QualityControlStatus qcStatus, String id, String qcType) throws ToolException {
        try {
            switch (qcType) {
                case FAMILY_QC_TYPE: {
                    FamilyUpdateParams updateParams = new FamilyUpdateParams().setQualityControlStatus(qcStatus);
                    catalogManager.getFamilyManager().update(getStudy(), id, updateParams, null, token);
                    break;
                }
                case INDIVIDUAL_QC_TYPE: {
                    IndividualUpdateParams updateParams = new IndividualUpdateParams()
                            .setQualityControlStatus((IndividualQualityControlStatus) qcStatus);
                    catalogManager.getIndividualManager().update(getStudy(), id, updateParams, null, token);
                    break;
                }
                default: {
                    String msg = "Internal error: unknown QC type '" + qcType + "' (valid values are: " + StringUtils.join(
                            Arrays.asList(FAMILY_QC_TYPE, INDIVIDUAL_QC_TYPE), ",") + ")";
                    throw new ToolException(msg);
                }
            }
        } catch (CatalogException e) {
            String msg = "Could not set status to COMPUTING before performing QC for " + qcType + " ID '" + id + "': " + e.getMessage();
            logger.error(msg);
            addError(new ToolException(msg, e));
            return false;
        }
        return true;
    }

    protected <T> T checkQcReport(String id, String analysisId, List<String> skip, Path qcPath, String qcType, ObjectReader reader)
            throws ToolException {
        if (CollectionUtils.isEmpty(skip) || !skip.contains(analysisId)) {
            java.io.File qcFile = qcPath.resolve(analysisId).resolve(id + QC_JSON_EXTENSION).toFile();
            try {
                return reader.readValue(qcFile);
            } catch (IOException e) {
                String msg = "Error parsing '" + analysisId + "' report (" + qcFile.getName() + " ) for " + qcType + " " + id;
                logger.error(msg, e);
                addError(new ToolException(msg, e));
            }
        }
        return null;
    }

    protected static List<String> getNoSomaticSampleIds(Family family, String studyId, CatalogManager catalogManager, String token)
            throws CatalogException {
        // Get list of individual IDs
        List<String> individualIds = family.getMembers().stream().map(m -> m.getId()).collect(Collectors.toList());

        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualIds);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "samples");

        List<String> sampleIds = new ArrayList<>();
        OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().search(studyId, query, queryOptions, token);
        for (Individual individual : individualResult.getResults()) {
            if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                sampleIds.addAll(getNoSomaticSampleIds(individual));
            }
        }
        return sampleIds;
    }

    protected static List<String> getNoSomaticSampleIds(Individual individual) {
        List<String> sampleIds = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(individual.getSamples())) {
            for (Sample sample : individual.getSamples()) {
                if (!sample.isSomatic()) {
                    // We take the first no somatic sample for each individual
                    sampleIds.add(sample.getId());
                    break;
                }
            }
        }
        return sampleIds;
    }

    protected void copyQcResourceFile(Path path, String resourceName) throws ToolException {
        // Copy resource file
        if (path == null || !Files.exists(path.resolve(resourceName))) {
            // Use the default resource file
            copyQcResourceFile(resourceName);
        } else {
            // Use the custom resource file
            copyQcResourceFile(path.resolve(resourceName));
        }
    }

    protected void copyQcResourceFile(String resourceName) throws ToolException {
        Path srcResourcesPath = getOpencgaHome().resolve(ANALYSIS_RESOURCES_FOLDER).resolve(QC_FOLDER);
        Path destResourcesPath = checkResourcesPath(getOutDir().resolve(RESOURCES_FOLDER));
        if (Files.exists(srcResourcesPath.resolve(resourceName))) {
            // Copy resource file
            copyQcResourceFile(srcResourcesPath.resolve(resourceName));
        } else {
            // Download directly into the job dir
            // It can be improved by downloading once (the first time) in the analysis resources folder
            URL url = null;
            try {
                url = new URL(ResourceUtils.URL + ANALYSIS_FOLDER + QC_FOLDER + "/" + resourceName);
                ResourceUtils.downloadThirdParty(url, destResourcesPath);
            } catch (IOException e) {
                throw new ToolException("Something wrong happened when downloading the resource '" + resourceName + "' from '"
                        + url + "'", e);
            }

            if (!Files.exists(destResourcesPath.resolve(resourceName))) {
                throw new ToolException("Error downloading the resource '" + resourceName + "', it does not exist at " + destResourcesPath);
            }
        }
    }

    protected void copyQcResourceFile(Path srcResourcesPath) throws ToolException {
        String resourceName = srcResourcesPath.getFileName().toString();
        Path destResourcesPath = checkResourcesPath(getOutDir().resolve(RESOURCES_FOLDER));

        String msg = "Error copying resource file '" + resourceName + "'";

        // Copy resource file
        try {
            Files.copy(srcResourcesPath, destResourcesPath.resolve(resourceName));
        } catch (IOException e) {
            if (!Files.exists(destResourcesPath.resolve(resourceName))
                    || srcResourcesPath.toFile().length() != destResourcesPath.resolve(resourceName).toFile().length()) {
                throw new ToolException(msg, e);
            }
            logger.warn(msg, e);
        }

        if (!Files.exists(destResourcesPath.resolve(resourceName))) {
            throw new ToolException(msg  + ", it does not exist at " + destResourcesPath);
        }
    }

    protected Path checkResourcesPath(Path resourcesPath) throws ToolException {
        if (!Files.exists(resourcesPath)) {
            try {
                Files.createDirectories(resourcesPath);
                if (!Files.exists(resourcesPath)) {
                    throw new ToolException("Something wrong happened when creating the resources folder at " + resourcesPath);
                }
            } catch (IOException e) {
                throw new ToolException("Error creating the resources folder at " + resourcesPath, e);
            }
        }
        return resourcesPath;
    }

    protected boolean performQualityControl(QualityControlStatus qcStatus, Boolean overwrite) {
        boolean performQc;
        if (Boolean.TRUE.equals(overwrite)) {
            performQc = true;
        } else if (qcStatus != null) {
            String statusId = qcStatus.getId();
            performQc = !(statusId.equals(COMPUTING) || statusId.equals(READY));
        } else {
            performQc = true;
        }
        return performQc;
    }
}
