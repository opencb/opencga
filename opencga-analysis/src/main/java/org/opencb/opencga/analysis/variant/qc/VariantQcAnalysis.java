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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.CatalogFqn;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.JwtPayload;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opencb.opencga.core.models.study.StudyPermissions.Permissions.WRITE_SAMPLES;

public class VariantQcAnalysis extends OpenCgaToolScopeStudy {

    // QC folders
    private static final String QC = "qc";

    public static final String QC_FOLDER = QC + "/";
    public static final String RESOURCES_FOLDER = "resources/";
    public static final String QC_RESOURCES_FOLDER = QC_FOLDER + RESOURCES_FOLDER;

    public static final String QC_RESULTS_FILENAME = "results.json";

    // Data type
    public static final String FAMILY_QC_TYPE = "family";
    public static final String INDIVIDUAL_QC_TYPE = "individual";
    public static final String SAMPLE_QC_TYPE = "sample";

    // For relatedness analysis
    public static final String RELATEDNESS_ANALYSIS_ID = "relatedness";

    // For inferred sex analysis
    public static final String INFERRED_SEX_ANALYSIS_ID = "inferred-sex";

    // For mendelian errors sex analysis
    public static final String MENDELIAN_ERROR_ANALYSIS_ID = "mendelian-error";

    // For signature analysis
    public static final String SIGNATURE_ANALYSIS_ID = "signature";

    // For genome plot analysis
    public static final String GENOME_PLOT_ANALYSIS_ID = "genome-plot";

    // Tool QC steps
    protected static final String PREPARE_RESOURCES_STEP = "prepare-resources";
    protected static final String PREPARE_QC_STEP = "prepare-qc";
    protected static final String INDEX_QC_STEP = "index-qc";

    // Messages
    protected static final String FAILURE_ERROR_PARSING_QC_JSON_FILE = "Failure: error parsing QC JSON file '";
    protected static final String FAILURE_FILE = "Failure: file '";
    protected static final String NOT_FOUND = "' not found";
    protected static final String FAILURE_COULD_NOT_UPDATE_QUALITY_CONTROL_IN_OPEN_CGA_CATALOG = "Failure: Could not update quality control"
            + " in OpenCGA catalog";
    protected static final String SUCCESS = "Success";

    // Common attributes
    public static final String OPENCGA_JOB_ID_ATTR = "OPENCGA_JOB_ID";

    protected LinkedList<Path> vcfPaths = new LinkedList<>();
    protected LinkedList<Path> jsonPaths = new LinkedList<>();

    protected Path userResourcesPath;
    protected Set<String> failedQcSet;

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

    //-------------------------------------------------------------------------

    protected void clean() {
        deleteFiles(vcfPaths);
        deleteFiles(jsonPaths);
    }

    private void deleteFiles(List<Path> paths) {
        for (Path path : paths) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                try {
                    addWarning("Could not delete file '" + path + "'");
                } catch (ToolException ex) {
                    logger.warn("When deleting file '" + path + "'", e);
                }
            }
        }
    }

    //-------------------------------------------------------------------------
    // CHECKS MANAGEMENT
    //-------------------------------------------------------------------------

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

    protected static Path checkUserResourcesDir(String userResourcesDir, String studyId, CatalogManager catalogManager, String token) throws ToolException {
        Path userResourcesPath = null;
        if (!StringUtils.isEmpty(userResourcesDir)) {
            try {
                File file = catalogManager.getFileManager().get(studyId, userResourcesDir, QueryOptions.empty(), token).first();
                userResourcesPath = Paths.get(file.getUri().getPath());
                if (!Files.exists(userResourcesPath)) {
                    throw new ToolException("User resources path does not exist: " + userResourcesPath);
                }
            } catch (CatalogException e) {
                throw new ToolException("Error accessing user resources dir '" + userResourcesDir + "'", e);
            }
        }
        return userResourcesPath;
    }

    protected static void checkPermissions(StudyPermissions.Permissions permissions, Boolean skipIndex, String studyId,
                                           CatalogManager catalogManager, String token) throws ToolException {
        checkStudy(studyId, catalogManager, token);

        try {
            JwtPayload jwtPayload = catalogManager.getUserManager().validateToken(token);
            CatalogFqn studyFqn = CatalogFqn.extractFqnFromStudy(studyId, jwtPayload);
            String organizationId = studyFqn.getOrganizationId();
            String userId = jwtPayload.getUserId(organizationId);

            // Check acess permissions
            Study study = catalogManager.getStudyManager().get(studyId, QueryOptions.empty(), token).first();
            catalogManager.getAuthorizationManager().checkStudyPermission(organizationId, study.getUid(), userId, permissions);

            // Check admin permissions to Catalog index
            if (!Boolean.TRUE.equals(skipIndex)) {
                catalogManager.getAuthorizationManager().checkIsAtLeastOrganizationOwnerOrAdmin(organizationId, userId);
            }
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

                // TODO: Check permissions to read
            } catch (CatalogException e) {
                throw new ToolException("Error searching the OpenCGA catalog path '" + resourcesDir + "'", e);
            }
        }
        return path;
    }

    protected void checkFailedQcCounter(int size, String individualQcType) throws ToolException {
        if (CollectionUtils.isNotEmpty(failedQcSet) && failedQcSet.size() == size) {
            // If all QC fail, then the job fails
            clean();
            throw new ToolException("All " + individualQcType + " QCs fail. Please, check job results and logs for more details.");
        }
    }

    protected Path getCustomResourcePath(String fileEntry, String token, String resourceDesc) throws CatalogException, ToolException {
        File file = getCatalogManager().getFileManager().get(study, fileEntry, QueryOptions.empty(), token).first();
        Path path = Paths.get(file.getUri());
        if (!Files.exists(path)) {
            throw new ToolException(resourceDesc + " does not exist: " + path.toAbsolutePath());
        }
        return path;
    }

    //-------------------------------------------------------------------------
    // QC file result management
    //-------------------------------------------------------------------------

    protected boolean isQcArray(Path qcPath) throws ToolException {
        try {
            // Create an ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            // Read the JSON file into a JsonNode
            JsonNode rootNode = objectMapper.readTree(qcPath.toFile());

            // Check if the root element is an array
            return rootNode.isArray();
        } catch (IOException e) {
            throw new ToolException("Error checking QC file '" + qcPath + "'", e);
        }
    }

    protected void addCommonAttributes(ObjectMap attributes) {
        if (attributes != null) {
            attributes.append(OPENCGA_JOB_ID_ATTR, getJobId());
        } else {
            String msg = "Could not add common attributes, such as " + OPENCGA_JOB_ID_ATTR;
            try {
                addWarning(msg);
            } catch (ToolException e) {
                logger.warn(msg, e);
            }
        }
    }

    protected <T> T parseQcFile(String id, String analysisId, List<String> skip, Path qcPath, String qcType, ObjectReader reader)
            throws ToolException {
        if (CollectionUtils.isEmpty(skip) || !skip.contains(analysisId)) {
            java.io.File qcFile = qcPath.resolve(analysisId).resolve(QC_RESULTS_FILENAME).toFile();
            if (qcFile.exists()) {
                try {
                    return reader.readValue(qcFile);
                } catch (IOException e) {
                    String msg = "Error parsing '" + analysisId + "' report (" + qcFile.getName() + " ) for " + qcType + " " + id;
                    logger.error(msg, e);
                    addError(new ToolException(msg, e));
                }
            }
        }
        return null;
    }

    //-------------------------------------------------------------------------
    // Catalog utils
    //-------------------------------------------------------------------------

    protected static List<String> getIndexedAndNoSomaticSampleIds(Family family, String studyId, CatalogManager catalogManager, String token)
            throws CatalogException {
        // Get list of individual IDs
        List<String> individualIds = family.getMembers().stream().map(m -> m.getId()).collect(Collectors.toList());

        Query query = new Query(IndividualDBAdaptor.QueryParams.ID.key(), individualIds);
        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, "samples");

        List<String> sampleIds = new ArrayList<>();
        OpenCGAResult<Individual> individualResult = catalogManager.getIndividualManager().search(studyId, query, queryOptions, token);
        for (Individual individual : individualResult.getResults()) {
            if (CollectionUtils.isNotEmpty(individual.getSamples())) {
                sampleIds.addAll(getIndexedAndNoSomaticSampleIds(individual));
            }
        }
        return sampleIds;
    }

    protected static List<String> getIndexedAndNoSomaticSampleIds(Individual individual) {
        List<String> sampleIds = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(individual.getSamples())) {
            for (Sample sample : individual.getSamples()) {
                if (isSampleIndexed(sample) && !sample.isSomatic()) {
                    // We take the first no somatic sample for each individual
                    sampleIds.add(sample.getId());
                    break;
                }
            }
        }
        return sampleIds;
    }

    protected static boolean isSampleIndexed(Sample sample) {
        if (sample.getInternal() != null
                && sample.getInternal().getVariant() != null
                && sample.getInternal().getVariant().getIndex() != null
                && sample.getInternal().getVariant().getIndex().getStatus() != null
                && InternalStatus.READY.equals(sample.getInternal().getVariant().getIndex().getStatus().getId())) {
            return true;
        }
        return false;
    }

    //-------------------------------------------------------------------------
    // QC RESOURCES MANAGEMENT
    //-------------------------------------------------------------------------

//    protected void prepareResources() throws ToolException {
//        // Check resources are available
//        Path destResourcesPath = checkResourcesPath(getOutDir().resolve(RESOURCES_FOLDER));
//        if (userResourcesPath != null) {
//            // If necessary, copy the user resource files
//            copyUserResourceFiles();
//        }
//    }

    protected void copyUserResourceFiles() throws ToolException {
        // Sanity check
        if (userResourcesPath != null && Files.exists(userResourcesPath)) {
            copyUserResourceFiles(userResourcesPath);
        }
    }

    protected void copyUserResourceFiles(Path inputPath) throws ToolException {
        Path destResourcesPath = checkResourcesPath(getOutDir().resolve(RESOURCES_FOLDER));

        // Copy custom resource files to the job dir
        for (java.io.File file : inputPath.toFile().listFiles()) {
            Path destPath = destResourcesPath.resolve(file.getName());
            if (file.isFile()) {
                try {
                    Files.copy(file.toPath(), destPath, StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    if (!Files.exists(destPath) || destPath.toFile().length() != file.length()) {
                        throw new ToolException("Error copying resource file '" + file + "'", e);
                    }
                }
            }
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

    protected String getIdLogMessage(String id, String qcType) {
        return " for " + qcType + " '" + id + "'";
    }
}
