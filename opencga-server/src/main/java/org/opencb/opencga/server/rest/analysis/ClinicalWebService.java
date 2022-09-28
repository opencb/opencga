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

package org.opencb.opencga.server.rest.analysis;

import org.opencb.opencga.analysis.clinical.exomiser.ExomiserInterpretationAnalysis;
import org.opencb.opencga.core.models.AclEntryList;
import org.opencb.opencga.core.tools.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.analysis.clinical.rga.AuxiliarRgaAnalysis;
import org.opencb.opencga.analysis.clinical.rga.RgaAnalysis;
import org.opencb.opencga.analysis.clinical.team.TeamInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.CancerTieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.tiering.TieringInterpretationAnalysis;
import org.opencb.opencga.analysis.clinical.zetta.ZettaInterpretationAnalysis;
import org.opencb.opencga.analysis.rga.RgaManager;
import org.opencb.opencga.analysis.rga.RgaQueryParams;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.managers.InterpretationManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.AclParams;
import org.opencb.opencga.core.models.analysis.knockout.*;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.study.configuration.ClinicalAnalysisStudyConfiguration;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.SAVED_FILTER_DESCR;
import static org.opencb.opencga.core.api.ParamConstants.INCLUDE_INTERPRETATION;
import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;
import static org.opencb.opencga.server.rest.analysis.VariantWebService.getVariantQuery;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

@Path("/{apiVersion}/analysis/clinical")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Clinical", position = 4, description = "Methods for working with Clinical Interpretations")
public class ClinicalWebService extends AnalysisWebService {

    private final ClinicalAnalysisManager clinicalManager;
    private final InterpretationManager catalogInterpretationManager;
    private final ClinicalInterpretationManager clinicalInterpretationManager;
    public static final AtomicReference<RgaManager> rgaManagerAtomicRef = new AtomicReference<>();
    public static final AtomicReference<VariantStorageManager> variantStorageManagerAtomicRef = new AtomicReference<>();

    public ClinicalWebService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);

        clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager, storageEngineFactory, opencgaHome);
        catalogInterpretationManager = catalogManager.getInterpretationManager();
        clinicalManager = catalogManager.getClinicalAnalysisManager();
    }

    private RgaManager getRgaManager() {
        RgaManager rgaManager = rgaManagerAtomicRef.get();
        if (rgaManager == null) {
            synchronized (rgaManagerAtomicRef) {
                rgaManager = rgaManagerAtomicRef.get();
                if (rgaManager == null) {
                    VariantStorageManager variantStorageManager = getVariantStorageManager();
                    rgaManager = new RgaManager(catalogManager, variantStorageManager);
                    rgaManagerAtomicRef.set(rgaManager);
                }
            }
        }
        return rgaManager;
    }

    private VariantStorageManager getVariantStorageManager() {
        VariantStorageManager variantStorageManager = variantStorageManagerAtomicRef.get();
        if (variantStorageManager == null) {
            synchronized (variantStorageManagerAtomicRef) {
                variantStorageManager = variantStorageManagerAtomicRef.get();
                if (variantStorageManager == null) {
                    variantStorageManager = new VariantStorageManager(catalogManager, storageEngineFactory);
                    variantStorageManagerAtomicRef.set(variantStorageManager);
                }
            }
        }
        return variantStorageManager;
    }

//    public ClinicalWebService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
//                              @Context HttpHeaders httpHeaders) throws IOException, VersionException {
//        super(version, uriInfo, httpServletRequest, httpHeaders);
//
//        clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager, storageEngineFactory,
//                Paths.get(opencgaHome + "/analysis/resources/roleInCancer.txt"),
//                Paths.get(opencgaHome + "/analysis/resources/"));
//        catalogInterpretationManager = catalogManager.getInterpretationManager();
//        clinicalManager = catalogManager.getClinicalAnalysisManager();
//    }

    @POST
    @Path("/clinical/configuration/update")
    @ApiOperation(value = "Update Clinical Analysis configuration.", response = ObjectMap.class)
    public Response clinicalConfigure(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Configuration params to update") ClinicalAnalysisStudyConfiguration params) {
        return run(() -> clinicalManager.configureStudy(study, params, token));
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new clinical analysis", response = ClinicalAnalysis.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response create(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.CLINICAL_ANALYSIS_SKIP_CREATE_DEFAULT_INTERPRETATION_DESCRIPTION)
            @QueryParam(ParamConstants.CLINICAL_ANALYSIS_SKIP_CREATE_DEFAULT_INTERPRETATION_PARAM) boolean skipCreateInterpretation,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(name = "body", value = "JSON containing clinical analysis information", required = true)
                    ClinicalAnalysisCreateParams params) {
        try {
            return createOkResponse(clinicalManager.create(studyStr, params.toClinicalAnalysis(), skipCreateInterpretation, queryOptions,
                    token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update clinical analysis attributes", response = ClinicalAnalysis.class, hidden = true)
    public Response update(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Clinical analysis type") @QueryParam("type") String type,
            @ApiParam(value = "Priority") @QueryParam("priority") String priority,
            @ApiParam(value = "Clinical analysis status") @QueryParam("status") String status,
            @ApiParam(value = ParamConstants.CREATION_DATE_DESCRIPTION)
            @QueryParam("creationDate") String creationDate,
            @ApiParam(value = ParamConstants.MODIFICATION_DATE_DESCRIPTION)
            @QueryParam("modificationDate") String modificationDate,
            @ApiParam(value = "Due date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)") @QueryParam("dueDate") String dueDate,
            @ApiParam(value = "Description") @QueryParam("description") String description,
            @ApiParam(value = "Family") @QueryParam("family") String family,
            @ApiParam(value = "Proband") @QueryParam("proband") String proband,
            @ApiParam(value = "Proband sample") @QueryParam("sample") String sample,
            @ApiParam(value = "Clinical analyst assignee") @QueryParam("analystAssignee") String assignee,
            @ApiParam(value = "Disorder ID or name") @QueryParam("disorder") String disorder,
            @ApiParam(value = "Flags") @QueryParam("flags") String flags,
            @ApiParam(value = "Release value") @QueryParam("release") String release,
            @ApiParam(value = "Text attributes (Format: sex=male,age>20 ...)") @QueryParam("attributes") String attributes,

            @ApiParam(name = "body", value = "JSON containing clinical analysis information", required = true)
                    ClinicalAnalysisUpdateParams params) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            return createOkResponse(clinicalManager.update(studyStr, query, params, true, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{clinicalAnalyses}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update clinical analysis attributes", response = ClinicalAnalysis.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response update(
            @ApiParam(value = "Comma separated list of clinical analysis IDs") @PathParam(value = "clinicalAnalyses") String clinicalAnalysisStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed if the array of comments is being updated.", allowableValues = "ADD,REMOVE,REPLACE", defaultValue = "ADD")
            @QueryParam("commentsAction") ParamUtils.AddRemoveReplaceAction commentsAction,
            @ApiParam(value = "Action to be performed if the array of flags is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("flagsAction") ParamUtils.BasicUpdateAction flagsAction,
            @ApiParam(value = "Action to be performed if the array of files is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("filesAction") ParamUtils.BasicUpdateAction filesAction,
            @ApiParam(value = "Action to be performed if the array of panels is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("panelsAction") ParamUtils.BasicUpdateAction panelsAction,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(name = "body", value = "JSON containing clinical analysis information", required = true) ClinicalAnalysisUpdateParams params) {
        try {
            if (commentsAction == null) {
                commentsAction = ParamUtils.AddRemoveReplaceAction.ADD;
            }
            if (flagsAction == null) {
                flagsAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (filesAction == null) {
                filesAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (panelsAction == null) {
                panelsAction = ParamUtils.BasicUpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.COMMENTS.key(), commentsAction);
            actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), flagsAction);
            actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.FILES.key(), filesAction);
            actionMap.put(ClinicalAnalysisDBAdaptor.QueryParams.PANELS.key(), panelsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            return createOkResponse(clinicalManager.update(studyStr, getIdList(clinicalAnalysisStr), params, true, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{clinicalAnalyses}/delete")
    @ApiOperation(value = "Delete clinical analyses", response = ClinicalAnalysis.class)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Force deletion if the ClinicalAnalysis contains interpretations or is locked", defaultValue = "false") @QueryParam(Constants.FORCE) boolean force,
            @ApiParam(value = ParamConstants.CLINICAL_ANALYSES_DESCRIPTION) @PathParam(ParamConstants.CLINICAL_ANALYSES_PARAM) String clinicalAnalyses) {
        try {
            return createOkResponse(clinicalManager.delete(studyStr, getIdList(clinicalAnalyses), queryOptions, true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{clinicalAnalysis}/info")
    @ApiOperation(value = "Clinical analysis info", response = ClinicalAnalysis.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query")
    })
    public Response info(
            @ApiParam(value = ParamConstants.CLINICAL_ANALYSES_DESCRIPTION) @PathParam(value = "clinicalAnalysis") String clinicalAnalysisStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("clinicalAnalysis");

            List<String> analysisList = getIdList(clinicalAnalysisStr);
            DataResult<ClinicalAnalysis> analysisResult = clinicalManager.get(studyStr, analysisList, queryOptions, true, token);
            return createOkResponse(analysisResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Clinical analysis search.", response = ClinicalAnalysis.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, defaultValue = "false", dataType = "boolean", paramType = "query")
    })
    public Response search(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.CLINICAL_ID_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.CLINICAL_UUID_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.CLINICAL_TYPE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.CLINICAL_DISORDER_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_DISORDER_PARAM) String disorder,
            @ApiParam(value = ParamConstants.CLINICAL_FILES_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FILES_PARAM) String files,
            @ApiParam(value = ParamConstants.CLINICAL_SAMPLE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_SAMPLE_PARAM) String sample,
            @ApiParam(value = ParamConstants.CLINICAL_INDIVIDUAL_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_INDIVIDUAL_PARAM) String individual,
            @ApiParam(value = ParamConstants.CLINICAL_PROBAND_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PROBAND_PARAM) String proband,
            @ApiParam(value = ParamConstants.CLINICAL_PROBAND_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PROBAND_SAMPLES_PARAM) String probandSamples,
            @ApiParam(value = ParamConstants.CLINICAL_FAMILY_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FAMILY_PARAM) String family,
            @ApiParam(value = ParamConstants.CLINICAL_FAMILY_MEMBERS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FAMILY_MEMBERS_PARAM) String familyMembers,
            @ApiParam(value = ParamConstants.CLINICAL_FAMILY_MEMBERS_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FAMILY_MEMBERS_SAMPLES_PARAM) String familyMembersSamples,
            @ApiParam(value = ParamConstants.CLINICAL_PANELS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PANELS_PARAM) String panels,
            @ApiParam(value = ParamConstants.CLINICAL_LOCKED_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_LOCKED_PARAM) Boolean locked,
            @ApiParam(value = ParamConstants.CLINICAL_ANALYST_ID_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_ANALYST_ID_PARAM) String clinicalAnalystId,
            @ApiParam(value = ParamConstants.CLINICAL_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.CLINICAL_FLAGS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FLAGS_PARAM) String flags,
            @ApiParam(value = ParamConstants.CLINICAL_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.CLINICAL_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.CLINICAL_DUE_DATE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_DUE_DATE_PARAM) String dueDate,
            @ApiParam(value = ParamConstants.CLINICAL_QUALITY_CONTROL_SUMMARY_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_QUALITY_CONTROL_SUMMARY_PARAM) String qualityControl,
            @ApiParam(value = ParamConstants.CLINICAL_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.CLINICAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.CLINICAL_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION) @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            DataResult<ClinicalAnalysis> queryResult = clinicalManager.search(studyStr, query, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/distinct")
    @ApiOperation(value = "Clinical Analysis distinct method")
    public Response distinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.CLINICAL_ID_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.CLINICAL_UUID_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.CLINICAL_TYPE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_TYPE_PARAM) String type,
            @ApiParam(value = ParamConstants.CLINICAL_DISORDER_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_DISORDER_PARAM) String disorder,
            @ApiParam(value = ParamConstants.CLINICAL_FILES_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FILES_PARAM) String files,
            @ApiParam(value = ParamConstants.CLINICAL_SAMPLE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_SAMPLE_PARAM) String sample,
            @ApiParam(value = ParamConstants.CLINICAL_INDIVIDUAL_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_INDIVIDUAL_PARAM) String individual,
            @ApiParam(value = ParamConstants.CLINICAL_PROBAND_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PROBAND_PARAM) String proband,
            @ApiParam(value = ParamConstants.CLINICAL_PROBAND_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PROBAND_SAMPLES_PARAM) String probandSamples,
            @ApiParam(value = ParamConstants.CLINICAL_FAMILY_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FAMILY_PARAM) String family,
            @ApiParam(value = ParamConstants.CLINICAL_FAMILY_MEMBERS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FAMILY_MEMBERS_PARAM) String familyMembers,
            @ApiParam(value = ParamConstants.CLINICAL_FAMILY_MEMBERS_SAMPLES_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FAMILY_MEMBERS_SAMPLES_PARAM) String familyMembersSamples,
            @ApiParam(value = ParamConstants.CLINICAL_PANELS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PANELS_PARAM) String panels,
            @ApiParam(value = ParamConstants.CLINICAL_LOCKED_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_LOCKED_PARAM) Boolean locked,
            @ApiParam(value = ParamConstants.CLINICAL_ANALYST_ID_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_ANALYST_ID_PARAM) String clinicalAnalystId,
            @ApiParam(value = ParamConstants.CLINICAL_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_PRIORITY_PARAM) String priority,
            @ApiParam(value = ParamConstants.CLINICAL_FLAGS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_FLAGS_PARAM) String flags,
            @ApiParam(value = ParamConstants.CLINICAL_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.CLINICAL_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.CLINICAL_DUE_DATE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_DUE_DATE_PARAM) String dueDate,
            @ApiParam(value = ParamConstants.CLINICAL_QUALITY_CONTROL_SUMMARY_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_QUALITY_CONTROL_SUMMARY_PARAM) String qualityControl,
            @ApiParam(value = ParamConstants.CLINICAL_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_RELEASE_PARAM) String release,
            @ApiParam(value = ParamConstants.CLINICAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.CLINICAL_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.CLINICAL_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION) @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);
            return createOkResponse(clinicalManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{clinicalAnalyses}/acl")
    @ApiOperation(value = "Returns the acl of the clinical analyses. If member is provided, it will only return the acl for the member.",
            response = AclEntryList.class)
    public Response getAcls(
            @ApiParam(value = ParamConstants.CLINICAL_ANALYSES_DESCRIPTION, required = true)
            @PathParam("clinicalAnalyses") String clinicalAnalysis,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "User or group ID") @QueryParam("member") String member,
            @ApiParam(value = ParamConstants.SILENT_DESCRIPTION,
                    defaultValue = "false") @QueryParam(Constants.SILENT) boolean silent) {
        try {
            List<String> idList = getIdList(clinicalAnalysis);
            return createOkResponse(clinicalManager.getAcls(studyStr, idList, member, silent, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{members}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", response = AclEntryList.class)
    public Response updateAcl(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of user or group IDs", required = true) @PathParam("members") String memberId,
            @ApiParam(value = ParamConstants.ACL_ACTION_DESCRIPTION, required = true, defaultValue = "ADD") @QueryParam(ParamConstants.ACL_ACTION_PARAM) ParamUtils.AclAction action,
            @ApiParam(value = "Propagate permissions to related families, individuals, samples and files", defaultValue = "false") @QueryParam("propagate") boolean propagate,
            @ApiParam(value = "JSON containing the parameters to add ACLs", required = true) ClinicalAnalysisAclUpdateParams params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new ClinicalAnalysisAclUpdateParams());
            AclParams clinicalAclParams = new AclParams(params.getPermissions());
            List<String> idList = getIdList(params.getClinicalAnalysis());
            return createOkResponse(clinicalManager.updateAcl(studyStr, idList, memberId, clinicalAclParams, action, propagate, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/index")
//    @ApiOperation(value = "Index clinical analysis interpretations in the clinical variant database", response = Map.class)
//    public Response index(@ApiParam(value = "Comma separated list of clinical analysis IDs to be indexed in the clinical variant database")
//                          @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
//                          @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study) {
//        try {
//            clinicalInterpretationManager.index(study, sessionId);
//            return Response.ok().build();
//        } catch (IOException | ClinicalVariantException | CatalogException e) {
//            return createErrorResponse(e);
//        }
//        return createErrorResponse(new NotImplementedException("Operation not yet implemented"));
//    }


    /*
     * INTERPRETATION METHODS
     */

    @POST
    @Path("/{clinicalAnalysis}/interpretation/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new Interpretation", response = Interpretation.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response create(
            @ApiParam(value = "Clinical analysis ID") @PathParam("clinicalAnalysis") String clinicalId,
            @ApiParam(value = "[[user@]project:]study id") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Set interpretation as", allowableValues = "PRIMARY,SECONDARY", defaultValue = "SECONDARY")
            @QueryParam("setAs") ParamUtils.SaveInterpretationAs setAs,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(name = "body", value = "JSON containing clinical interpretation information", required = true)
                    InterpretationCreateParams params) {
        try {
            if (setAs == null) {
                setAs = ParamUtils.SaveInterpretationAs.SECONDARY;
            }
            return createOkResponse(catalogInterpretationManager.create(studyStr, clinicalId, params.toClinicalInterpretation(), setAs,
                    queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{clinicalAnalysis}/interpretation/{interpretation}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update interpretation fields", response = Interpretation.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    dataType = "string", paramType = "query")
    })
    public Response updateInterpretation(
            @ApiParam(value = "[[user@]project:]study ID") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Action to be performed if the array of primary findings is being updated.",
                    allowableValues = "ADD,SET,REMOVE,REPLACE", defaultValue = "ADD")
            @QueryParam("primaryFindingsAction") ParamUtils.UpdateAction primaryFindingsAction,
            @ApiParam(value = "Action to be performed if the array of methods is being updated.",
                    allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
            @QueryParam("methodsAction") ParamUtils.BasicUpdateAction methodsAction,
            @ApiParam(value = "Action to be performed if the array of secondary findings is being updated.",
                    allowableValues = "ADD,SET,REMOVE,REPLACE", defaultValue = "ADD")
            @QueryParam("secondaryFindingsAction") ParamUtils.UpdateAction secondaryFindingsAction,
            @ApiParam(value = "Action to be performed if the array of comments is being updated. To REMOVE or REPLACE, the date will need to be provided to identify the comment.",
                    allowableValues = "ADD,REMOVE,REPLACE", defaultValue = "ADD") @QueryParam("commentsAction") ParamUtils.AddRemoveReplaceAction commentsAction,
            @ApiParam(value = "Action to be performed if the array of panels is being updated.", allowableValues = "ADD,SET,REMOVE", defaultValue = "ADD")
                @QueryParam("panelsAction") ParamUtils.BasicUpdateAction panelsAction,
            @ApiParam(value = "Set interpretation as", allowableValues = "PRIMARY,SECONDARY") @QueryParam("setAs") ParamUtils.SaveInterpretationAs setAs,
            @ApiParam(value = "Clinical analysis ID") @PathParam("clinicalAnalysis") String clinicalId,
            @ApiParam(value = "Interpretation ID") @PathParam("interpretation") String interpretationId,
            @ApiParam(value = ParamConstants.INCLUDE_RESULT_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.INCLUDE_RESULT_PARAM) boolean includeResult,
            @ApiParam(name = "body", value = "JSON containing clinical interpretation information", required = true)
                    InterpretationUpdateParams params) {
        try {
            if (primaryFindingsAction == null) {
                primaryFindingsAction = ParamUtils.UpdateAction.ADD;
            }
            if (secondaryFindingsAction == null) {
                secondaryFindingsAction = ParamUtils.UpdateAction.ADD;
            }
            if (commentsAction == null) {
                commentsAction = ParamUtils.AddRemoveReplaceAction.ADD;
            }
            if (methodsAction == null) {
                methodsAction = ParamUtils.BasicUpdateAction.ADD;
            }
            if (panelsAction == null) {
                panelsAction = ParamUtils.BasicUpdateAction.ADD;
            }

            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(InterpretationDBAdaptor.QueryParams.PRIMARY_FINDINGS.key(), primaryFindingsAction);
            actionMap.put(InterpretationDBAdaptor.QueryParams.SECONDARY_FINDINGS.key(), secondaryFindingsAction);
            actionMap.put(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), commentsAction);
            actionMap.put(InterpretationDBAdaptor.QueryParams.METHOD.key(), methodsAction);
            actionMap.put(InterpretationDBAdaptor.QueryParams.PANELS.key(), panelsAction);
            queryOptions.put(Constants.ACTIONS, actionMap);

            return createOkResponse(catalogInterpretationManager.update(studyStr, clinicalId, interpretationId, params, setAs, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{clinicalAnalysis}/interpretation/{interpretation}/revert")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Revert to a previous interpretation version", response = Interpretation.class)
    public Response revertInterpretation(
            @ApiParam(value = "[[user@]project:]study ID") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Clinical analysis ID") @PathParam("clinicalAnalysis") String clinicalId,
            @ApiParam(value = "Interpretation ID") @PathParam("interpretation") String interpretationId,
            @ApiParam(value = "Version to revert to", required = true) @QueryParam("version") int version) {
        try {
            return createOkResponse(catalogInterpretationManager.revert(studyStr, clinicalId, interpretationId, version, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @DELETE
    @Path("/{clinicalAnalysis}/interpretation/{interpretations}/delete")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete interpretation", response = Interpretation.class)
    public Response deleteInterpretation(
            @ApiParam(value = "[[user@]project:]study ID") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Clinical analysis ID") @PathParam("clinicalAnalysis") String clinicalId,
            @ApiParam(value = "Interpretation IDs of the Clinical Analysis") @PathParam("interpretations") String interpretations,
            @ApiParam(value = "Interpretation id to set as primary from the list of secondaries in case of deleting the actual primary one")
            @QueryParam("setAsPrimary") String newPrimaryInterpretation) {
        try {
            return createOkResponse(catalogInterpretationManager.delete(studyStr, clinicalId, getIdList(interpretations), true, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{clinicalAnalysis}/interpretation/{interpretations}/clear")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Clear the fields of the main interpretation of the Clinical Analysis", response = Interpretation.class)
    public Response clearInterpretation(
            @ApiParam(value = "[[user@]project:]study ID") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Interpretation IDs of the Clinical Analysis") @PathParam("interpretations") String interpretations,
            @ApiParam(value = "Clinical analysis ID") @PathParam("clinicalAnalysis") String clinicalId) {
        try {
            return createOkResponse(catalogInterpretationManager.clear(studyStr, clinicalId, getIdList(interpretations), token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @POST
//    @Path("/{clinicalAnalysis}/interpretations/{interpretation}/comments/update")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Update comments of an Interpretation",
//            response = org.opencb.biodata.models.clinical.interpretation.Interpretation.class, hidden = true)
//    public Response commentsUpdate(
//            @ApiParam(value = "[[user@]project:]study ID") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "Clinical analysis ID") @PathParam("clinicalAnalysis") String clinicalId,
//            @ApiParam(value = "Interpretation ID") @PathParam("interpretation") String interpretationId,
//            @ApiParam(value = "Action to be performed.", defaultValue = "ADD") @QueryParam("action") ParamUtils.UpdateAction action,
//            @ApiParam(name = "body", value = "JSON containing a list of comments", required = true)
//                    List<ClinicalComment> comments) {
//        try {
//            InterpretationUpdateParams updateParams = new InterpretationUpdateParams().setComments(comments);
//
//            Map<String, Object> actionMap = new HashMap<>();
//            actionMap.put(InterpretationDBAdaptor.QueryParams.COMMENTS.key(), action.name());
//            queryOptions.put(Constants.ACTIONS, actionMap);
//
//            return createOkResponse(catalogInterpretationManager.update(studyStr, clinicalId, interpretationId, updateParams, null, queryOptions,
//                    token));
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @GET
    @Path("/interpretation/{interpretations}/info")
    @ApiOperation(value = "Clinical interpretation information", response = Interpretation.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query")
    })
    public Response interpretationInfo(
            @ApiParam(value = ParamConstants.INTERPRETATION_DESCRIPTION) @PathParam(value = "interpretations") String interpretations,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Comma separated list of interpretation versions. 'all' to get all the interpretation versions. "
                    + "Not supported if multiple interpretation ids are provided.") @QueryParam("version") String version,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("interpretations");

            List<String> interpretationList = getIdList(interpretations);
            DataResult<Interpretation> interpretationOpenCGAResult = catalogInterpretationManager.get(studyStr, interpretationList, query, queryOptions, true, token);
            return createOkResponse(interpretationOpenCGAResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/interpretation/search")
    @ApiOperation(value = "Search clinical interpretations", response = Interpretation.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query")
    })
    public Response interpretationSearch(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INTERPRETATION_ID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.INTERPRETATION_UUID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.INTERPRETATION_CLINICAL_ANALYSIS_ID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_CLINICAL_ANALYSIS_ID_PARAM) String clinicalAnalysisId,
            @ApiParam(value = ParamConstants.INTERPRETATION_ANALYST_ID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_ANALYST_ID_PARAM) String analystId,
            @ApiParam(value = ParamConstants.INTERPRETATION_METHOD_NAME_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_METHOD_NAME_PARAM) String method,
            @ApiParam(value = ParamConstants.INTERPRETATION_PANELS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_PANELS_PARAM) String panels,
            @ApiParam(value = ParamConstants.INTERPRETATION_PRIMARY_FINDINGS_IDS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_PRIMARY_FINDINGS_IDS_PARAM) String primaryFindings,
            @ApiParam(value = ParamConstants.INTERPRETATION_SECONDARY_FINDINGS_IDS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_SECONDARY_FINDINGS_IDS_PARAM) String secondaryFindings,
            @ApiParam(value = ParamConstants.INTERPRETATION_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.INTERPRETATION_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.INTERPRETATION_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.INTERPRETATION_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.INTERPRETATION_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_RELEASE_PARAM) String release,
            // The following variables were hid on version 2.1.0
            @ApiParam(value = "Clinical analyst ID", hidden = true) @QueryParam("analyst") String clinicalAnalyst,
            @ApiParam(value = "Interpretation method name", hidden = true) @QueryParam("methods") String methods) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.putIfNotEmpty(ParamConstants.INTERPRETATION_ANALYST_ID_PARAM, clinicalAnalyst);
            query.putIfNotEmpty(ParamConstants.INTERPRETATION_METHOD_NAME_PARAM, clinicalAnalyst);
            query.remove("analyst");
            query.remove("methods");
            return createOkResponse(catalogInterpretationManager.search(studyStr, query, queryOptions, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/interpretation/distinct")
    @ApiOperation(value = "Interpretation distinct method")
    public Response interpretationDistinct(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.INTERPRETATION_ID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_ID_PARAM) String id,
            @ApiParam(value = ParamConstants.INTERPRETATION_UUID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_UUID_PARAM) String uuid,
            @ApiParam(value = ParamConstants.INTERPRETATION_CLINICAL_ANALYSIS_ID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_CLINICAL_ANALYSIS_ID_PARAM) String clinicalAnalysisId,
            @ApiParam(value = ParamConstants.INTERPRETATION_ANALYST_ID_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_ANALYST_ID_PARAM) String analystId,
            @ApiParam(value = ParamConstants.INTERPRETATION_METHOD_NAME_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_METHOD_NAME_PARAM) String methodsName,
            @ApiParam(value = ParamConstants.INTERPRETATION_PANELS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_PANELS_PARAM) String panels,
            @ApiParam(value = ParamConstants.INTERPRETATION_PRIMARY_FINDINGS_IDS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_PRIMARY_FINDINGS_IDS_PARAM) String primaryFindings,
            @ApiParam(value = ParamConstants.INTERPRETATION_SECONDARY_FINDINGS_IDS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_SECONDARY_FINDINGS_IDS_PARAM) String secondaryFindings,
            @ApiParam(value = ParamConstants.INTERPRETATION_CREATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_CREATION_DATE_PARAM) String creationDate,
            @ApiParam(value = ParamConstants.INTERPRETATION_MODIFICATION_DATE_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_MODIFICATION_DATE_PARAM) String modificationDate,
            @ApiParam(value = ParamConstants.INTERPRETATION_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_STATUS_PARAM) String status,
            @ApiParam(value = ParamConstants.INTERPRETATION_INTERNAL_STATUS_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_INTERNAL_STATUS_PARAM) String internalStatus,
            @ApiParam(value = ParamConstants.INTERPRETATION_RELEASE_DESCRIPTION) @QueryParam(ParamConstants.INTERPRETATION_RELEASE_PARAM) String release,
            // The following variables were hid on version 2.1.0
            @ApiParam(value = "Clinical analyst ID", hidden = true) @QueryParam("analyst") String clinicalAnalyst,
            @ApiParam(value = "Interpretation method name", hidden = true) @QueryParam("methods") String methods,
            @ApiParam(value = ParamConstants.DISTINCT_FIELD_DESCRIPTION, required = true) @QueryParam(ParamConstants.DISTINCT_FIELD_PARAM) String field) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove(ParamConstants.DISTINCT_FIELD_PARAM);

            query.putIfNotEmpty(ParamConstants.INTERPRETATION_ANALYST_ID_PARAM, clinicalAnalyst);
            query.putIfNotEmpty(ParamConstants.INTERPRETATION_METHOD_NAME_PARAM, clinicalAnalyst);
            query.remove("analyst");
            query.remove("methods");

            return createOkResponse(catalogInterpretationManager.distinct(studyStr, field, query, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    //-------------------------------------------------------------------------
    // R E C E S S I V E      G E N E       A N A L Y S I S
    //-------------------------------------------------------------------------

    @GET
    @Path("/rga/individual/query")
    @ApiOperation(value = "Query individual RGA", response = KnockoutByIndividual.class)
    @ApiImplicitParams({
            // Query options
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),

            @ApiImplicitParam(name = "sampleId", value = RgaQueryParams.SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "individualId", value = RgaQueryParams.INDIVIDUAL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sex", value = RgaQueryParams.SEX_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "phenotypes", value = RgaQueryParams.PHENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "disorders", value = RgaQueryParams.DISORDERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "numParents", value = RgaQueryParams.NUM_PARENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneId", value = RgaQueryParams.GENE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneName", value = RgaQueryParams.GENE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneBiotype", value = RgaQueryParams.GENE_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = RgaQueryParams.CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "start", value = RgaQueryParams.START_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "end", value = RgaQueryParams.END_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptId", value = RgaQueryParams.TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptBiotype", value = RgaQueryParams.TRANSCRIPT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "variants", value = RgaQueryParams.VARIANTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "dbSnps", value = RgaQueryParams.DB_SNPS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "knockoutType", value = RgaQueryParams.KNOCKOUT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = RgaQueryParams.FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = RgaQueryParams.TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = RgaQueryParams.CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequency", value = RgaQueryParams.POPULATION_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "consequenceType", value = RgaQueryParams.CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query")
    })
    public Response rgaIndividualQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = RgaQueryParams.getQueryParams(queryOptions);

            return getRgaManager().individualQuery(studyStr, query, queryOptions, token);
        });
    }

    @GET
    @Path("/rga/individual/summary")
    @ApiOperation(value = "RGA individual summary stats", response = KnockoutByIndividualSummary.class)
    @ApiImplicitParams({
            // Query options
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),

            @ApiImplicitParam(name = "sampleId", value = RgaQueryParams.SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "individualId", value = RgaQueryParams.INDIVIDUAL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sex", value = RgaQueryParams.SEX_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "phenotypes", value = RgaQueryParams.PHENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "disorders", value = RgaQueryParams.DISORDERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "numParents", value = RgaQueryParams.NUM_PARENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneId", value = RgaQueryParams.GENE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneName", value = RgaQueryParams.GENE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneBiotype", value = RgaQueryParams.GENE_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = RgaQueryParams.CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "start", value = RgaQueryParams.START_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "end", value = RgaQueryParams.END_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptId", value = RgaQueryParams.TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptBiotype", value = RgaQueryParams.TRANSCRIPT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "variants", value = RgaQueryParams.VARIANTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "dbSnps", value = RgaQueryParams.DB_SNPS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "knockoutType", value = RgaQueryParams.KNOCKOUT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = RgaQueryParams.FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = RgaQueryParams.TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = RgaQueryParams.CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequency", value = RgaQueryParams.POPULATION_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "consequenceType", value = RgaQueryParams.CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query")
    })
    public Response rgaIndividualSummary(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = RgaQueryParams.getQueryParams(queryOptions);

            return getRgaManager().individualSummary(studyStr, query, queryOptions, token);
        });
    }

    @GET
    @Path("/rga/gene/query")
    @ApiOperation(value = "Query gene RGA", response = RgaKnockoutByGene.class)
    @ApiImplicitParams({
            // Query options
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = RgaQueryParams.INCLUDE_INDIVIDUAL, value = RgaQueryParams.INCLUDE_INDIVIDUAL_DESCR, example = "ind1,ind2,ind3", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = RgaQueryParams.SKIP_INDIVIDUAL, value = RgaQueryParams.SKIP_INDIVIDUAL_DESCR, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = RgaQueryParams.LIMIT_INDIVIDUAL, value = RgaQueryParams.LIMIT_INDIVIDUAL_DESCR, dataType = "integer", paramType = "query"),

            @ApiImplicitParam(name = "sampleId", value = RgaQueryParams.SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "individualId", value = RgaQueryParams.INDIVIDUAL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sex", value = RgaQueryParams.SEX_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "phenotypes", value = RgaQueryParams.PHENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "disorders", value = RgaQueryParams.DISORDERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "numParents", value = RgaQueryParams.NUM_PARENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneId", value = RgaQueryParams.GENE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneName", value = RgaQueryParams.GENE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneBiotype", value = RgaQueryParams.GENE_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = RgaQueryParams.CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "start", value = RgaQueryParams.START_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "end", value = RgaQueryParams.END_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptId", value = RgaQueryParams.TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptBiotype", value = RgaQueryParams.TRANSCRIPT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "variants", value = RgaQueryParams.VARIANTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "dbSnps", value = RgaQueryParams.DB_SNPS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "knockoutType", value = RgaQueryParams.KNOCKOUT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = RgaQueryParams.FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = RgaQueryParams.TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = RgaQueryParams.CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequency", value = RgaQueryParams.POPULATION_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "consequenceType", value = RgaQueryParams.CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query")
    })
    public Response rgaGeneQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = RgaQueryParams.getQueryParams(queryOptions);

            return getRgaManager().geneQuery(studyStr, query, queryOptions, token);
        });
    }

    @GET
    @Path("/rga/gene/summary")
    @ApiOperation(value = "RGA gene summary stats", response = KnockoutByGeneSummary.class)
    @ApiImplicitParams({
            // Query options
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),

            @ApiImplicitParam(name = "sampleId", value = RgaQueryParams.SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "individualId", value = RgaQueryParams.INDIVIDUAL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sex", value = RgaQueryParams.SEX_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "phenotypes", value = RgaQueryParams.PHENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "disorders", value = RgaQueryParams.DISORDERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "numParents", value = RgaQueryParams.NUM_PARENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneId", value = RgaQueryParams.GENE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneName", value = RgaQueryParams.GENE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneBiotype", value = RgaQueryParams.GENE_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = RgaQueryParams.CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "start", value = RgaQueryParams.START_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "end", value = RgaQueryParams.END_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptId", value = RgaQueryParams.TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptBiotype", value = RgaQueryParams.TRANSCRIPT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "variants", value = RgaQueryParams.VARIANTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "dbSnps", value = RgaQueryParams.DB_SNPS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "knockoutType", value = RgaQueryParams.KNOCKOUT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = RgaQueryParams.FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = RgaQueryParams.TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = RgaQueryParams.CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequency", value = RgaQueryParams.POPULATION_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "consequenceType", value = RgaQueryParams.CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query")
    })
    public Response rgaGeneSummary(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = RgaQueryParams.getQueryParams(queryOptions);

            return getRgaManager().geneSummary(studyStr, query, queryOptions, token);
        });
    }

    @GET
    @Path("/rga/variant/query")
    @ApiOperation(value = "Query variant RGA", response = KnockoutByVariant.class)
    @ApiImplicitParams({
            // Query options
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = RgaQueryParams.INCLUDE_INDIVIDUAL, value = RgaQueryParams.INCLUDE_INDIVIDUAL_DESCR, example = "ind1,ind2,ind3", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = RgaQueryParams.SKIP_INDIVIDUAL, value = RgaQueryParams.SKIP_INDIVIDUAL_DESCR, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = RgaQueryParams.LIMIT_INDIVIDUAL, value = RgaQueryParams.LIMIT_INDIVIDUAL_DESCR, dataType = "integer", paramType = "query"),

            @ApiImplicitParam(name = "sampleId", value = RgaQueryParams.SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "individualId", value = RgaQueryParams.INDIVIDUAL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sex", value = RgaQueryParams.SEX_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "phenotypes", value = RgaQueryParams.PHENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "disorders", value = RgaQueryParams.DISORDERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "numParents", value = RgaQueryParams.NUM_PARENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneId", value = RgaQueryParams.GENE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneName", value = RgaQueryParams.GENE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneBiotype", value = RgaQueryParams.GENE_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = RgaQueryParams.CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "start", value = RgaQueryParams.START_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "end", value = RgaQueryParams.END_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptId", value = RgaQueryParams.TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptBiotype", value = RgaQueryParams.TRANSCRIPT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "variants", value = RgaQueryParams.VARIANTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "dbSnps", value = RgaQueryParams.DB_SNPS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "knockoutType", value = RgaQueryParams.KNOCKOUT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = RgaQueryParams.FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = RgaQueryParams.TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = RgaQueryParams.CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequency", value = RgaQueryParams.POPULATION_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "consequenceType", value = RgaQueryParams.CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query")
    })
    public Response rgaVariantQuery(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = RgaQueryParams.getQueryParams(queryOptions);

            return getRgaManager().variantQuery(studyStr, query, queryOptions, token);
        });
    }

    @GET
    @Path("/rga/variant/summary")
    @ApiOperation(value = "RGA variant summary stats", response = KnockoutByVariantSummary.class)
    @ApiImplicitParams({
            // Query options
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),

            @ApiImplicitParam(name = "sampleId", value = RgaQueryParams.SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "individualId", value = RgaQueryParams.INDIVIDUAL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sex", value = RgaQueryParams.SEX_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "phenotypes", value = RgaQueryParams.PHENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "disorders", value = RgaQueryParams.DISORDERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "numParents", value = RgaQueryParams.NUM_PARENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneId", value = RgaQueryParams.GENE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneName", value = RgaQueryParams.GENE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneBiotype", value = RgaQueryParams.GENE_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = RgaQueryParams.CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "start", value = RgaQueryParams.START_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "end", value = RgaQueryParams.END_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptId", value = RgaQueryParams.TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptBiotype", value = RgaQueryParams.TRANSCRIPT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "variants", value = RgaQueryParams.VARIANTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "dbSnps", value = RgaQueryParams.DB_SNPS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "knockoutType", value = RgaQueryParams.KNOCKOUT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = RgaQueryParams.FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = RgaQueryParams.TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = RgaQueryParams.CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequency", value = RgaQueryParams.POPULATION_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "consequenceType", value = RgaQueryParams.CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query")
    })
    public Response rgaVariantSummary(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr
    ) {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = RgaQueryParams.getQueryParams(queryOptions);

            return getRgaManager().variantSummary(studyStr, query, queryOptions, token);
        });
    }

    @GET
    @Path("/rga/aggregationStats")
    @ApiOperation(value = "RGA aggregation stats", response = FacetField.class)
    @ApiImplicitParams({
            // QueryOptions
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),

            @ApiImplicitParam(name = "sampleId", value = RgaQueryParams.SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "individualId", value = RgaQueryParams.INDIVIDUAL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sex", value = RgaQueryParams.SEX_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "phenotypes", value = RgaQueryParams.PHENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "disorders", value = RgaQueryParams.DISORDERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "numParents", value = RgaQueryParams.NUM_PARENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneId", value = RgaQueryParams.GENE_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneName", value = RgaQueryParams.GENE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneBiotype", value = RgaQueryParams.GENE_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = RgaQueryParams.CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "start", value = RgaQueryParams.START_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "end", value = RgaQueryParams.END_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptId", value = RgaQueryParams.TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptBiotype", value = RgaQueryParams.TRANSCRIPT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "variants", value = RgaQueryParams.VARIANTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "dbSnps", value = RgaQueryParams.DB_SNPS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "knockoutType", value = RgaQueryParams.KNOCKOUT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = RgaQueryParams.FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = RgaQueryParams.TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = RgaQueryParams.CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequency", value = RgaQueryParams.POPULATION_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "consequenceType", value = RgaQueryParams.CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query")
    })
    public Response rgaAggregationStats(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "List of fields separated by semicolons, e.g.: clinicalSignificances;type. For nested fields use >>, e.g.: type>>clinicalSignificances;knockoutType", required = true) @QueryParam("field") String facet
    ) {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = RgaQueryParams.getQueryParams(queryOptions);

            return getRgaManager().aggregationStats(studyStr, query, queryOptions, facet, token);
        });
    }

    @POST
    @Path("/rga/index/run")
    @ApiOperation(value = RgaAnalysis.DESCRIPTION, response = Job.class)
    public Response rgaIndexRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.INDEX_AUXILIAR_COLLECTION_DESCRIPTION, defaultValue = "false")
            @QueryParam(ParamConstants.INDEX_AUXILIAR_COLLECTION) boolean indexAuxiliarColl,
            @ApiParam(value = RgaAnalysisParams.DESCRIPTION, required = true) RgaAnalysisParams params) {
        if (indexAuxiliarColl) {
            Map<String, Object> paramsMap = new HashMap<>();
            if (StringUtils.isNotEmpty(study)) {
                paramsMap.putIfAbsent(ParamConstants.STUDY_PARAM, study);
            }
            return submitJob(AuxiliarRgaAnalysis.ID, null, study, paramsMap, jobName, jobDescription, dependsOn, jobTags);
        } else {
            return submitJob(RgaAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
        }
    }

    //-------------------------------------------------------------------------
    // C L I N I C A L      V A R I A N T S
    //-------------------------------------------------------------------------

    @GET
    @Path("/variant/query")
    @ApiOperation(value = "Fetch clinical variants", response = ClinicalVariant.class)
    @ApiImplicitParams({

            // Query options
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCount", value = "Get an approximate count, instead of an exact total count. Reduces execution time", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCountSamplingSize", value = "Sampling size to get the approximate count. "
                    + "Larger values increase accuracy but also increase execution time", dataType = "integer", paramType = "query"),

            @ApiImplicitParam(name = "savedFilter", value = SAVED_FILTER_DESCR, dataType = "string", paramType = "query"),

            // Interpretation ID to include fields related to
            @ApiImplicitParam(name = ParamConstants.INCLUDE_INTERPRETATION, value = ParamConstants.INCLUDE_INTERPRETATION_DESCRIPTION, dataType = "string", paramType = "query"),
            // Variant filters
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "qual", value = QUAL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "fileData", value = FILE_DATA_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleData", value = SAMPLE_DATA_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsRef", value = STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsAlt", value = STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMaf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsPass", value = STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "score", value = SCORE_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "family", value = VariantCatalogQueryUtils.FAMILY_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyDisorder", value = VariantCatalogQueryUtils.FAMILY_DISORDER_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familySegregation", value = VariantCatalogQueryUtils.FAMILY_SEGREGATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyMembers", value = VariantCatalogQueryUtils.FAMILY_MEMBERS_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyProband", value = VariantCatalogQueryUtils.FAMILY_PROBAND_DESC, dataType = "string", paramType = "query"),

            // Annotation filters
            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "xref", value = ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "biotype", value = ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "proteinSubstitution", value = ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "conservation", value = ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequencyRef", value = ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequencyMaf", value = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "transcriptFlag", value = ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "geneTraitId", value = ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "go", value = ANNOT_GO_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "expression", value = ANNOT_EXPRESSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "proteinKeyword", value = ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "drug", value = ANNOT_DRUG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "functionalScore", value = ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinical", value = ANNOT_CLINICAL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalConfirmedStatus", value = ANNOT_CLINICAL_CONFIRMED_STATUS_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "customAnnotation", value = CUSTOM_ANNOTATION_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "panel", value = VariantCatalogQueryUtils.PANEL_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelModeOfInheritance", value = VariantCatalogQueryUtils.PANEL_MOI_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelConfidence", value = VariantCatalogQueryUtils.PANEL_CONFIDENCE_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelRoleInCancer", value = VariantCatalogQueryUtils.PANEL_ROLE_IN_CANCER_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelFeatureType", value = VariantCatalogQueryUtils.PANEL_FEATURE_TYPE_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelIntersection", value = VariantCatalogQueryUtils.PANEL_INTERSECTION_DESC, dataType = "boolean", paramType = "query"),

            @ApiImplicitParam(name = "trait", value = ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
    })
    public Response variantQuery() {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);

            // Because of the parameter includeInterpretation is not a standard variant query parameter, it is added to the query
            // after parsing the query parameters
            if (uriInfo.getQueryParameters().containsKey(INCLUDE_INTERPRETATION)) {
                String includeInterpretation = uriInfo.getQueryParameters().get(INCLUDE_INTERPRETATION).get(0);
                logger.info("Adding the includeInterpretation ({}) to the variant query", includeInterpretation);
                query.put(INCLUDE_INTERPRETATION, includeInterpretation);
            }

            return clinicalInterpretationManager.get(query, queryOptions, token);
        });
    }

    //-------------------------------------------------------------------------
    // I N T E R P R E T A T I O N     A N A L Y S I S
    //-------------------------------------------------------------------------

    @POST
    @Path("/interpreter/tiering/run")
    @ApiOperation(value = TieringInterpretationAnalysis.DESCRIPTION, response = Job.class)
    public Response interpretationTieringRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = TieringInterpretationAnalysisParams.DESCRIPTION, required = true) TieringInterpretationAnalysisParams params) {
        return submitJob(TieringInterpretationAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }


    @POST
    @Path("/interpreter/exomiser/run")
    @ApiOperation(value = ExomiserInterpretationAnalysis.DESCRIPTION, response = Job.class)
    public Response interpretationExomiserRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ExomiserInterpretationAnalysisParams.DESCRIPTION, required = true) ExomiserInterpretationAnalysisParams params) {
        return submitJob(ExomiserInterpretationAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/interpreter/team/run")
    @ApiOperation(value = TeamInterpretationAnalysis.DESCRIPTION, response = Job.class)
    public Response interpretationTeamRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = TeamInterpretationAnalysisParams.DESCRIPTION, required = true) TeamInterpretationAnalysisParams params) {
        return submitJob(TeamInterpretationAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/interpreter/zetta/run")
    @ApiOperation(value = ZettaInterpretationAnalysis.DESCRIPTION, response = Job.class)
    public Response interpretationZettaRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ZettaInterpretationAnalysisParams.DESCRIPTION, required = true) ZettaInterpretationAnalysisParams params) {
        return submitJob(ZettaInterpretationAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/interpreter/cancerTiering/run")
    @ApiOperation(value = CancerTieringInterpretationAnalysis.DESCRIPTION, response = Job.class)
    public Response interpretationCancerTieringRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = CancerTieringInterpretationAnalysisParams.DESCRIPTION, required = true) CancerTieringInterpretationAnalysisParams params) {
        return submitJob(CancerTieringInterpretationAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }
}
