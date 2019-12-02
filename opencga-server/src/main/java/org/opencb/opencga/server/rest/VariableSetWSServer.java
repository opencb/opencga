/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.server.rest;

import io.swagger.annotations.*;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Variable;
import org.opencb.opencga.core.models.VariableSet;
import org.opencb.opencga.core.models.summaries.VariableSetSummary;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jacobo on 16/12/14.
 */
@Path("/{apiVersion}/variableset")
@Produces("application/json")
@Api(value = "VariableSet (DEPRECATED)", position = 8, description = "Methods for working with 'variableset' endpoint")
public class VariableSetWSServer extends OpenCGAWSServer {


    public VariableSetWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }



    private static class VariableSetParameters {
        public Boolean unique;
        public Boolean confidential;
        public String id;
        public String name;
        public String description;
        public List<String> entities;
        public List<Variable> variables;
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create variable set [DEPRECATED]", position = 1, response = VariableSet.class)
    public Response createSet(
            @ApiParam(value = "DEPRECATED: studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value="JSON containing the variableSet information", required = true) VariableSetParameters params) {
        try {
            params = ObjectUtils.defaultIfNull(params, new VariableSetParameters());

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            logger.debug("variables: {}", params.variables);

            // Fix variable set params to support 1.3.x
            // TODO: Remove in version 2.0.0
            params.id = StringUtils.isNotEmpty(params.id) ? params.id : params.name;
            for (Variable variable : params.variables) {
                fixVariable(variable);
            }

            DataResult<VariableSet> queryResult = catalogManager.getStudyManager().createVariableSet(studyStr, params.id, params.name,
                    params.unique, params.confidential, params.description, null, params.variables,
                    getAnnotableDataModelsList(params.entities), token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private void fixVariable(Variable variable) {
        variable.setId(StringUtils.isNotEmpty(variable.getId()) ? variable.getId() : variable.getName());
        if (variable.getVariableSet() != null && variable.getVariableSet().size() > 0) {
            for (Variable variable1 : variable.getVariableSet()) {
                fixVariable(variable1);
            }
        }
    }

    @GET
    @Path("/{variableset}/info")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet info [DEPRECATED]", position = 2, response = VariableSet.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response variablesetInfo(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM)
                    String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, required = true) @PathParam("variableset") String variableset) {
        try {
            DataResult<VariableSet> queryResult =
                    catalogManager.getStudyManager().getVariableSet(studyStr, variableset, queryOptions, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variableset}/summary")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet summary [DEPRECATED]", position = 2, response = VariableSetSummary.class)
    public Response variablesetSummary(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, required = true) @PathParam("variableset") String variablesetId) {
        try {
            ParamUtils.checkIsSingleID(variablesetId);
            DataResult<VariableSetSummary> queryResult = catalogManager.getStudyManager().getVariableSetSummary(studyStr, variablesetId,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Get VariableSet info [DEPRECATED]", position = 2, response = VariableSet[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION,
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Total number of results. [PENDING]", dataType = "boolean", paramType = "query")
    })
    public Response search(@ApiParam(value = "studyId", hidden = true) @QueryParam("studyId") String studyIdStr,
                           @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                           @ApiParam(value = "CSV list of variable set ids or names", required = false) @QueryParam("id") String id,
                           @ApiParam(value = "name", required = false) @QueryParam("name") String name,
                           @ApiParam(value = "description", required = false) @QueryParam("description") String description,
                           @ApiParam(value = "Release value") @QueryParam("release") String release,
                           @ApiParam(value = "attributes", required = false) @QueryParam("attributes") String attributes,
                           @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (StringUtils.isNotEmpty(studyIdStr)) {
                studyStr = studyIdStr;
            }
            if (StringUtils.isNotEmpty(query.getString(ParamConstants.STUDY_PARAM))) {
                query.remove(ParamConstants.STUDY_PARAM);
            }

            DataResult<VariableSet> queryResult = catalogManager.getStudyManager().searchVariableSets(studyStr, query, queryOptions,
                    token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private static class VariableSetUpdateParameters {
        public String name;
        public String description;
    }

    @GET
    @Path("/{variableset}/delete")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Delete an unused variable Set [DEPRECATED]", position = 4)
    public Response delete(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, required = true) @PathParam("variableset") String variablesetId) {
        try {
            DataResult<VariableSet> queryResult = catalogManager.getStudyManager().deleteVariableSet(studyStr, variablesetId, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{variableset}/field/add")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Add a new field in a variable set [DEPRECATED]", position = 5)
    public Response addFieldToVariableSet(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, required = true) @PathParam("variableset") String variablesetId,
            @ApiParam(value = "Variable to be added", required = true) Variable variable) {
        try {
            ObjectUtils.defaultIfNull(variable, new Variable());

            DataResult<VariableSet> queryResult = catalogManager.getStudyManager().addFieldToVariableSet(studyStr, variablesetId,
                    variable, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variableset}/field/delete")
    @ApiOperation(value = "Delete one field from a variable set [DEPRECATED]", position = 6)
    public Response renameFieldInVariableSet(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, required = true) @PathParam("variableset") String variablesetId,
            @ApiParam(value = "Variable name to delete", required = true) @QueryParam("name") String name) {
        try {
            ParamUtils.checkIsSingleID(variablesetId);
            DataResult<VariableSet> queryResult = catalogManager.getStudyManager().removeFieldFromVariableSet(studyStr, variablesetId,
                    name, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variableset}/field/rename")
    @ApiOperation(value = "Rename the field id of a field in a variable set [DEPRECATED]", position = 7)
    public Response renameFieldInVariableSet(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.VARIANBLE_SET_DESCRIPTION, required = true) @PathParam("variableset") String variablesetId,
            @ApiParam(value = "Variable name to rename", required = true) @QueryParam("oldName") String oldName,
            @ApiParam(value = "New name for the variable", required = true) @QueryParam("newName") String newName) {
        try {
            ParamUtils.checkIsSingleID(variablesetId);
            DataResult<VariableSet> queryResult = catalogManager.getStudyManager().renameFieldFromVariableSet(studyStr, variablesetId,
                    oldName, newName, token);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    private List<VariableSet.AnnotableDataModels> getAnnotableDataModelsList(List<String> entityStringList) {
        List<VariableSet.AnnotableDataModels> entities = new ArrayList<>();
        if (ListUtils.isEmpty(entityStringList)) {
            return entities;
        }

        for (String entity : entityStringList) {
            entities.add(VariableSet.AnnotableDataModels.valueOf(entity.toUpperCase()));
        }
        return entities;
    }
}
