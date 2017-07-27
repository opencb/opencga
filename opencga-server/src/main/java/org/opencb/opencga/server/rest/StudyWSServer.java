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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.managers.api.IStudyManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.AclParams;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageEngine;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.manager.variant.operations.StorageOperation;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;


@Path("/{version}/studies")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Studies", position = 3, description = "Methods for working with 'studies' endpoint")
public class StudyWSServer extends OpenCGAWSServer {

    private IStudyManager studyManager;

    public StudyWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        studyManager = catalogManager.getStudyManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new study", response = Study.class)
    public Response createStudyPOST(@ApiParam(value = "Project id or alias", required = true) @QueryParam("projectId") String projectIdStr,
                                    @ApiParam(value="study", required = true) StudyParams study) {
        long projectId;
        try {
            String userId = catalogManager.getUserManager().getId(sessionId);
            projectId = catalogManager.getProjectManager().getId(userId, projectIdStr);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
        logger.debug("study = {}", study);
        try {
            return createOkResponse(catalogManager.createStudy(projectId, study.name, study.alias, study.type, null,
                    study.description, null, null, null, null, null, study.stats, study.attributes, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search studies", response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Get a count of the number of results obtained. Deactivated by default.",
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllStudies(@ApiParam(value = "Project id or alias", required = true) @QueryParam("projectId") String projectId,
                                  @ApiParam(value = "Study name") @QueryParam("name") String name,
                                  @ApiParam(value = "Study alias") @QueryParam("alias") String alias,
                                  @ApiParam(value = "Type of study: CASE_CONTROL, CASE_SET...") @QueryParam("type") String type,
                                  @ApiParam(value = "Creation date") @QueryParam("creationDate") String creationDate,
                                  @ApiParam(value = "Status") @QueryParam("status") String status,
                                  @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes,
                                  @Deprecated @ApiParam(value = "Numerical attributes") @QueryParam("nattributes") String nattributes,
                                  @Deprecated @ApiParam(value = "Boolean attributes") @QueryParam("battributes") boolean battributes,
                                  @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount,
                                  @ApiParam(value = "Release value") @QueryParam("release") String release) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            if (projectId != null) {
                query.put(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), catalogManager.getProjectId(projectId, sessionId));
            }
            QueryResult<Study> queryResult = catalogManager.getAllStudies(query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Search studies", position = 2, hidden = true, response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAllStudiesByPost(@ApiParam(value="studies", required = true) Query query,
                                        @ApiParam(value = "Skip count", defaultValue = "false") @QueryParam("skipCount") boolean skipCount) {
        try {
            queryOptions.put(QueryOptions.SKIP_COUNT, skipCount);

            QueryResult<Study> queryResult = catalogManager.getAllStudies(query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some study attributes")
    public Response updateByPost(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                 @ApiParam(value = "JSON containing the params to be updated.", required = true) StudyParams updateParams) {
        try {
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            QueryResult queryResult = catalogManager
                    .modifyStudy(studyId, new ObjectMap(jsonObjectMapper.writeValueAsString(updateParams)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/delete")
    @ApiOperation(value = "Delete a study [PENDING]", response = Study.class)
    public Response delete(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr) {
        return createOkResponse("PENDING");
    }

    @GET
    @Path("/{study}/info")
    @ApiOperation(value = "Fetch study information", response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query")
    })
    public Response info(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr) {
        try {
            List<QueryResult<Study>> queryResults = new LinkedList<>();
            List<Long> studyIds = catalogManager.getStudyIds(studyStr, sessionId);
            for (Long studyId : studyIds) {
                queryResults.add(catalogManager.getStudy(studyId, queryOptions, sessionId));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/summary")
    @ApiOperation(value = "Fetch study information plus some basic stats", notes = "Fetch study information plus some basic stats such as"
            + " the number of files, samples, cohorts...")
    public Response summary(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr) {
        try {
            List<Long> studyIds = catalogManager.getStudyIds(studyStr, sessionId);
            List<QueryResult<StudySummary>> queryResults = new LinkedList<>();
            for (Long studyId : studyIds) {
                queryResults.add(catalogManager.getStudySummary(studyId, sessionId, queryOptions));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/files")
    @ApiOperation(value = "Fetch files in study", response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Get a count of the number of results obtained. Deactivated by default.",
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllFiles(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                @ApiParam(value = "File id") @QueryParam("id") String id,
                                @ApiParam(value = "File name") @QueryParam("name") String name,
                                @ApiParam(value = "File path") @QueryParam("path") String path,
                                @ApiParam(value = "File type (FILE or DIRECTORY)") @QueryParam("type") String type,
                                @ApiParam(value = "Comma separated list of bioformat values. For existing Bioformats see files/bioformats")
                                @QueryParam("bioformat") String bioformat,
                                @ApiParam(value = "Comma separated list of format values. For existing Formats see files/formats")
                                @QueryParam("format") String formats,
                                @ApiParam(value = "File status") @QueryParam("status") File.FileStatus status,
                                @ApiParam(value = "Directory where the files will be looked for") @QueryParam("directory") String directory,
                                @ApiParam(value = "Creation date of the file") @QueryParam("creationDate") String creationDate,
                                @ApiParam(value = "Last modification date of the file") @QueryParam("modificationDate")
                                        String modificationDate,
                                @ApiParam(value = "File description") @QueryParam("description") String description,
                                @ApiParam(value = "File size") @QueryParam("size") Long size,
                                @ApiParam(value = "List of sample ids associated with the files") @QueryParam("sampleIds") String sampleIds,
                                @ApiParam(value = "Job id that generated the file") @QueryParam("jobId") String jobId,
                                @ApiParam(value = "Attributes") @QueryParam("attributes") String attributes,
                                @ApiParam(value = "Numerical attributes") @QueryParam("nattributes") String nattributes) {
        try {
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            QueryResult queryResult = catalogManager.getAllFiles(studyId, query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/samples")
    @ApiOperation(value = "Fetch samples in study", response = Sample[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Set which fields are included in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Set which fields are excluded in the response, e.g.: name,alias...",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Max number of results to be returned.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to be skipped.", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Get a count of the number of results obtained. Deactivated by default.",
                    dataType = "boolean", paramType = "query")
    })
    public Response getAllSamples(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                  @ApiParam(value = "Sample name") @QueryParam("name") String name,
                                  @Deprecated @ApiParam(value = "source", hidden = true) @QueryParam("source") String source,
                                  @ApiParam(value = "individualId") @QueryParam("individualId") String individualId,
                                  @ApiParam(value = "annotationSetName") @QueryParam("annotationSetName") String annotationSetName,
                                  @ApiParam(value = "variableSetId", hidden = true) @QueryParam("variableSetId") String variableSetId,
                                  @ApiParam(value = "variableSet") @QueryParam("variableSet") String variableSet,
                                  @ApiParam(value = "annotation") @QueryParam("annotation") String annotation) {
        try {
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            QueryResult queryResult = catalogManager.getAllSamples(studyId, query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/jobs")
    @ApiOperation(value = "Return filtered jobs in study [PENDING]", position = 9, notes = "Currently it returns all the jobs in the study."
            + " No filters are being used yet.", response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAllJobs(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                               @ApiParam(value = "name") @DefaultValue("") @QueryParam("name") String name,
                               @ApiParam(value = "tool name") @DefaultValue("") @QueryParam("toolName") String tool,
                               @ApiParam(value = "status") @DefaultValue("") @QueryParam("status") String status,
                               @ApiParam(value = "ownerId") @DefaultValue("") @QueryParam("ownerId") String ownerId,
                               @ApiParam(value = "date") @DefaultValue("") @QueryParam("date") String date,
                               @ApiParam(value = "Comma separated list of output file ids") @DefaultValue("")
                               @QueryParam("inputFiles") String inputFiles,
                               @ApiParam(value = "Comma separated list of output file ids") @DefaultValue("")
                               @QueryParam("outputFiles") String outputFiles) {
        try {
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            return createOkResponse(catalogManager.getAllJobs(studyId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{study}/variants")
    @ApiOperation(value = "[DEPRECATED]: use analysis/variant/query instead", position = 10, hidden = true, response = Variant[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getVariants(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                @ApiParam(value = "List of variant ids") @QueryParam("ids") String ids,
                                @ApiParam(value = "List of regions: {chr}:{start}-{end}") @QueryParam("region") String region,
                                @ApiParam(value = "List of chromosomes") @QueryParam("chromosome") String chromosome,
                                @ApiParam(value = "List of genes") @QueryParam("gene") String gene,
                                @ApiParam(value = "Variant type: [SNV, MNV, INDEL, SV, CNV]") @QueryParam("type") String type,
                                @ApiParam(value = "Reference allele") @QueryParam("reference") String reference,
                                @ApiParam(value = "Main alternate allele") @QueryParam("alternate") String alternate,
//                                @ApiParam(value = "") @QueryParam("studies") String studies,
                                @ApiParam(value = "List of studies to be returned") @QueryParam("returnedStudies") String returnedStudies,
                                @ApiParam(value = "List of samples to be returned") @QueryParam("returnedSamples") String returnedSamples,
                                @ApiParam(value = "List of files to be returned.") @QueryParam("returnedFiles") String returnedFiles,
                                @ApiParam(value = "Variants in specific files") @QueryParam("files") String files,
                                @ApiParam(value = VariantQueryParam.FILTER_DESCR) @QueryParam("filter") String filter,
                                @ApiParam(value = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}")
                                @QueryParam("maf") String maf,
                                @ApiParam(value = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}")
                                @QueryParam("mgf") String mgf,
                                @ApiParam(value = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}")
                                @QueryParam("missingAlleles") String missingAlleles,
                                @ApiParam(value = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}")
                                @QueryParam("missingGenotypes") String missingGenotypes,
                                @ApiParam(value = "Specify if the variant annotation must exists.")
                                @QueryParam("annotationExists") boolean annotationExists,
                                @ApiParam(value = "Samples with a specific genotype: "
                                        + "{samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1")
                                @QueryParam("genotype") String genotype,
                                @ApiParam(value = VariantQueryParam.SAMPLES_DESCR) @QueryParam("samples") String samples,
                                @ApiParam(value = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578")
                                @QueryParam("annot-ct") String annot_ct,
                                @ApiParam(value = "XRef") @QueryParam("annot-xref") String annot_xref,
                                @ApiParam(value = "Biotype") @QueryParam("annot-biotype") String annot_biotype,
                                @ApiParam(value = "Polyphen, protein substitution score. "
                                        + "[<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign")
                                @QueryParam("polyphen") String polyphen,
                                @ApiParam(value = "Sift, protein substitution score. "
                                        + "[<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant")
                                @QueryParam("sift") String sift,
                                @ApiParam(value = "Protein substitution score. {protein_score}[<|>|<=|>=]{number} or "
                                        + "{protein_score}[~=|=]{description} e.g. polyphen>0.1 , sift=tolerant")
                                @QueryParam ("protein_substitution") String protein_substitution,
                                @ApiParam(value = "Conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. "
                                        + "phastCons>0.5,phylop<0.1,gerp>0.1") @QueryParam("conservation") String conservation,
                                @ApiParam(value = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}")
                                @QueryParam("annot-population-maf") String annotPopulationMaf,
                                @ApiParam(value = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}")
                                @QueryParam("alternate_frequency") String alternate_frequency,
                                @ApiParam(value = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}")
                                @QueryParam("reference_frequency") String reference_frequency,
                                @ApiParam(value = "List of transcript annotation flags. e.g. "
                                        + "CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno")
                                @QueryParam ("annot-transcription-flags") String transcriptionFlags,
                                @ApiParam(value = "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\"")
                                @QueryParam("annot-gene-trait-id") String geneTraitId,
                                @ApiParam(value = "List of gene trait association names. e.g. \"Cardiovascular Diseases\"")
                                @QueryParam("annot-gene-trait-name") String geneTraitName,
                                @ApiParam(value = "List of HPO terms. e.g. \"HP:0000545\"") @QueryParam("annot-hpo") String hpo,
                                @ApiParam(value = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"")
                                @QueryParam("annot-go") String go,
                                @ApiParam(value = "List of tissues of interest. e.g. \"tongue\"")
                                @QueryParam("annot-expression") String expression,
                                @ApiParam(value = "List of protein variant annotation keywords")
                                @QueryParam("annot-protein-keywords") String proteinKeyword,
                                @ApiParam(value = "List of drug names") @QueryParam("annot-drug") String drug,
                                @ApiParam(value = "Functional score: "
                                        + "{functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3")
                                @QueryParam ("annot-functional-score") String functional,

                                @ApiParam(value = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]")
                                @QueryParam("unknownGenotype") String unknownGenotype,
                                @ApiParam(value = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.")
                                @QueryParam("samplesMetadata") boolean samplesMetadata,
                                @ApiParam(value = "Count results", required = false) @QueryParam("count") boolean count,
                                @ApiParam(value = "Sort the results", required = false) @QueryParam("sort") boolean sort,
                                @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("")
                                @QueryParam("groupBy") String groupBy,
                                @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false")
                                @QueryParam("histogram") boolean histogram,
                                @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000")
                                @QueryParam("interval") int interval,
                                @ApiParam(value = "Merge results", required = false) @DefaultValue("false")
                                @QueryParam("merge") boolean merge) {

        try {
            List<Long> studyIds = catalogManager.getStudyIds(studyStr, sessionId);
            List<QueryResult> queryResults = new LinkedList<>();
            for (Long studyId : studyIds) {
                QueryResult queryResult;
                // Get all query options
                QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
                Query query = VariantStorageManager.getVariantQuery(queryOptions);
                query.putIfAbsent(VariantQueryParam.STUDIES.key(), studyId);
                if (count) {
                    queryResult = variantManager.count(query, sessionId);
                } else if (histogram) {
                    queryResult = variantManager.getFrequency(query, interval, sessionId);
                } else if (StringUtils.isNotEmpty(groupBy)) {
                    queryResult = variantManager.groupBy(groupBy, query, queryOptions, sessionId);
                } else {
                    queryResult = variantManager.get(query, queryOptions, sessionId);
                }
                queryResults.add(queryResult);
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @Deprecated
    @GET
    @Path("/{study}/alignments")
    @ApiOperation(value = "[DEPCRATED]: use analysis/alignment/query instead", position = 11, hidden = true, response = Alignment[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided",
                    example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided",
                    example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAlignments(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                                  @ApiParam(value = "sample id", required = false) @DefaultValue("")
                                  @QueryParam("sampleId") String sampleIds,
                                  @ApiParam(value = "file id", required = false) @DefaultValue("") @QueryParam("fileId") String fileIds,
                                  @ApiParam(value = "region with a maximum value of 10000 nucleotides", required = true) @DefaultValue("")
                                  @QueryParam("region") String region,
                                  @ApiParam(value = "view_as_pairs", required = false) @DefaultValue("false")
                                  @QueryParam("view_as_pairs") boolean view_as_pairs,
                                  @ApiParam(value = "include_coverage", required = false) @DefaultValue("true")
                                  @QueryParam("include_coverage") boolean include_coverage,
                                  @ApiParam(value = "process_differences", required = false) @DefaultValue("true")
                                  @QueryParam("process_differences") boolean process_differences,
                                  @ApiParam(value = "histogram", required = false) @DefaultValue("false")
                                  @QueryParam("histogram") boolean histogram,
                                  @ApiParam(value = "interval", required = false) @DefaultValue("2000")
                                  @QueryParam("interval") int interval) {
        query.put(VariantQueryParam.STUDIES.key(), studyStr);
        List<Region> regions = Region.parseRegions(region);

        List<QueryResult> results = new ArrayList<>();
//        QueryResult alignmentsByRegion;
        QueryResult alignmentsByRegion;

        // TODO if SampleIds are passed we need to get the BAM files for them and execute the code below

        long studyId = 4;
        long sampleId = 33;
        QueryOptions qOptions = new QueryOptions(queryOptions);
        try {
            File file = catalogManager.getAllFiles(studyId, query
                            .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.ALIGNMENT)
                            .append(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId)
                            .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.READY),
                    qOptions, sessionId).first();
        } catch (CatalogException e) {
            e.printStackTrace();
        }

        for (String fileId : fileIds.split(",")) {
            long fileIdNum;
            File file;
            URI fileUri;
            try {
                AbstractManager.MyResourceId resource = catalogManager.getFileManager().getId(fileId, Long.toString(studyId), sessionId);
                fileIdNum = resource.getResourceId();
                QueryResult<File> queryResult = catalogManager.getFile(fileIdNum, sessionId);
                file = queryResult.getResult().get(0);
                fileUri = catalogManager.getFileUri(file);
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e);
            }

//            if (!file.getType().equals(File.Type.INDEX)) {
            if (file.getIndex() == null || !file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.READY)) {
                return createErrorResponse("", "File {id:" + file.getId() + " name:'" + file.getName() + "'} " +
                        " is not an indexed file.");
            }
            ObjectMap indexAttributes = new ObjectMap(file.getIndex().getAttributes());
            DataStore dataStore;
            try {
                dataStore = StorageOperation.getDataStore(catalogManager, Integer.parseInt(studyStr), File.Bioformat.VARIANT, sessionId);
            } catch (CatalogException e) {
                e.printStackTrace();
                return createErrorResponse(e);
            }
            String storageEngine = dataStore.getStorageEngine();
            String dbName = dataStore.getDbName();

            int chunkSize = indexAttributes.getInt("coverageChunkSize", 200);
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.put(AlignmentDBAdaptor.QO_FILE_ID, Long.toString(fileIdNum));
            queryOptions.put(AlignmentDBAdaptor.QO_BAM_PATH, fileUri.getPath());     //TODO: Make uri-compatible
            queryOptions.put(AlignmentDBAdaptor.QO_VIEW_AS_PAIRS, view_as_pairs);
            queryOptions.put(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, include_coverage);
            queryOptions.put(AlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, process_differences);
            queryOptions.put(AlignmentDBAdaptor.QO_INTERVAL_SIZE, interval);
            queryOptions.put(AlignmentDBAdaptor.QO_HISTOGRAM, histogram);
            queryOptions.put(AlignmentDBAdaptor.QO_COVERAGE_CHUNK_SIZE, chunkSize);

            if (indexAttributes.containsKey("baiFileId")) {
                File baiFile = null;
                try {
                    baiFile = catalogManager.getFile(indexAttributes.getInt("baiFileId"), sessionId).getResult().get(0);
                    URI baiUri = catalogManager.getFileUri(baiFile);
                    queryOptions.put(AlignmentDBAdaptor.QO_BAI_PATH, baiUri.getPath());  //TODO: Make uri-compatible
                } catch (CatalogException e) {
                    e.printStackTrace();
                    logger.error("Can't obtain bai file for file " + fileIdNum, e);
                }
            }

            AlignmentDBAdaptor dbAdaptor;
            try {

                AlignmentStorageEngine alignmentStorageManager = storageEngineFactory.getAlignmentStorageEngine(storageEngine, dbName);
                dbAdaptor = alignmentStorageManager.getDBAdaptor();
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageEngineException e) {
                return createErrorResponse(e);
            }

            if (histogram) {
                if (regions.size() != 1) {
                    return createErrorResponse("", "Histogram fetch only accepts one region.");
                }
                alignmentsByRegion = dbAdaptor.getAllIntervalFrequencies(regions.get(0), new QueryOptions(queryOptions));
            } else {
                alignmentsByRegion = dbAdaptor.getAllAlignmentsByRegion(regions, new QueryOptions(queryOptions));
            }
            results.add(alignmentsByRegion);
        }

        return createOkResponse(results);
    }

    @GET
    @Path("/{study}/scanFiles")
    @ApiOperation(value = "Scan the study folder to find untracked or missing files", position = 12)
    public Response scanFiles(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr) {
        try {
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            Study study = catalogManager.getStudy(studyId, sessionId).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /** First, run CheckStudyFiles to find new missing files **/
            List<File> checkStudyFiles = fileScanner.checkStudyFiles(study, false, sessionId);
            List<File> found = checkStudyFiles
                    .stream()
                    .filter(f -> f.getStatus().getName().equals(File.FileStatus.READY))
                    .collect(Collectors.toList());

            /** Get untracked files **/
            Map<String, URI> untrackedFiles = fileScanner.untrackedFiles(study, sessionId);

            /** Get missing files **/
            List<File> missingFiles = catalogManager.getAllFiles(studyId, query.append(FileDBAdaptor.QueryParams.STATUS_NAME.key(),
                    File.FileStatus.MISSING), queryOptions, sessionId).getResult();

            ObjectMap fileStatus = new ObjectMap("untracked", untrackedFiles).append("found", found).append("missing", missingFiles);

            return createOkResponse(new QueryResult<>("status", 0, 1, 1, null, null, Collections.singletonList(fileStatus)));
//            /** Print pretty **/
//            int maxFound = found.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);
//            int maxUntracked = untrackedFiles.keySet().stream().map(String::length).max(Comparator.<Integer>naturalOrder()).orElse(0);
//            int maxMissing = missingFiles.stream().map(f -> f.getPath().length()).max(Comparator.<Integer>naturalOrder()).orElse(0);
//
//            String format = "\t%-" + Math.max(Math.max(maxMissing, maxUntracked), maxFound) + "s  -> %s\n";
//
//            if (!untrackedFiles.isEmpty()) {
//                System.out.println("UNTRACKED files");
//                untrackedFiles.forEach((s, u) -> System.out.printf(format, s, u));
//                System.out.println("\n");
//            }
//
//            if (!missingFiles.isEmpty()) {
//                System.out.println("MISSING files");
//                for (File file : missingFiles) {
//                    System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
//                }
//                System.out.println("\n");
//            }
//
//            if (!found.isEmpty()) {
//                System.out.println("FOUND files");
//                for (File file : found) {
//                    System.out.printf(format, file.getPath(), catalogManager.getFileUri(file));
//                }
//            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/resyncFiles")
    @ApiOperation(value = "Scan the study folder to find untracked or missing files.", notes = "This method is intended to keep the "
            + "consistency between the database and the file system. It will check all the files and folders belonging to the study and "
            + "will keep track of those new files and/or folders found in the file system as well as update the status of those "
            + "files/folders that are no longer available in the file system setting their status to MISSING.")
    public Response resyncFiles(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr) {
        try {
            long studyId = catalogManager.getStudyId(studyStr, sessionId);
            Study study = catalogManager.getStudy(studyId, sessionId).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /* Resync files */
            List<File> resyncFiles = fileScanner.reSync(study, false, sessionId);

            return createOkResponse(new QueryResult<>("status", 0, 1, 1, null, null, Arrays.asList(resyncFiles)));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/groups")
    @ApiOperation(value = "Return the groups present in the studies", position = 13, response = Group[].class)
    public Response getGroups(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value = "Group name. If provided, it will only fetch information for the provided group.") @QueryParam("name")
                    String groupId) {
        try {
            QueryResult<Group> groupQueryResult = catalogManager.getStudyManager().getGroup(studyStr, groupId, sessionId);
            return createOkResponse(groupQueryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/groups/create")
    @ApiOperation(value = "Create a group", position = 14, hidden = true)
    public Response createGroup(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value = "Id of the new group to be created", required = true) @QueryParam("groupId") String groupId,
            @ApiParam(value = "Comma separated list of users to take part of the group") @QueryParam ("users") String users) {
        try {
            QueryResult group = catalogManager.getStudyManager().createGroup(studyStr, groupId, users, sessionId);
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/create")
    @ApiOperation(value = "Create a group", position = 14)
    public Response createGroupPOST(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value="JSON containing the parameters", required = true) GroupCreateParams params) {
        if (StringUtils.isNotEmpty(params.groupId) && StringUtils.isEmpty(params.name)) {
            params.name = params.groupId;
        }
        if (StringUtils.isEmpty(params.name)) {
            return createErrorResponse(new CatalogException("groupId key missing."));
        }
        try {
            QueryResult group = catalogManager.getStudyManager().createGroup(studyStr, params.name, params.users, sessionId);
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/groups/{group}/info")
    @ApiOperation(value = "Return the group [DEPRECATED]", position = 15,
            notes = "This webservice has been replaced by /{study}/groups")
    public Response getGroup(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value = "groupId", required = true) @PathParam("group") String groupId) {
        try {
            QueryResult<Group> group = catalogManager.getGroup(studyStr, groupId, sessionId);
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/{group}/update")
    @ApiOperation(value = "Updates the members of the group")
    public Response addMembersToGroupPOST(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value = "Group name", required = true) @PathParam("group") String groupId,
            @ApiParam(value="JSON containing the action to be performed", required = true) GroupParams params) {
        try {
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, groupId, params, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/groups/members/update")
    @ApiOperation(value = "Add/Remove users with access to study")
    public Response registerUsersToStudy(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value="JSON containing the action to be performed", required = true) MemberParams params) {
        try {
            return createOkResponse(
                    catalogManager.getStudyManager().updateGroup(studyStr, "@members", params.toGroupParams(), sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/groups/{group}/delete")
    @ApiOperation(value = "Delete the group", position = 17, notes = "Delete the group selected from the study.")
    public Response deleteMembersFromGroup(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value = "Group name", required = true) @PathParam("group") String groupId) {
        try {
            return createOkResponse(catalogManager.getStudyManager().deleteGroup(studyStr, groupId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/acl")
    @ApiOperation(value = "Return the acl of the study. If member is provided, it will only return the acl for the member.", position = 18)
    public Response getAcls(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
                @PathParam("study") String studyStr,
            @ApiParam(value = "User or group id") @QueryParam("member") String member) {
        try {
            if (StringUtils.isEmpty(member)) {
                return createOkResponse(catalogManager.getAllStudyAcls(studyStr, sessionId));
            } else {
                return createOkResponse(catalogManager.getStudyAcl(studyStr, member, sessionId));
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class CreateAclCommands {
        public String permissions;
        public String members;
    }

    public static class CreateAclCommandsTemplate extends CreateAclCommands {
        public String templateId;
    }

    // Temporal method used by deprecated methods. This will be removed at some point.
    private Study.StudyAclParams getAclParams(
            @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("add") String addPermissions,
            @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("remove") String removePermissions,
            @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("set") String setPermissions,
            @ApiParam(value = "Template of permissions (only to create)", required = false) @QueryParam("template") String template)
            throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(setPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(addPermissions) ? 1 : 0;
        count += StringUtils.isNotEmpty(removePermissions) ? 1 : 0;
        if (count > 1) {
            throw new CatalogException("Only one of add, remove or set parameters are allowed.");
        } else if (count == 0) {
            if (StringUtils.isNotEmpty(template)) {
                throw new CatalogException("One of add, remove or set parameters is expected.");
            }
        }

        String permissions = null;
        AclParams.Action action = null;
        if (StringUtils.isNotEmpty(addPermissions) || StringUtils.isNotEmpty(template)) {
            permissions = addPermissions;
            action = AclParams.Action.ADD;
        }
        if (StringUtils.isNotEmpty(setPermissions)) {
            permissions = setPermissions;
            action = AclParams.Action.SET;
        }
        if (StringUtils.isNotEmpty(removePermissions)) {
            permissions = removePermissions;
            action = AclParams.Action.REMOVE;
        }
        return new Study.StudyAclParams(permissions, action, template);
    }

    @GET
    @Path("/{study}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of users or groups", hidden = true)
    public Response createRole(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'",
                                       required = true) @QueryParam("members") String members,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list")
                               @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Template of permissions to be used (admin, analyst or view_only)")
                               @QueryParam("templateId") String templateId) {
        try {
            Study.StudyAclParams aclParams = getAclParams(permissions, null, null, templateId);
            aclParams.setAction(AclParams.Action.SET);
            return createOkResponse(studyManager.updateAcl(studyStr, members, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{study}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of users or groups [DEPRECATED]", hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. From now one this will be internally managed by the "
                    + "/acl/{members}/update entrypoint.")
    public Response createRolePOST(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value="JSON containing the parameters defined in GET. Mandatory keys: 'members'", required = true)
                    CreateAclCommandsTemplate params) {
        try {
            Study.StudyAclParams aclParams = getAclParams(params.permissions, null, null, params.templateId);
            aclParams.setAction(AclParams.Action.SET);
            return createOkResponse(studyManager.updateAcl(studyStr, params.members, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the user or group [DEPRECATED]", position = 20, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. From now one this will be internally managed by the "
                + "/acl entrypoint.")
    public Response getAcl(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                           @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getStudyAcl(studyStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the user or group", hidden = true, position = 21)
    public Response updateAcl(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                              @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add")
                              @QueryParam("add") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove")
                              @QueryParam("remove") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set")
                              @QueryParam("set") String setPermissions) {
        try {
            Study.StudyAclParams aclParams = getAclParams(addPermissions, removePermissions, setPermissions, null);
            return createOkResponse(studyManager.updateAcl(studyStr, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class MemberAclUpdateOld {
        public String add;
        public String set;
        public String remove;
    }

    @POST
    @Path("/{study}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the user or group [WARNING]", position = 21,
            notes = "WARNING: The usage of this webservice is discouraged. A different entrypoint /acl/{members}/update has been added "
                    + "to also support changing permissions using queries.")
    public Response updateAcl(
            @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias", required = true)
            @PathParam("study") String studyStr,
            @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
            @ApiParam(value="JSON containing one of the keys 'add', 'set' or 'remove'", required = true) MemberAclUpdateOld params) {
//        if (params == null || params.isEmpty()) {
//            return createErrorResponse(new CatalogException("At least one of the keys 'addUsers', 'setUsers' or 'removeUsers'"));
//        }
        try {
            Study.StudyAclParams aclParams = getAclParams(params.add, params.remove, params.set, null);
            return createOkResponse(studyManager.updateAcl(studyStr, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/acl/{memberIds}/update")
    @ApiOperation(value = "Update the set of permissions granted for the member", position = 21)
    public Response updateAcl(
            @ApiParam(value = "Comma separated list of user or group ids", required = true) @PathParam("memberIds") String memberId,
            @ApiParam(value="JSON containing the parameters to modify ACLs. 'template' could be either 'admin', 'analyst' or 'view_only'",
                    required = true) StudyAcl params) {
        try {
            Study.StudyAclParams aclParams = new Study.StudyAclParams(params.getPermissions(), params.getAction(), params.template);
            return createOkResponse(studyManager.updateAcl(params.study, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{study}/acl/{memberId}/delete")
    @ApiOperation(value = "Delete all the permissions granted for the user or group [DEPRECATED]", position = 22, hidden = true,
            notes = "DEPRECATED: The usage of this webservice is discouraged. A RESET action has been added to the /acl/{members}/update "
                    + "entrypoint.")
    public Response deleteAcl(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias",
            required = true) @PathParam("study") String studyStr,
                              @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId) {
        try {
            Study.StudyAclParams aclParams = new Study.StudyAclParams(null, AclParams.Action.RESET, null);
            return createOkResponse(studyManager.updateAcl(studyStr, memberId, aclParams, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class StudyAcl extends AclParams {
        public String study;
        public String template;
    }

    public static class StudyParams {
        public String name;
        public String alias;
        public Study.Type type;
        public String description;

        public Map<String, Object> stats;
        public Map<String, Object> attributes;

        public boolean checkValidCreateParams() {
            if (StringUtils.isEmpty(name) || StringUtils.isEmpty(alias)) {
                return false;
            }
            return true;
        }
    }

    public static class GroupCreateParams {
        @JsonProperty(required = true)
        public String name;
        @Deprecated
        public String groupId;
        public String users;
    }

    public static class GroupUpdateParams {
        public String addUsers;
        public String setUsers;
        public String removeUsers;
    }

}
