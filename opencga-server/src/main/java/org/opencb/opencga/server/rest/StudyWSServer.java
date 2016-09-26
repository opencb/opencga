/*
 * Copyright 2015 OpenCB
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
import org.opencb.biodata.models.alignment.Alignment;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.variant.AbstractFileIndexer;
import org.opencb.opencga.catalog.utils.FileScanner;
import org.opencb.opencga.analysis.storage.variant.VariantFetcher;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.summaries.StudySummary;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;


@Path("/{version}/studies")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Studies", position = 3, description = "Methods for working with 'studies' endpoint")
public class StudyWSServer extends OpenCGAWSServer {


    public StudyWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest) throws IOException, VersionException {
        super(uriInfo, httpServletRequest);
    }

    @GET
    @Path("/create")
    @ApiOperation(value = "Create study with GET method", position = 1, response = Study.class)
    public Response createStudy(@ApiParam(value = "projectId", required = true) @QueryParam("projectId") String projectIdStr,
                                @ApiParam(value = "name", required = true) @QueryParam("name") String name,
                                @ApiParam(value = "alias", required = true) @QueryParam("alias") String alias,
                                @ApiParam(value = "type", required = false) @DefaultValue("CASE_CONTROL") @QueryParam("type") Study.Type type,
                                @ApiParam(value = "description", required = false) @QueryParam("description") String description) {
        try {
            long projectId = catalogManager.getProjectId(projectIdStr, sessionId);
            QueryResult queryResult = catalogManager.createStudy(projectId, name, alias, type, description, sessionId);
            queryResult.setId("Create study in " + projectIdStr);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/search")
    @ApiOperation(value = "Search studies", position = 2, response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAllStudies(@ApiParam(value = "id") @QueryParam("id") String id,
                                  @ApiParam(value = "projectId") @QueryParam("projectId") String projectId,
                                  @ApiParam(value = "name") @QueryParam("name") String name,
                                  @ApiParam(value = "alias") @QueryParam("alias") String alias,
                                  @ApiParam(value = "type") @QueryParam("Comma separated list of type") String type,
                                  @ApiParam(value = "creationDate") @QueryParam("creationDate") String creationDate,
                                  @ApiParam(value = "status") @QueryParam("status") String status,
                                  @ApiParam(value = "attributes") @QueryParam("attributes") String attributes,
                                  @Deprecated @ApiParam(value = "numerical attributes") @QueryParam("nattributes") String nattributes,
                                  @Deprecated @ApiParam(value = "boolean attributes") @QueryParam("battributes") boolean battributes) {
        try {
            // FIXME this is not needed right?
            QueryOptions qOptions = new QueryOptions(queryOptions);
            parseQueryParams(params, StudyDBAdaptor.QueryParams::getParam, query, qOptions);
            if (projectId != null) {
                query.put(StudyDBAdaptor.QueryParams.PROJECT_ID.key(), catalogManager.getProjectId(projectId, sessionId));
            }
            QueryResult<Study> queryResult = catalogManager.getAllStudies(query, qOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/update")
    @ApiOperation(value = "Study modify", position = 3, response = Study.class)
    public Response update(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                           @ApiParam(value = "name") @DefaultValue("") @QueryParam("name") String name,
                           @ApiParam(value = "type") @DefaultValue("") @QueryParam("type") String type,
                           @ApiParam(value = "description") @DefaultValue("") @QueryParam("description") String description,
                           @ApiParam(defaultValue = "attributes") @DefaultValue("") @QueryParam("attributes") String attributes,
                           @ApiParam(defaultValue = "stats") @DefaultValue("") @QueryParam("stats") String stats) throws IOException {
        try {
            ObjectMap params = new ObjectMap();
            params.putIfNotEmpty("name", name);
            params.putIfNotEmpty("type", type);
            params.putIfNotEmpty("description", description);
            params.putIfNotEmpty("attributes", attributes);
            params.putIfNotEmpty("stats", stats);

            logger.debug(params.toJson());
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            QueryResult result = catalogManager.modifyStudy(studyId, params, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{studyId}/delete")
    @ApiOperation(value = "Delete a study [PENDING]", position = 4, response = Study.class)
    public Response delete(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyId) {
        return createOkResponse("PENDING");
    }

    @GET
    @Path("/{studyId}/info")
    @ApiOperation(value = "Study information", position = 5, response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response info(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdsStr) {
        try {
            List<QueryResult<Study>> queryResults = new LinkedList<>();
            List<Long> studyIds = catalogManager.getStudyIds(studyIdsStr, sessionId);
            for (Long studyId : studyIds) {
                queryResults.add(catalogManager.getStudy(studyId, queryOptions, sessionId));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{studyId}/summary")
    @ApiOperation(value = "Summary with the general stats of a study", position = 6)
    public Response summary(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdsStr) {
        try {
            String[] studyIdArray = studyIdsStr.split(",");
            List<QueryResult<StudySummary>> queryResults = new LinkedList<>();
            for (String studyIdStr : studyIdArray) {
                long studyId = catalogManager.getStudyId(studyIdStr);
                queryResults.add(catalogManager.getStudySummary(studyId, sessionId, queryOptions));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    // FIXME: Implement filters
    @GET
    @Path("/{studyId}/files")
    @ApiOperation(value = "Return filtered files in study [PENDING]", position = 7, notes = "Currently it returns all the files in the study. No filters are applied yet.", response = File[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAllFiles(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                                @ApiParam(value = "id", required = false) @DefaultValue("") @QueryParam("id") String id,
                                @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                                @ApiParam(value = "path", required = false) @DefaultValue("") @QueryParam("path") String path,
                                @ApiParam(value = "Comma separated Type values. For existing Types see files/help", required = false) @DefaultValue("") @QueryParam("type") String type,
                                @ApiParam(value = "Comma separated Bioformat values. For existing Bioformats see files/help", required = false) @DefaultValue("") @QueryParam("bioformat") String bioformat,
                                @ApiParam(value = "Comma separated Format values. For existing Formats see files/help", required = false) @DefaultValue("") @QueryParam("format") String formats,
                                @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") File.FileStatus status,
                                @ApiParam(value = "directory", required = false) @DefaultValue("") @QueryParam("directory") String directory,
                                @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                                @ApiParam(value = "creationDate", required = false) @DefaultValue("") @QueryParam("creationDate") String creationDate,
                                @ApiParam(value = "modificationDate", required = false) @DefaultValue("") @QueryParam("modificationDate") String modificationDate,
                                @ApiParam(value = "description", required = false) @DefaultValue("") @QueryParam("description") String description,
                                @ApiParam(value = "diskUsage", required = false) @DefaultValue("") @QueryParam("diskUsage") Long diskUsage,
                                @ApiParam(value = "Comma separated sampleIds", required = false) @DefaultValue("") @QueryParam("sampleIds") String sampleIds,
                                @ApiParam(value = "jobId", required = false) @DefaultValue("") @QueryParam("jobId") String jobId,
                                @ApiParam(value = "attributes", required = false) @DefaultValue("") @QueryParam("attributes") String attributes,
                                @ApiParam(value = "numerical attributes", required = false) @DefaultValue("") @QueryParam("nattributes") String nattributes) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            QueryOptions qOptions = new QueryOptions(queryOptions);
            parseQueryParams(params, FileDBAdaptor.QueryParams::getParam, query, qOptions);
            QueryResult queryResult = catalogManager.getAllFiles(studyId, query, qOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/samples")
    @ApiOperation(value = "Return filtered samples in study [PENDING]", position = 8, notes = "Currently it returns all the samples in the study. No filters are being used yet", response = Sample[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAllSamples(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                                  @ApiParam(value = "name") @QueryParam("name") String name,
                                  @Deprecated @ApiParam(value = "source") @QueryParam("source") String source,
                                  @ApiParam(value = "individualId") @QueryParam("individualId") String individualId,
                                  @ApiParam(value = "annotationSetName") @QueryParam("annotationSetName") String annotationSetName,
                                  @ApiParam(value = "variableSetId") @QueryParam("variableSetId") String variableSetId,
                                  @ApiParam(value = "annotation") @QueryParam("annotation") String annotation) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            QueryOptions qOptions = new QueryOptions(queryOptions);
            parseQueryParams(params, SampleDBAdaptor.QueryParams::getParam, query, qOptions);
            QueryResult queryResult = catalogManager.getAllSamples(studyId, query, qOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/jobs")
    @ApiOperation(value = "Return filtered jobs in study [PENDING]", position = 9, notes = "Currently it returns all the jobs in the study. No filters are being used yet.", response = Job[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAllJobs(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                               @ApiParam(value = "name", required = false) @DefaultValue("") @QueryParam("name") String name,
                               @ApiParam(value = "tool name", required = false) @DefaultValue("") @QueryParam("toolName") String tool,
                               @ApiParam(value = "status", required = false) @DefaultValue("") @QueryParam("status") String status,
                               @ApiParam(value = "ownerId", required = false) @DefaultValue("") @QueryParam("ownerId") String ownerId,
                               @ApiParam(value = "date", required = false) @DefaultValue("") @QueryParam("date") String date,
                               @ApiParam(value = "Comma separated list of output file ids", required = false) @DefaultValue("") @QueryParam("inputFiles") String inputFiles,
                               @ApiParam(value = "Comma separated list of output file ids", required = false) @DefaultValue("") @QueryParam("outputFiles") String outputFiles) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            return createOkResponse(catalogManager.getAllJobs(studyId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/variants")
    @ApiOperation(value = "Fetch variants data from the selected study", position = 10, response = Variant[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getVariants(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStrCvs,
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
                                @ApiParam(value = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("maf") String maf,
                                @ApiParam(value = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("mgf") String mgf,
                                @ApiParam(value = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingAlleles") String missingAlleles,
                                @ApiParam(value = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingGenotypes") String missingGenotypes,
                                @ApiParam(value = "Specify if the variant annotation must exists.") @QueryParam("annotationExists") boolean annotationExists,
                                @ApiParam(value = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1") @QueryParam("genotype") String genotype,
                                @ApiParam(value = "Consequence type SO term list. e.g. SO:0000045,SO:0000046") @QueryParam("annot-ct") String annot_ct,
                                @ApiParam(value = "XRef") @QueryParam("annot-xref") String annot_xref,
                                @ApiParam(value = "Biotype") @QueryParam("annot-biotype") String annot_biotype,
                                @ApiParam(value = "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign") @QueryParam("polyphen") String polyphen,
                                @ApiParam(value = "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant") @QueryParam("sift") String sift,
//                                @ApiParam(value = "") @QueryParam("protein_substitution") String protein_substitution,
                                @ApiParam(value = "Conservation score: {conservation_score}[<|>|<=|>=]{number} e.g. phastCons>0.5,phylop<0.1,gerp>0.1") @QueryParam("conservation") String conservation,
                                @ApiParam(value = "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("annot-population-maf") String annotPopulationMaf,
                                @ApiParam(value = "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("alternate_frequency") String alternate_frequency,
                                @ApiParam(value = "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}") @QueryParam("reference_frequency") String reference_frequency,
                                @ApiParam(value = "List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno") @QueryParam("annot-transcription-flags") String transcriptionFlags,
                                @ApiParam(value = "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\"") @QueryParam("annot-gene-trait-id") String geneTraitId,
                                @ApiParam(value = "List of gene trait association names. e.g. \"Cardiovascular Diseases\"") @QueryParam("annot-gene-trait-name") String geneTraitName,
                                @ApiParam(value = "List of HPO terms. e.g. \"HP:0000545\"") @QueryParam("annot-hpo") String hpo,
                                @ApiParam(value = "List of GO (Genome Ontology) terms. e.g. \"GO:0002020\"") @QueryParam("annot-go") String go,
                                @ApiParam(value = "List of tissues of interest. e.g. \"tongue\"") @QueryParam("annot-expression") String expression,
                                @ApiParam(value = "List of protein variant annotation keywords") @QueryParam("annot-protein-keywords") String proteinKeyword,
                                @ApiParam(value = "List of drug names") @QueryParam("annot-drug") String drug,
                                @ApiParam(value = "Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3") @QueryParam("annot-functional-score") String functional,

                                @ApiParam(value = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]") @QueryParam("unknownGenotype") String unknownGenotype,
//                                @ApiParam(value = "Limit the number of returned variants. Max value: " + VariantFetcher.LIMIT_MAX) @DefaultValue("" + VariantFetcher.LIMIT_DEFAULT) @QueryParam("limit") int limit,
                                @ApiParam(value = "Returns the samples metadata group by studyId, instead of the variants", required = false) @QueryParam("samplesMetadata") boolean samplesMetadata,
                                @ApiParam(value = "Sort the results", required = false) @QueryParam("sort") boolean sort,
                                @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
                                @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000") @QueryParam("interval") int interval,
                                @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge) {

        try {
            String[] studyIds = studyIdStrCvs.split(",");
            List<QueryResult> queryResults = new LinkedList<>();
            VariantFetcher variantFetcher = new VariantFetcher(catalogManager, storageManagerFactory);
            for (String studyIdStr : studyIds) {
                long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
                queryResults.add(variantFetcher.getVariantsPerStudy(studyId, region, histogram, groupBy, interval, sessionId, queryOptions));
            }
            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/alignments")
    @ApiOperation(value = "Fetch alignments. [PENDING]", position = 11, response = Alignment[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAlignments(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                                  @ApiParam(value = "sample id", required = false) @DefaultValue("") @QueryParam("sampleId") String sampleIds,
                                  @ApiParam(value = "file id", required = false) @DefaultValue("") @QueryParam("fileId") String fileIds,
                                  @ApiParam(value = "region with a maximum value of 10000 nucleotides", required = true) @DefaultValue("") @QueryParam("region") String region,
                                  @ApiParam(value = "view_as_pairs", required = false) @DefaultValue("false") @QueryParam("view_as_pairs") boolean view_as_pairs,
                                  @ApiParam(value = "include_coverage", required = false) @DefaultValue("true") @QueryParam("include_coverage") boolean include_coverage,
                                  @ApiParam(value = "process_differences", required = false) @DefaultValue("true") @QueryParam("process_differences") boolean process_differences,
                                  @ApiParam(value = "histogram", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                  @ApiParam(value = "interval", required = false) @DefaultValue("2000") @QueryParam("interval") int interval) {

        query.put(VariantDBAdaptor.VariantQueryParams.STUDIES.key(), studyIdStr);
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
                fileIdNum = catalogManager.getFileId(fileId);
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
                dataStore = AbstractFileIndexer.getDataStore(catalogManager, Integer.parseInt(studyIdStr), File.Bioformat.VARIANT, sessionId);
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
                AlignmentStorageManager alignmentStorageManager = storageManagerFactory.getAlignmentStorageManager(storageEngine);
                dbAdaptor = alignmentStorageManager.getDBAdaptor(dbName);
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | StorageManagerException e) {
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

    @Deprecated
    @GET
    @Path("/{studyId}/scanFiles")
    @ApiOperation(value = "Scans the study folder to find untracked or missing files", position = 12)
    public Response scanFiles(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            Study study = catalogManager.getStudy(studyId, sessionId).first();
            FileScanner fileScanner = new FileScanner(catalogManager);

            /** First, run CheckStudyFiles to find new missing files **/
            List<File> checkStudyFiles = fileScanner.checkStudyFiles(study, false, sessionId);
            List<File> found = checkStudyFiles.stream().filter(f -> f.getStatus().getName().equals(File.FileStatus.READY)).collect(Collectors.toList());

            /** Get untracked files **/
            Map<String, URI> untrackedFiles = fileScanner.untrackedFiles(study, sessionId);

            /** Get missing files **/
            List<File> missingFiles = catalogManager.getAllFiles(studyId, query.append(FileDBAdaptor.QueryParams.FILE_STATUS.key(),
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
    @Path("/{studyId}/groups")
    @ApiOperation(value = "Return the groups present in the studies", position = 13, response = Group[].class)
    public Response getGroups(@ApiParam(value = "Comma separated list of studies", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            QueryResult<Group> allGroups = catalogManager.getAllGroups(studyIdStr, sessionId);
            return createOkResponse(allGroups);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/groups/create")
    @ApiOperation(value = "Create a group", position = 14)
    public Response createGroup(@ApiParam(value = "Study name or id", required = true) @PathParam("studyId") String studyIdStr,
                                @ApiParam(value = "Id of the new group to be created", required = true) @DefaultValue("") @QueryParam("groupId") String groupId,
                                @ApiParam(value = "Comma separated list of users to take part of the group", required = true)
                                    @DefaultValue("") @QueryParam("users") String users) {
        try {
            QueryResult group = catalogManager.createGroup(studyIdStr, groupId, users, sessionId);
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/groups/{groupId}/info")
    @ApiOperation(value = "Return the group", position = 15)
    public Response getGroup(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                             @ApiParam(value = "groupId", required = true) @DefaultValue("") @PathParam("groupId") String groupId) {
        try {
            QueryResult<Group> group = catalogManager.getGroup(studyIdStr, groupId, sessionId);
            return createOkResponse(group);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/groups/{groupId}/update")
    @ApiOperation(value = "Updates the members of the group", position = 16)
    public Response addMembersToGroup(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                                      @ApiParam(value = "groupId", required = true) @DefaultValue("") @PathParam("groupId") String groupId,
                                      @ApiParam(value = "Comma separated list of users that will be added to the group", required = false) @QueryParam("addUsers") String addUsers,
                                      @ApiParam(value = "Comma separated list of users that will be part of the group. Previous users will be removed.", required = false) @QueryParam("setUsers") String setUsers,
                                      @ApiParam(value = "Comma separated list of users that will be removed from the group", required = false) @QueryParam("removeUsers") String removeUsers) {
        try {
            return createOkResponse(catalogManager.updateGroup(studyIdStr, groupId, addUsers, removeUsers, setUsers, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/groups/{groupId}/delete")
    @ApiOperation(value = "Delete the group", position = 17, notes = "Delete the group selected from the study. When filled in with a list of users," +
            " it will just take them out from the group leaving the group untouched.")
    public Response deleteMembersFromGroup(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                                           @ApiParam(value = "groupId", required = true) @DefaultValue("") @PathParam("groupId") String groupId) {
        try {
            return createOkResponse(catalogManager.deleteGroup(studyIdStr, groupId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/acl")
    @ApiOperation(value = "Return the acl of the study", position = 18)
    public Response getAcls(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr) {
        try {
            return createOkResponse(catalogManager.getAllStudyAcls(studyIdStr, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/{studyId}/acl/create")
    @ApiOperation(value = "Define a set of permissions for a list of users or groups", position = 19)
    public Response createRole(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                               @ApiParam(value = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true) @QueryParam("members") String members,
                               @ApiParam(value = "Comma separated list of permissions that will be granted to the member list", required = false) @DefaultValue("") @QueryParam("permissions") String permissions,
                               @ApiParam(value = "Template of permissions to be used (admin, analyst or locked)", required = false) @QueryParam("templateId") String templateId) {
        try {
            return createOkResponse(catalogManager.createStudyAcls(studyIdStr, members, permissions, templateId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/acl/{memberId}/info")
    @ApiOperation(value = "Return the set of permissions granted for the user or group", position = 20)
    public Response getAcl(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                           @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.getStudyAcl(studyIdStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/acl/{memberId}/update")
    @ApiOperation(value = "Update the set of permissions granted for the user or group", position = 21)
    public Response updateAcl(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                              @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId,
                              @ApiParam(value = "Comma separated list of permissions to add", required = false) @QueryParam("addPermissions") String addPermissions,
                              @ApiParam(value = "Comma separated list of permissions to remove", required = false) @QueryParam("removePermissions") String removePermissions,
                              @ApiParam(value = "Comma separated list of permissions to set", required = false) @QueryParam("setPermissions") String setPermissions) {
        try {
            return createOkResponse(catalogManager.updateStudyAcl(studyIdStr, memberId, addPermissions, removePermissions, setPermissions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{studyId}/acl/{memberId}/delete")
    @ApiOperation(value = "Delete all the permissions granted for the user or group", position = 22)
    public Response deleteAcl(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                              @ApiParam(value = "User or group id", required = true) @PathParam("memberId") String memberId) {
        try {
            return createOkResponse(catalogManager.removeStudyAcl(studyIdStr, memberId, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a file with POST method", response = QueryResult.class, position = 1, notes =
            "Wont't accept files, jobs, experiments, samples.<br>" +
                    "Will accept (but not yet): acl, uri, cohorts, datasets.<br>" +
//            "Work in progress.<br>" +
//            "Only nested files parameter accepted, and only a few parameters.<br>" +
//            "<b>{ files:[ { format, bioformat, path, description, type, jobId, attributes } ] }</b><br>" +
                    "<ul>" +
                    "<il><b>id</b>, <b>lastModified</b> and <b>diskUsage</b> parameters will be ignored.<br></il>" +
                    "<il><b>type</b> accepted values: [<b>'CASE_CONTROL', 'CASE_SET', 'CONTROL_SET', 'FAMILY', 'PAIRED', 'TRIO'</b>].<br></il>" +
                    "<ul>")
    public Response createStudyPOST(@ApiParam(value = "Project id or alias", required = true) @QueryParam("projectId") String projectIdStr,
                                    @ApiParam(value="studies", required = true) List<Study> studies) {
//        List<Study> catalogStudies = new LinkedList<>();
        List<QueryResult<Study>> queryResults = new LinkedList<>();
        long projectId;
        try {
            String userId = catalogManager.getUserManager().getId(sessionId);
            projectId = catalogManager.getProjectManager().getId(userId, projectIdStr);
        } catch (CatalogException e) {
            e.printStackTrace();
            return createErrorResponse(e);
        }
        for (Study study : studies) {
            System.out.println("study = " + study);
            try {
                QueryResult<Study> queryResult = catalogManager.createStudy(projectId, study.getName(),
                        study.getAlias(), study.getType(), study.getCreationDate(),
                        study.getDescription(), new Status(), study.getCipher(), null, null, null, study.getStats(),
                        study.getAttributes(), queryOptions, sessionId);
//                Study studyAdded = queryResult.getResult().get(0);
                queryResults.add(queryResult);
//                List<File> files = study.getFiles();
//                if(files != null) {
//                    for (File file : files) {
//                        QueryResult<File> fileQueryResult = catalogManager.createFile(studyAdded.getId(), file.getType(), file.getFormat(),
//                                file.getBioformat(), file.getPath(), file.getOwnerId(), file.getCreationDate(),
//                                file.getDescription(), file.getName(), file.getDiskUsage(), file.getExperimentId(),
//                                file.getSampleIds(), file.getJobId(), file.getStats(), file.getAttributes(), true, sessionId);
//                        file = fileQueryResult.getResult().get(0);
//                        System.out.println("fileQueryResult = " + fileQueryResult);
//                        studyAdded.getFiles().add(file);
//                    }
//                }
            } catch (Exception e) {
//                queryResults.add(new QueryResult<>("createStudy", 0, 0, 0, "", e, Collections.<Study>emptyList()));
                return createErrorResponse(e);
            }
        }
        return createOkResponse(queryResults);
    }


    @POST
    @Path("/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Search studies", position = 2, response = Study[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "include", value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "exclude", value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "limit", value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "skip", value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getAllStudiesByPost(@ApiParam(value="studies", required = true) Query query) {
        try {
            QueryOptions qOptions = new QueryOptions(queryOptions);
//            parseQueryParams(params, CatalogStudyDBAdaptor.QueryParams::getParam, query, qOptions);
            QueryResult<Study> queryResult = catalogManager.getAllStudies(query, qOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public static class UpdateStudy {

        public String name;
        public Study.Type type;
        public String description;
//        public String status;
//        public String lastModified;
//        public long diskUsage;
//        public String cipher;

        //public URI uri;

        public Map<String, Object> stats;
        public Map<String, Object> attributes;
    }

    @POST
    @Path("/{studyId}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update some study attributes using POST method", position = 3)
    public Response updateByPost(@ApiParam(value = "studyId", required = true) @PathParam("studyId") String studyIdStr,
                                 @ApiParam(value = "params", required = true) UpdateStudy updateParams) {
        try {
            long studyId = catalogManager.getStudyId(studyIdStr, sessionId);
            QueryResult queryResult = catalogManager.modifyStudy(studyId, new QueryOptions(jsonObjectMapper.writeValueAsString(updateParams)), sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
