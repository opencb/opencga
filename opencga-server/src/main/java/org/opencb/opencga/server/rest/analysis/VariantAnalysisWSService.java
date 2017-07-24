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

package org.opencb.opencga.server.rest.analysis;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.*;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.analysis.VariantSampleFilter;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{version}/analysis/variant")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Variant", position = 4, description = "Methods for working with 'files' endpoint")
public class VariantAnalysisWSService extends AnalysisWSService {


    public VariantAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public VariantAnalysisWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }


    @GET
    @Path("/index")
    @ApiOperation(value = "Index variant files", position = 14, response = QueryResponse.class)
    public Response index(@ApiParam(value = "(DEPRECATED) Comma separated list of file ids (files or directories)", hidden = true)
                              @QueryParam (value = "fileId") String fileIdStrOld,
                          @ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                          @QueryParam(value = "file") String fileIdStr,
                          // Study id is not ingested by the analysis index command line. No longer needed.
                          @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyStrOld,
                          @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr,
                          @ApiParam("Output directory id") @QueryParam("outDir") String outDirStr,
                          @ApiParam("Boolean indicating that only the transform step will be run") @DefaultValue("false") @QueryParam("transform") boolean transform,
                          @ApiParam("Boolean indicating that only the load step will be run") @DefaultValue("false") @QueryParam("load") boolean load,
                          @ApiParam("Currently two levels of merge are supported: \"basic\" mode merge genotypes of the same variants while \"advanced\" merge multiallelic and overlapping variants.") @DefaultValue("ADVANCED") @QueryParam("merge") VariantStorageEngine.MergeMode merge,
                          @ApiParam("Comma separated list of fields to be include in the index") @QueryParam("includeExtraFields") String includeExtraFields,
                          @ApiParam("Type of aggregated VCF file: none, basic, EVS or ExAC") @DefaultValue("none") @QueryParam("aggregated") String aggregated,
                          @ApiParam("Calculate indexed variants statistics after the load step") @DefaultValue("false") @QueryParam("calculateStats") boolean calculateStats,
                          @ApiParam("Annotate indexed variants after the load step") @DefaultValue("false") @QueryParam("annotate") boolean annotate,
                          @ApiParam("Overwrite annotations already present in variants") @DefaultValue("false") @QueryParam("overwrite") boolean overwriteAnnotations) {

        if (StringUtils.isNotEmpty(fileIdStrOld)) {
            fileIdStr = fileIdStrOld;
        }

        if (StringUtils.isNotEmpty(studyStrOld)) {
            studyStr = studyStrOld;
        }

        Map<String, String> params = new LinkedHashMap<>();
        addParamIfNotNull(params, "study", studyStr);
        addParamIfNotNull(params, "outdir", outDirStr);
        addParamIfTrue(params, "transform", transform);
        addParamIfTrue(params, "load", load);
        addParamIfNotNull(params, "merge", merge);
        addParamIfNotNull(params, EXTRA_GENOTYPE_FIELDS.key(), includeExtraFields);
        addParamIfNotNull(params, AGGREGATED_TYPE.key(), aggregated);
        addParamIfTrue(params, CALCULATE_STATS.key(), calculateStats);
        addParamIfTrue(params, ANNOTATE.key(), annotate);
        addParamIfTrue(params, VariantAnnotationManager.OVERWRITE_ANNOTATIONS, overwriteAnnotations);

        Set<String> knownParams = new HashSet<>();
        knownParams.add("study");
        knownParams.add("studyId");
        knownParams.add("outDir");
        knownParams.add("transform");
        knownParams.add("load");
        knownParams.add("merge");
        knownParams.add("includeExtraFields");
        knownParams.add("aggregated");
        knownParams.add("calculateStats");
        knownParams.add("annotate");
        knownParams.add("overwrite");
        knownParams.add("sid");
        knownParams.add("include");
        knownParams.add("exclude");

        // Add other params
        query.forEach((key, value) -> {
            if (!knownParams.contains(key)) {
                if (value != null) {
                    params.put(key, value.toString());
                }
            }
        });
        logger.info("ObjectMap: {}", params);

        try {
            QueryResult queryResult = catalogManager.getFileManager().index(fileIdStr, studyStr, "VCF", params, sessionId);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file", position = 15, response = Variant[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getVariants(@ApiParam(value = "List of variant ids") @QueryParam("ids") String ids,
                                @ApiParam(value = "List of regions: {chr}:{start}-{end}") @QueryParam("region") String region,
                                @ApiParam(value = "List of chromosomes") @QueryParam("chromosome") String chromosome,
                                @ApiParam(value = "List of genes") @QueryParam("gene") String gene,
                                @ApiParam(value = "Variant type: [SNV, MNV, INDEL, SV, CNV]") @QueryParam("type") String type,
                                @ApiParam(value = "Reference allele") @QueryParam("reference") String reference,
                                @ApiParam(value = "Main alternate allele") @QueryParam("alternate") String alternate,
                                @ApiParam(value = "", required = true) @QueryParam("studies") String studies,
                                @ApiParam(value = "List of studies to be returned") @QueryParam("returnedStudies") String returnedStudies,
                                @ApiParam(value = "List of samples to be returned") @QueryParam("returnedSamples") String returnedSamples,
                                @ApiParam(value = "List of files to be returned.") @QueryParam("returnedFiles") String returnedFiles,
                                @ApiParam(value = "Variants in specific files") @QueryParam("files") String files,
                                @ApiParam(value = FILTER_DESCR) @QueryParam("filter") String filter,
                                @ApiParam(value = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("maf") String maf,
                                @ApiParam(value = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("mgf") String mgf,
                                @ApiParam(value = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingAlleles") String missingAlleles,
                                @ApiParam(value = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingGenotypes") String missingGenotypes,
                                @ApiParam(value = "Specify if the variant annotation must exists.") @QueryParam("annotationExists") boolean annotationExists,
                                @ApiParam(value = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1") @QueryParam("genotype") String genotype,
                                @ApiParam(value = SAMPLES_DESCR) @QueryParam("samples") String samples,
                                @ApiParam(value = INCLUDE_FORMAT_DESCR) @QueryParam("include-format") String includeFormat,
                                @ApiParam(value = INCLUDE_GENOTYPE_DESCR) @QueryParam("include-genotype") String includeGenotype,
                                @ApiParam(value = VariantCatalogQueryUtils.SAMPLE_FILTER_DESC) @QueryParam("sampleFilter") String sampleFilter,
                                @ApiParam(value = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578") @QueryParam("annot-ct") String annot_ct,
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
                                @ApiParam(value = "Perform a full-text search on a list of traits") @QueryParam("traits") String traits,
                                @ApiParam(value = "Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3") @QueryParam("annot-functional-score") String functional,

                                @ApiParam(value = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]") @QueryParam("unknownGenotype") String unknownGenotype,
//                                @ApiParam(value = "Limit the number of returned variants. Max value: " + VariantFetcher.LIMIT_MAX) @DefaultValue(""+VariantFetcher.LIMIT_DEFAULT) @QueryParam("limit") int limit,
//                                @ApiParam(value = "Skip some number of variants.") @QueryParam("skip") int skip,
                                @ApiParam(value = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.", required = false) @QueryParam("samplesMetadata") boolean samplesMetadata,
                                @ApiParam(value = "Sort the results", required = false) @QueryParam("sort") boolean sort,
                                @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
                                @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000") @QueryParam("interval") int interval,
                                @ApiParam(value = "Fetch summary data from Solr", required = false) @QueryParam("summary") boolean summary,
                                @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge) {

        try {
            List<QueryResult> queryResults = new LinkedList<>();
            QueryResult queryResult = null;
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put("summary", summary);

            Query query = VariantStorageManager.getVariantQuery(queryOptions);

            if (count) {
                queryResult = variantManager.count(query, sessionId);
            } else if (histogram) {
                queryResult = variantManager.getFrequency(query, interval, sessionId);
            } else if (StringUtils.isNotEmpty(groupBy)) {
                queryResult = variantManager.groupBy(groupBy, query, queryOptions, sessionId);
            } else {
                queryResult = variantManager.get(query, queryOptions, sessionId);
//                System.out.println("queryResult = " + jsonObjectMapper.writeValueAsString(queryResult));

//                VariantQueryResult variantQueryResult = variantManager.get(query, queryOptions, sessionId);
//                queryResults.add(variantQueryResult);
            }
            queryResults.add(queryResult);

            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    /**
     * Do not use native values (like boolean or int), so they are null by default.
     */
    private static class VariantQueryParams {
        public String ids;
        public String region;
        public String chromosome;
        public String gene;
        public String type;
        public String reference;
        public String alternate;
        public String studies;
        public String returnedStudies;
        public String returnedSamples;
        public String returnedFiles;
        public String files;
        public String filter;
        public String maf;
        public String mgf;
        public String missingAlleles;
        public String missingGenotypes;
        public Boolean annotationExists;
        public String genotype;
        @JsonProperty("annot-ct")
//        @ApiModelProperty(name = "annot-ct")
        public String annot_ct;
        @JsonProperty("annot-xref")
        public String annot_xref;
        @JsonProperty("annot-biotype")
        public String annot_biotype;
        public String polyphen;
        public String sift;
//        public String protein_substitution;
        public String conservation;
        @JsonProperty("annot-population-maf")
        public String annotPopulationMaf;
        public String alternate_frequency;
        public String reference_frequency;
        @JsonProperty("annot-transcription-flags")
        public String transcriptionFlags;
        @JsonProperty("annot-gene-trait-id")
        public String geneTraitId;
        @JsonProperty("annot-gene-trait-name")
        public String geneTraitName;
        @JsonProperty("annot-hpo")
        public String hpo;
        @JsonProperty("annot-go")
        public String go;
        @JsonProperty("annot-expression")
        public String expression;
        @JsonProperty("annot-protein-keywords")
        public String proteinKeyword;
        @JsonProperty("annot-drug")
        public String drug;
        @JsonProperty("annot-functional-score")
        public String functional;
        public String unknownGenotype;
        public boolean samplesMetadata = false;
        public boolean sort = false;
        public String groupBy;
        public boolean histogram = false;
        public int interval = 2000;
        public boolean merge = false;

    }

    @POST
    @Path("/query")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file", position = 15, response = Variant[].class)
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
    public Response getVariants(@ApiParam(name = "params", value = "Query parameters", required = true) VariantQueryParams params) {
        logger.info("count {} , limit {} , skip {}", count, limit, skip);
        try {
            List<QueryResult> queryResults = new LinkedList<>();
            QueryResult queryResult;
            // Get all query options
            QueryOptions postParams = new QueryOptions(jsonObjectMapper.writeValueAsString(params));
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = VariantStorageManager.getVariantQuery(postParams);

            logger.info("query " + query.toJson());
            logger.info("postParams " + postParams.toJson());
            logger.info("queryOptions " + queryOptions.toJson());

            if (count) {
                queryResult = variantManager.count(query, sessionId);
            } else if (params.histogram) {
                queryResult = variantManager.getFrequency(query, params.interval, sessionId);
            } else if (StringUtils.isNotEmpty(params.groupBy)) {
                queryResult = variantManager.groupBy(params.groupBy, query, queryOptions, sessionId);
            } else {
                queryResult = variantManager.get(query, queryOptions, sessionId);
            }
            queryResults.add(queryResult);

            return createOkResponse(queryResults);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/samples")
    @ApiOperation(value = "Get samples given a set of variants", position = 14, response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "ids", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "chromosome", value = CHROMOSOME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query")
    })
    public Response samples(
            @ApiParam(value = "Study where all the samples belong to") @QueryParam("study") String study,
            @ApiParam(value = "List of samples to check. By default, all samples") @QueryParam("samples") String samples,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotypes") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Samples must be present in ALL variants or in ANY variant.") @QueryParam("all") @DefaultValue("false") boolean all
    ) {
        try {
            VariantSampleFilter variantSampleFilter = new VariantSampleFilter(variantManager.iterable(sessionId));
            List<String> genotypes = Arrays.asList(genotypesStr.split(","));

            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = VariantStorageManager.getVariantQuery(queryOptions);

            if (StringUtils.isNotEmpty(samples)) {
                query.append(RETURNED_SAMPLES.key(), Arrays.asList(samples.split(",")));
                query.remove(SAMPLES.key());
            }
            if (StringUtils.isNotEmpty(study)) {
                query.append(STUDIES.key(), study);
            }

            long studyId = catalogManager.getStudyId(study, sessionId);
            Collection<String> sampleNames;
            if (all) {
                sampleNames = variantSampleFilter.getSamplesInAllVariants(query, genotypes);
            } else {
                Map<String, Set<Variant>> samplesInAnyVariants = variantSampleFilter.getSamplesInAnyVariants(query, genotypes);
                sampleNames = samplesInAnyVariants.keySet();
            }
            Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.NAME.key(), String.join(",", sampleNames));
            QueryResult<Sample> allSamples = catalogManager.getAllSamples(studyId, sampleQuery, queryOptions, sessionId);
            return createOkResponse(allSamples);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/ibs")
    @ApiOperation(value = "[PENDING]", position = 15, response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "limit", value = "[TO BE IMPLEMENTED] Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
    })
    public Response ibs() {
        try {
            return createOkResponse("[PENDING]");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/stats")
    @ApiOperation(value = "Calculate variant stats [PENDING]", position = 2)
    public Response stats() {
        return createErrorResponse(new NotImplementedException("Pending"));
    }

    @GET
    @Path("/facet")
    @ApiOperation(value = "Fetch variant facets", position = 15, response = QueryResponse.class)
/*
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    */
    public Response getFacets(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: studies>>biotype;type") @QueryParam("facet") String facet,
                              @ApiParam(value = "List of facet ranges separated by semicolons with the format {field_name}:{start}:{end}:{step}, e.g.: sift:0:1:0.2;caddRaw:0:30:1") @QueryParam("facetRange") String facetRange,
                              @ApiParam(value = "List of facet intersections separated by semicolons with the format {field_name}:{value1}:{value2}[:{value3}], e.g.: studies:1kG_phase3:EXAC:ESP6500") @QueryParam("facetIntersection") String facetIntersection,
                              @ApiParam(value = "List of variant ids") @QueryParam("ids") String ids,
                                @ApiParam(value = "List of regions: {chr}:{start}-{end}") @QueryParam("region") String region,
                                @ApiParam(value = "List of chromosomes") @QueryParam("chromosome") String chromosome,
                                @ApiParam(value = "List of genes") @QueryParam("gene") String gene,
                                @ApiParam(value = "Variant type: [SNV, MNV, INDEL, SV, CNV]") @QueryParam("type") String type,
                                @ApiParam(value = "Reference allele") @QueryParam("reference") String reference,
                                @ApiParam(value = "Main alternate allele") @QueryParam("alternate") String alternate,
                                @ApiParam(value = "", required = true) @QueryParam("studies") String studies,
//                                @ApiParam(value = "List of studies to be returned") @QueryParam("returnedStudies") String returnedStudies,
//                                @ApiParam(value = "List of samples to be returned") @QueryParam("returnedSamples") String returnedSamples,
//                                @ApiParam(value = "List of files to be returned.") @QueryParam("returnedFiles") String returnedFiles,
                                @ApiParam(value = "Variants in specific files") @QueryParam("files") String files,
                                @ApiParam(value = VariantQueryParam.FILTER_DESCR) @QueryParam("filter") String filter,
                                @ApiParam(value = "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("maf") String maf,
                                @ApiParam(value = "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("mgf") String mgf,
                                @ApiParam(value = "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingAlleles") String missingAlleles,
                                @ApiParam(value = "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}") @QueryParam("missingGenotypes") String missingGenotypes,
                                @ApiParam(value = "Specify if the variant annotation must exists.") @QueryParam("annotationExists") boolean annotationExists,
                                @ApiParam(value = "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1") @QueryParam("genotype") String genotype,
                                @ApiParam(value = VariantQueryParam.SAMPLES_DESCR) @QueryParam("samples") String samples,
                                @ApiParam(value = VariantCatalogQueryUtils.SAMPLE_FILTER_DESC) @QueryParam("sampleFilter") String sampleFilter,
                                @ApiParam(value = "Consequence type SO term list. e.g. missense_variant,stop_lost or SO:0001583,SO:0001578") @QueryParam("annot-ct") String annot_ct,
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
                                @ApiParam(value = "Perform a full-text search on a list of traits") @QueryParam("traits") String traits,
                                @ApiParam(value = "Functional score: {functional_score}[<|>|<=|>=]{number} e.g. cadd_scaled>5.2 , cadd_raw<=0.3") @QueryParam("annot-functional-score") String functional) {

//                                @ApiParam(value = "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]") @QueryParam("unknownGenotype") String unknownGenotype,
//                                @ApiParam(value = "Limit the number of returned variants. Max value: " + VariantFetcher.LIMIT_MAX) @DefaultValue(""+VariantFetcher.LIMIT_DEFAULT) @QueryParam("limit") int limit,
//                                @ApiParam(value = "Skip some number of variants.") @QueryParam("skip") int skip,
//                                @ApiParam(value = "Returns the samples metadata group by study. Sample names will appear in the same order as their corresponding genotypes.", required = false) @QueryParam("samplesMetadata") boolean samplesMetadata,
//                                @ApiParam(value = "Sort the results", required = false) @QueryParam("sort") boolean sort,
//                                @ApiParam(value = "Group variants by: [ct, gene, ensemblGene]", required = false) @DefaultValue("") @QueryParam("groupBy") String groupBy,
//                                @ApiParam(value = "Calculate histogram. Requires one region.", required = false) @DefaultValue("false") @QueryParam("histogram") boolean histogram,
//                                @ApiParam(value = "Histogram interval size", required = false) @DefaultValue("2000") @QueryParam("interval") int interval,
//                                @ApiParam(value = "Fetch summary data from Solr", required = false) @QueryParam("summary") boolean summary,
//                                @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge
        try {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put(QueryOptions.LIMIT, 0);
            queryOptions.put(QueryOptions.SKIP, 0);
            queryOptions.remove(QueryOptions.COUNT);
            queryOptions.remove(QueryOptions.INCLUDE);
            queryOptions.remove(QueryOptions.SORT);

            Query query = VariantStorageManager.getVariantQuery(queryOptions);

            FacetedQueryResult queryResult = variantManager.facet(query, queryOptions, sessionId);

            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }
}



