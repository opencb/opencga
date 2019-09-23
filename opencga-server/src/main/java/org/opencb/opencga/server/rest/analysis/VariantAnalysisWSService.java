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
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleDataManager;
import org.opencb.opencga.storage.core.variant.analysis.VariantSampleFilter;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;
import static org.opencb.opencga.storage.core.manager.CatalogUtils.parseSampleAnnotationQuery;
import static org.opencb.opencga.storage.core.variant.VariantStorageEngine.Options.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/analysis/variant")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Variant", position = 4, description = "Methods for working with 'files' endpoint")
public class VariantAnalysisWSService extends AnalysisWSService {

    private static final String DEPRECATED = " [DEPRECATED] ";
    private static final Map<String, org.opencb.commons.datastore.core.QueryParam> DEPRECATED_VARIANT_QUERY_PARAM;

    static {
        Map<String, org.opencb.commons.datastore.core.QueryParam> map = new LinkedHashMap<>();

        map.put("ids", ID);
        map.put("studies", STUDY);
        map.put("files", FILE);
        map.put("samples", SAMPLE);
        map.put("samplesMetadata", SAMPLE_METADATA);
        map.put("cohorts", COHORT);

        map.put("returnedStudies", INCLUDE_STUDY);
        map.put("returnedSamples", INCLUDE_SAMPLE);
        map.put("returnedFiles", INCLUDE_FILE);
        map.put("include-format", INCLUDE_FORMAT);
        map.put("include-genotype", INCLUDE_GENOTYPE);
        map.put("sampleFilter", VariantCatalogQueryUtils.SAMPLE_ANNOTATION);
        map.put("maf", STATS_MAF);
        map.put("mgf", STATS_MGF);

        map.put("annot-ct", ANNOT_CONSEQUENCE_TYPE);
        map.put("annot-xref", ANNOT_XREF);
        map.put("annot-biotype", ANNOT_BIOTYPE);
        map.put("protein_substitution", ANNOT_PROTEIN_SUBSTITUTION);
        map.put("alternate_frequency", ANNOT_POPULATION_ALTERNATE_FREQUENCY);
        map.put("reference_frequency", ANNOT_POPULATION_REFERENCE_FREQUENCY);
        map.put("annot-population-maf", ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY);
        map.put("annot-transcription-flags", ANNOT_TRANSCRIPT_FLAG);
        map.put("transcriptionFlag", ANNOT_TRANSCRIPT_FLAG);
        map.put("annot-gene-trait-id", ANNOT_GENE_TRAIT_ID);
        map.put("annot-gene-trait-name", ANNOT_GENE_TRAIT_NAME);
        map.put("annot-hpo", ANNOT_HPO);
        map.put("annot-go", ANNOT_GO);
        map.put("annot-expression", ANNOT_EXPRESSION);
        map.put("annot-protein-keywords", ANNOT_PROTEIN_KEYWORD);
        map.put("annot-drug", ANNOT_DRUG);
        map.put("annot-functional-score", ANNOT_FUNCTIONAL_SCORE);
        map.put("annot-custom", CUSTOM_ANNOTATION);
        map.put("traits", ANNOT_TRAIT);

        DEPRECATED_VARIANT_QUERY_PARAM = Collections.unmodifiableMap(map);
    }

    public VariantAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public VariantAnalysisWSService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);
    }


    @GET
    @Path("/index")
    @ApiOperation(value = "Index variant files", position = 14, response = QueryResponse.class)
    public Response index(@Deprecated @ApiParam(value = "(DEPRECATED) Comma separated list of file ids (files or directories)", hidden = true)
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
                          @ApiParam("Index including other FORMAT fields. Use \"" + VariantQueryUtils.ALL + "\", \"" + VariantQueryUtils.NONE + "\", or CSV with the fields to load.") @QueryParam("includeExtraFields") String includeExtraFields,
                          @ApiParam("Type of aggregated VCF file: none, basic, EVS or ExAC") @DefaultValue("none") @QueryParam("aggregated") String aggregated,
                          @ApiParam("Calculate indexed variants statistics after the load step") @DefaultValue("false") @QueryParam("calculateStats") boolean calculateStats,
                          @ApiParam("Annotate indexed variants after the load step") @DefaultValue("false") @QueryParam("annotate") boolean annotate,
                          @ApiParam("Overwrite annotations already present in variants") @DefaultValue("false") @QueryParam("overwrite") boolean overwriteAnnotations,
                          @ApiParam("Add files to the secondary search index") @DefaultValue("false") @QueryParam("indexSearch") boolean indexSearch,
                          @ApiParam("Resume a previously failed indexation") @DefaultValue("false") @QueryParam("resume") boolean resume,
                          @ApiParam("Indicate that the variants from a sample (or group of samples) split into different files (by chromosome, by type, ...)") @DefaultValue("false") @QueryParam("loadSplitData") boolean loadSplitData,
                          @ApiParam("Do not execute post load checks over the database") @DefaultValue("false") @QueryParam("skipPostLoadCheck") boolean skipPostLoadCheck) {

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
        addParamIfTrue(params, INDEX_SEARCH.key(), indexSearch);
        addParamIfTrue(params, VariantAnnotationManager.OVERWRITE_ANNOTATIONS, overwriteAnnotations);
        addParamIfTrue(params, LOAD_SPLIT_DATA.key(), loadSplitData);
        addParamIfTrue(params, POST_LOAD_CHECK_SKIP.key(), skipPostLoadCheck);

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
        knownParams.add("indexSearch");
        knownParams.add("sid");
        knownParams.add("include");
        knownParams.add("exclude");
        knownParams.add("loadSplitData");
        knownParams.add("skipPostLoadCheck");

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
            List<String> idList = getIdList(fileIdStr);
            QueryResult queryResult = catalogManager.getFileManager().index(studyStr, idList, "VCF", params, sessionId);
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
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Total number of results", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP_COUNT, value = "Do not count total number of results", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = VariantField.SUMMARY, value = "Fast fetch of main variant parameters", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCount", value = "Get an approximate count, instead of an exact total count. Reduces execution time", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCountSamplingSize", value = "Sampling size to get the approximate count. "
                    + "Larger values increase accuracy but also increase execution time", dataType = "integer", paramType = "query"),

            // Variant filters
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reference", value = REFERENCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "alternate", value = ALTERNATE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = "project", value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "study", value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "qual", value = QUAL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "info", value = INFO_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "format", value = FORMAT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleMetadata", value = SAMPLE_METADATA_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "unknownGenotype", value = UNKNOWN_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleLimit", value = SAMPLE_LIMIT_DESCR, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = "sampleSkip", value = SAMPLE_SKIP_DESCR, dataType = "integer", paramType = "query"),

            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsRef", value = STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsAlt", value = STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMaf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "family", value = VariantCatalogQueryUtils.FAMILY_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyDisorder", value = VariantCatalogQueryUtils.FAMILY_DISORDER_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familySegregation", value = VariantCatalogQueryUtils.FAMILY_SEGREGATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyMembers", value = VariantCatalogQueryUtils.FAMILY_MEMBERS_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyProband", value = VariantCatalogQueryUtils.FAMILY_PROBAND_DESC, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "includeStudy", value = INCLUDE_STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeFile", value = INCLUDE_FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeSample", value = INCLUDE_SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeFormat", value = INCLUDE_FORMAT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeGenotype", value = INCLUDE_GENOTYPE_DESCR, dataType = "string", paramType = "query"),

            // Annotation filters
            @ApiImplicitParam(name = "annotationExists", value = ANNOT_EXISTS_DESCR, dataType = "boolean", paramType = "query"),
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
            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "customAnnotation", value = CUSTOM_ANNOTATION_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "panel", value = VariantCatalogQueryUtils.PANEL_DESC, dataType = "string", paramType = "query"),

            // WARN: Only available in Solr
            @ApiImplicitParam(name = "trait", value = ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),

//            // DEPRECATED PARAMS
//            @ApiImplicitParam(name = "chromosome", value = DEPRECATED + "Use 'region' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "polyphen", value = DEPRECATED + "Use 'proteinSubstitution' instead. e.g. polyphen>0.1", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sift", value = DEPRECATED + "Use 'proteinSubstitution' instead. e.g. sift>0.1", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneTraitName", value = DEPRECATED + "Use 'trait' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "hpo", value = DEPRECATED + "Use 'geneTraitId' or 'trait' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinvar", value = DEPRECATED + "Use 'xref' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "cosmic", value = DEPRECATED + "Use 'xref' instead", dataType = "string", paramType = "query"),
//
//            // RENAMED PARAMS
//            @ApiImplicitParam(name = "ids", value = DEPRECATED + "Use 'id' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "studies", value = DEPRECATED + "Use 'study' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "files", value = DEPRECATED + "Use 'file' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "samples", value = DEPRECATED + "Use 'sample' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "samplesMetadata", value = DEPRECATED + "Use 'sampleMetadata' instead", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "cohorts", value = DEPRECATED + "Use 'cohort' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "returnedStudies", value = DEPRECATED + "Use 'includeStudy' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "returnedSamples", value = DEPRECATED + "Use 'includeSample' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "returnedFiles", value = DEPRECATED + "Use 'includeFile' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "include-format", value = DEPRECATED + "Use 'includeFormat' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "include-genotype", value = DEPRECATED + "Use 'includeGenotype' instead", dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "annot-ct", value = DEPRECATED + "Use 'ct' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-xref", value = DEPRECATED + "Use 'xref' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-biotype", value = DEPRECATED + "Use 'biotype' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "protein_substitution", value = DEPRECATED + "Use 'proteinSubstitution' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "alternate_frequency", value = DEPRECATED + "Use 'populationFrequencyAlt' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reference_frequency", value = DEPRECATED + "Use 'populationFrequencyRef' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-population-maf", value = DEPRECATED + "Use 'populationFrequencyMaf' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-transcription-flags", value = DEPRECATED + "Use 'transcriptFlags' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-gene-trait-id", value = DEPRECATED + "Use 'geneTraitId' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-gene-trait-name", value = DEPRECATED + "Use 'geneTraitName' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-hpo", value = DEPRECATED + "Use 'hpo' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-go", value = DEPRECATED + "Use 'go' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-expression", value = DEPRECATED + "Use 'expression' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-protein-keywords", value = DEPRECATED + "Use 'proteinKeyword' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-drug", value = DEPRECATED + "Use 'drug' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "annot-functional-score", value = DEPRECATED + "Use 'functionalScore' instead", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "traits", value = DEPRECATED + "Use 'trait' instead", dataType = "string", paramType = "query"),
    })
    public Response getVariants(@ApiParam(value = "Group variants by: [ct, gene, ensemblGene]") @DefaultValue("") @QueryParam("groupBy") String groupBy,
                                @ApiParam(value = "Calculate histogram. Requires one region.") @DefaultValue("false") @QueryParam("histogram") boolean histogram,
                                @ApiParam(value = "Histogram interval size") @DefaultValue("2000") @QueryParam("interval") int interval,
                                @ApiParam(value = "Ranks different entities with the most number of variants. Rank by: [ct, gene, ensemblGene]") @QueryParam("rank") String rank
                                // @ApiParam(value = "Merge results", required = false) @DefaultValue("false") @QueryParam("merge") boolean merge
                                ) {

        try {
            List<QueryResult> queryResults = new LinkedList<>();
            QueryResult queryResult = null;
            // Get all query options

            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);

            if (count) {
                queryResult = variantManager.count(query, sessionId);
            } else if (histogram) {
                queryResult = variantManager.getFrequency(query, interval, sessionId);
            } else if (StringUtils.isNotEmpty(groupBy)) {
                queryResult = variantManager.groupBy(groupBy, query, queryOptions, sessionId);
            } else if (StringUtils.isNotEmpty(rank)) {
                queryResult = variantManager.rank(query, rank,  limit, true, sessionId);
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
        public String id;
        public String region;
        public String chromosome;
        public String gene;
        public String type;
        public String reference;
        public String alternate;
        public String project;
        public String study;

        public String includeStudy;
        public String includeSample;
        public String includeFile;
        public String includeFormat;
        public String includeGenotype;

        public String file;
        public String qual;
        public String filter;
        public String info;

        public String genotype;
        public String sample;
        public String format;
        public String sampleAnnotation;

        public String cohort;
        public String maf;
        public String mgf;
        public String missingAlleles;
        public String missingGenotypes;
        public Boolean annotationExists;

        public String ct;
        public String xref;
        public String biotype;
        @Deprecated public String polyphen;
        @Deprecated public String sift;
        public String proteinSubstitution;
        public String conservation;
        public String populationFrequencyMaf;
        public String populationFrequencyAlt;
        public String populationFrequencyRef;
        public String transcriptFlag;
        public String geneTraitId;
        public String geneTraitName;
        public String trait;
        public String cosmic;
        public String clinvar;
        public String hpo;
        public String go;
        public String expression;
        public String proteinKeyword;
        public String drug;
        public String functionalScore;
        public String clinicalSignificance;
        public String customAnnotation;

        public String unknownGenotype;
        public boolean sampleMetadata = false;
        public boolean sort = false;
        public String groupBy;
        public boolean histogram = false;
        public int interval = 2000;

        @Deprecated public String ids;
        @Deprecated public String studies;
        @Deprecated public String returnedStudies;
        @Deprecated public String returnedSamples;
        @Deprecated public String returnedFiles;
        @Deprecated public String files;
        @Deprecated public String samples;
        @Deprecated public boolean samplesMetadata = false;
        @Deprecated public String cohorts;
        @Deprecated @JsonProperty("include-genotype") public String include_genotype;
        @Deprecated @JsonProperty("include-format") public String include_format;

        @Deprecated @JsonProperty("annot-ct") public String annot_ct;
        @Deprecated @JsonProperty("annot-xref") public String annot_xref;
        @Deprecated @JsonProperty("annot-biotype") public String annot_biotype;
        @Deprecated @JsonProperty("annot-population-maf") public String annot_populationMaf;
        @Deprecated public String alternate_frequency;
        @Deprecated public String reference_frequency;
        @Deprecated @JsonProperty("annot-transcription-flags") public String transcriptFlags;
        @Deprecated @JsonProperty("annot-gene-trait-id") public String annot_geneTraitId;
        @Deprecated @JsonProperty("annot-gene-trait-name") public String annot_geneTraitName;
        @Deprecated @JsonProperty("annot-hpo") public String annot_hpo;
        @Deprecated @JsonProperty("annot-go") public String annot_go;
        @Deprecated @JsonProperty("annot-expression") public String annot_expression;
        @Deprecated @JsonProperty("annot-protein-keywords") public String annot_proteinKeyword;
        @Deprecated @JsonProperty("annot-drug") public String annot_drug;
        @Deprecated @JsonProperty("annot-functional-score") public String annot_functionalScore;
    }

    @POST
    @Path("/query")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file", position = 15, response = Variant[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = "Number of results to skip in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = "Total number of results", dataType = "boolean", paramType = "query")
    })
    public Response getVariants(@ApiParam(name = "params", value = "Query parameters", required = true) VariantQueryParams params) {
        logger.info("count {} , limit {} , skip {}", count, limit, skip);
        try {
            List<QueryResult> queryResults = new LinkedList<>();
            QueryResult queryResult;
            // Get all query options
            QueryOptions postParams = new QueryOptions(getUpdateObjectMapper().writeValueAsString(params));
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(postParams);

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
    @Path("/annotation/query")
    @ApiOperation(value = "Query variant annotations from any saved versions", position = 15, response = VariantAnnotation[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = "Number of results to skip in the queries", dataType = "integer", paramType = "query")
    })
    public Response getAnnotation(@ApiParam(value = "Annotation identifier") @DefaultValue(VariantAnnotationManager.CURRENT) @QueryParam("annotationId") String annotationId) {
        logger.debug("limit {} , skip {}", limit, skip);
        try {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);
            logger.debug("query = {}, queryOptions = {}" + query.toJson(), queryOptions.toJson());

            QueryResult<VariantAnnotation> result = variantManager.getAnnotation(annotationId, query, queryOptions, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/annotation/metadata")
    @ApiOperation(value = "Read variant annotations metadata from any saved versions", position = 15, response = VariantAnnotation[].class)
    public Response getAnnotationMetadata(@ApiParam(value = "Annotation identifier") @QueryParam("annotationId") String annotationId,
                                          @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam("project") String project) {
        try {
            QueryResult<ProjectMetadata.VariantAnnotationMetadata> result =
                    variantManager.getAnnotationMetadata(annotationId, project, sessionId);
            return createOkResponse(result);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/familyGenotypes")
    @ApiOperation(value = "Calculate the possible genotypes for the members of a family", position = 15, response = Map.class)
    public Response calculateGenotypes(
            @ApiParam(value = "Study [[user@]project:]study") @QueryParam("study") String studyStr,
            @ApiParam(value = "Family id") @QueryParam("family") String family,
            @ApiParam(value = "Clinical analysis id") @QueryParam("clinicalAnalysis") String clinicalAnalysis,
            @ApiParam(value = "Mode of inheritance", required = true, defaultValue = "MONOALLELIC")
                @QueryParam("modeOfInheritance") ClinicalProperty.ModeOfInheritance moi,
            @ApiParam(value = "Penetrance", defaultValue = "COMPLETE") @QueryParam("penetrance") ClinicalProperty.Penetrance penetrance,
            @ApiParam(value = "Disorder id") @QueryParam("disorder") String disorder) {
        try {
            if (penetrance == null) {
                penetrance = ClinicalProperty.Penetrance.COMPLETE;
            }

            return createOkResponse(catalogManager.getFamilyManager().calculateFamilyGenotypes(studyStr, clinicalAnalysis, family, moi,
                    disorder, penetrance, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/samples")
    @ApiOperation(value = "Get samples given a set of variants", position = 14, response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response samples(
            @ApiParam(value = "Study where all the samples belong to") @QueryParam("study") String studyStr,
            @ApiParam(value = "List of samples to check. By default, all samples") @QueryParam("sample") String samples,
            @ApiParam(value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC) @QueryParam("sampleAnnotation") String sampleAnnotation,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Samples must be present in ALL variants or in ANY variant.") @QueryParam("all") @DefaultValue("false") boolean all
    ) {
        try {
            VariantSampleFilter variantSampleFilter = new VariantSampleFilter(variantManager.iterable(sessionId));
            List<String> genotypes = Arrays.asList(genotypesStr.split(","));

            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);

            if (StringUtils.isNotEmpty(samples) && StringUtils.isNotEmpty(sampleAnnotation)) {
                throw new IllegalArgumentException("Use only one parameter between '" + SAMPLE.key() + "' "
                        + "and '" + VariantCatalogQueryUtils.SAMPLE_ANNOTATION.key() + "'.");
            }

            if (StringUtils.isNotEmpty(samples)) {
                query.append(INCLUDE_SAMPLE.key(), Arrays.asList(samples.split(",")));
                query.remove(SAMPLE.key());
            }

            if (StringUtils.isNotEmpty(sampleAnnotation)) {
                Query sampleQuery = parseSampleAnnotationQuery(sampleAnnotation, SampleDBAdaptor.QueryParams::getParam);
                QueryOptions options = new QueryOptions(INCLUDE, SampleDBAdaptor.QueryParams.UID);
                List<String> samplesList = catalogManager.getSampleManager().search(studyStr, sampleQuery, options, sessionId)
                        .getResult()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toList());

                query.append(INCLUDE_SAMPLE.key(), samplesList);
                query.remove(VariantCatalogQueryUtils.SAMPLE_ANNOTATION.key());
            }

            if (StringUtils.isNotEmpty(studyStr)) {
                query.append(STUDY.key(), studyStr);
            }

            // Remove "genotype" from query, as it could be mixed with que VariantQueryParam "genotype"
            if (StringUtils.isNotEmpty(genotypesStr)) {
                query.remove(GENOTYPE.key());
            }

            Collection<String> sampleNames;
            if (all) {
                sampleNames = variantSampleFilter.getSamplesInAllVariants(query, genotypes);
            } else {
                Map<String, Set<Variant>> samplesInAnyVariants = variantSampleFilter.getSamplesInAnyVariants(query, genotypes);
                sampleNames = samplesInAnyVariants.keySet();
            }
            Query sampleQuery = new Query(SampleDBAdaptor.QueryParams.ID.key(), String.join(",", sampleNames));
            QueryResult<Sample> allSamples = catalogManager.getSampleManager().search(studyStr, sampleQuery, queryOptions, sessionId);
            return createOkResponse(allSamples);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variant}/sampleData")
    @ApiOperation(value = "Get sample data of a given variant", position = 15, response = VariantSampleData.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = QueryOptions.LIMIT, value = "Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = "Number of results to skip in the queries", dataType = "integer", paramType = "query")
    })
    public Response sampleData(
            @ApiParam(value = "Variant") @PathParam("variant") String variant,
            @ApiParam(value = "Study where all the samples belong to") @QueryParam("study") String studyStr,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Do not group by genotype. Return all genotypes merged.") @QueryParam(VariantSampleDataManager.MERGE) @DefaultValue("false") boolean merge
    ) {
        try {
            queryOptions.putAll(query);
            QueryResult<VariantSampleData> sampleData = variantManager.getSampleData(variant, studyStr, queryOptions, sessionId);
            return createOkResponse(sampleData);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

//    @GET
//    @Path("/ibs")
//    @ApiOperation(value = "[PENDING]", position = 15, response = Job.class)
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = "limit", value = "[TO BE IMPLEMENTED] Number of results to be returned in the queries", dataType = "integer", paramType = "query"),
//    })
//    public Response ibs() {
//        try {
//            return createOkResponse("[PENDING]");
//        } catch (Exception e) {
//            return createErrorResponse(e);
//        }
//    }

    @POST
    @Path("/validate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Validate a VCF file", response = QueryResponse.class)
    public Response validate(
            @ApiParam(value = "Study [[user@]project:]study where study and project are the id") @QueryParam("study") String studyStr,
            @ApiParam(value = "VCF file id, name or path", required = true) @QueryParam("file") String file) {
        try {
            Map<String, String> params = new HashMap<>();
            params.put("input", file);

            QueryResult<Job> queryResult = catalogManager.getJobManager().create(studyStr, "", "", "opencga-analysis", "variant validate",
                    null, params, sessionId);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }


    @GET
    @Path("/cohortStats")
    @ApiOperation(value = "Calculate variant stats [PENDING]", position = 2)
    public Response cohortStats() {
        return createErrorResponse(new NotImplementedException("Pending"));
    }

    @Deprecated
    @GET
    @Path("/facet")
    @ApiOperation(value = "This method has been renamed, use endpoint /aggregationStats instead" + DEPRECATED, position = 15, hidden = true, response = QueryResponse.class)
    public Response getFacets(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: studies>>biotype;type") @QueryParam("facet") String facet,
                              @ApiParam(value = "List of facet ranges separated by semicolons with the format {field_name}:{start}:{end}:{step}, e.g.: sift:0:1:0.2;caddRaw:0:30:1") @QueryParam("facetRange") String facetRange) {
        return getAggregationStats(facet);
    }

    @Deprecated
    @GET
    @Path("/stats")
    @ApiOperation(value = "This method has been renamed, use endpoint /aggregationStats instead" + DEPRECATED, position = 15, hidden = true, response = QueryResponse.class)
    public Response getStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: studies>>biotype;type") @QueryParam("facet") String facet,
                              @ApiParam(value = "List of facet ranges separated by semicolons with the format {field_name}:{start}:{end}:{step}, e.g.: sift:0:1:0.2;caddRaw:0:30:1") @QueryParam("facetRange") String facetRange) {
        return getAggregationStats(facet);
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Calculate and fetch aggregation stats", position = 15, response = QueryResponse.class)
    @ApiImplicitParams({
            // Variant filters
//            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reference", value = REFERENCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "alternate", value = ALTERNATE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = "project", value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "study", value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "samplesMetadata", value = SAMPLE_METADATA_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "unknownGenotype", value = UNKNOWN_GENOTYPE_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsRef", value = STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsAlt", value = STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMaf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "family", value = VariantCatalogQueryUtils.FAMILY_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyDisorder", value = VariantCatalogQueryUtils.FAMILY_DISORDER_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familySegregation", value = VariantCatalogQueryUtils.FAMILY_SEGREGATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyMembers", value = VariantCatalogQueryUtils.FAMILY_MEMBERS_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyProband", value = VariantCatalogQueryUtils.FAMILY_PROBAND_DESC, dataType = "string", paramType = "query"),

            // Annotation filters
            @ApiImplicitParam(name = "annotationExists", value = ANNOT_EXISTS_DESCR, dataType = "boolean", paramType = "query"),
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
            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "customAnnotation", value = CUSTOM_ANNOTATION_DESCR, dataType = "string", paramType = "query"),

            // WARN: Only available in Solr
            @ApiImplicitParam(name = "trait", value = ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
    })
    public Response getAggregationStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: chromosome>>type;percentile(gerp)") @QueryParam("fields") String fields) {
        try {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put(QueryOptions.FACET, fields);
            Query query = getVariantQuery(queryOptions);

            FacetQueryResult queryResult = variantManager.facet(query, queryOptions, sessionId);
            return createOkResponse(queryResult);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/metadata")
    @ApiOperation(value = "", position = 15, response = VariantMetadata.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "project", value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "study", value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeStudy", value = INCLUDE_STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeFile", value = INCLUDE_FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeSample", value = INCLUDE_SAMPLE_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = "Fields included in the response, whole JSON path must be provided", example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = "Fields excluded in the response, whole JSON path must be provided", example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response metadata() {
        try {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);
            return createOkResponse(variantManager.getMetadata(query, queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    // FIXME This method must be deleted once deprecated params are not supported any more
    public static Query getVariantQuery(QueryOptions queryOptions) {
        Query query = VariantStorageManager.getVariantQuery(queryOptions);
        queryOptions.forEach((key, value) -> {
            org.opencb.commons.datastore.core.QueryParam newKey = DEPRECATED_VARIANT_QUERY_PARAM.get(key);
            if (newKey != null) {
                if (!VariantQueryUtils.isValidParam(query, newKey)) {
                    query.put(newKey.key(), value);
                }
            }
        });

        String chromosome = queryOptions.getString("chromosome");
        if (StringUtils.isNotEmpty(chromosome)) {
            String region = query.getString(REGION.key());
            if (StringUtils.isEmpty(region)) {
                query.put(REGION.key(), chromosome);
            } else {
                query.put(REGION.key(), region + VariantQueryUtils.OR + chromosome);
            }
        }
        return query;
    }
}

