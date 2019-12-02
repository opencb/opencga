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

import io.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.VariantStorageManager;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.wrappers.PlinkWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.RvtestsWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.results.OpenCGAResult;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleData;
import org.opencb.opencga.storage.core.variant.adaptors.sample.VariantSampleDataManager;
import org.opencb.opencga.storage.core.variant.analysis.VariantSampleFilter;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.opencga.analysis.variant.CatalogUtils.parseSampleAnnotationQuery;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;
import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.*;
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
    public static final String PENDING = " [PENDING] ";
    private static final Map<String, org.opencb.commons.datastore.core.QueryParam> DEPRECATED_VARIANT_QUERY_PARAM;

    static {
        Map<String, org.opencb.commons.datastore.core.QueryParam> map = new LinkedHashMap<>();

        map.put("ids", ID);
        map.put(ParamConstants.STUDIES_PARAM, STUDY);
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

    @Deprecated
    @GET
    @Path("/index")
    @ApiOperation(value = DEPRECATED + "Use operations/variant/file/index instead", response = DataResponse.class)
    public Response index(@Deprecated @ApiParam(value = "(DEPRECATED) Comma separated list of file ids (files or directories)", hidden = true)
                          @QueryParam (value = "fileId") String fileIdStrOld,
                          @ApiParam(value = "Comma separated list of file ids (files or directories)", required = true)
                          @QueryParam(value = "file") String fileIdStr,
                          // Study id is not ingested by the analysis index command line. No longer needed.
                          @ApiParam(value = "(DEPRECATED) Study id", hidden = true) @QueryParam("studyId") String studyStrOld,
                          @ApiParam(value = ParamConstants.STUDY_DESCRIPTION)
                          @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
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

        Map<String, Object> params = new LinkedHashMap<>();
        addParamIfNotNull(params, "file", fileIdStr);
        addParamIfNotNull(params, ParamConstants.STUDY_PARAM, studyStr);
        addParamIfNotNull(params, "outdir", outDirStr);
        addParamIfTrue(params, "transform", transform);
        addParamIfTrue(params, "load", load);
        addParamIfNotNull(params, "merge", merge);
        addParamIfNotNull(params, EXTRA_FORMAT_FIELDS.key(), includeExtraFields);
        addParamIfNotNull(params, STATS_AGGREGATION.key(), aggregated);
        addParamIfTrue(params, STATS_CALCULATE.key(), calculateStats);
        addParamIfTrue(params, ANNOTATE.key(), annotate);
        addParamIfTrue(params, INDEX_SEARCH.key(), indexSearch);
        addParamIfTrue(params, ANNOTATION_OVERWEITE.key(), overwriteAnnotations);
        addParamIfTrue(params, LOAD_SPLIT_DATA.key(), loadSplitData);
        addParamIfTrue(params, POST_LOAD_CHECK_SKIP.key(), skipPostLoadCheck);

        Set<String> knownParams = new HashSet<>();
        knownParams.add(ParamConstants.STUDY_PARAM);
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
        knownParams.add(QueryOptions.INCLUDE);
        knownParams.add(QueryOptions.EXCLUDE);
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
//            List<String> idList = getIdList(fileIdStr);
            OpenCGAResult<Job> queryResult = catalogManager.getJobManager().submit(studyStr, "variant", "index", Enums.Priority.HIGH,
                    params, token);
//            DataResult queryResult = catalogManager.getFileManager().index(studyStr, idList, "VCF", params, token);
            return createOkResponse(queryResult);
        } catch(Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file", response = Variant[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
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
            @ApiImplicitParam(name = ParamConstants.PROJECT_PARAM, value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
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
            @ApiImplicitParam(name = "score", value = SCORE_DESCR, dataType = "string", paramType = "query"),

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
//            @ApiImplicitParam(name = Params.STUDIES_PARAM, value = DEPRECATED + "Use 'study' instead", dataType = "string", paramType = "query"),
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
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);

            if (count) {
                return variantManager.count(query, token);
            } else if (histogram) {
                return variantManager.getFrequency(query, interval, token);
            } else if (StringUtils.isNotEmpty(groupBy)) {
                return variantManager.groupBy(groupBy, query, queryOptions, token);
            } else if (StringUtils.isNotEmpty(rank)) {
                return variantManager.rank(query, rank, limit, true, token);
            } else {
                return variantManager.get(query, queryOptions, token);
            }
        });
    }

    /**
     * Do not use native values (like boolean or int), so they are null by default.
     */
    public static class VariantQueryParams extends RestBodyParams {

        public VariantQueryParams() {
        }

        public VariantQueryParams(Query query) {
            for (String key : query.keySet()) {
                try {
                    Field field = getClass().getDeclaredField(key);
                    if (field.getType().equals(String.class)) {
                        field.set(this, query.getString(key));
                    } else if (field.getType().equals(Boolean.class)) {
                        field.set(this, query.getBoolean(key));
                    } else {
                        field.set(this, query.getInt(key));
                    }
                } catch (NoSuchFieldException ignore) {
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public String id;
        public String region;
        public String chromosome;
        public String gene;
        public String type;
        public String reference;
        public String alternate;
        public String project;
        public String study;
        public String release;

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
        public Integer sampleLimit;
        public Integer sampleSkip;
        public String format;
        public String sampleAnnotation;

        public String family;
        public String familyMembers;
        public String familyDisorder;
        public String familyProband;
        public String familySegregation;
        public String panel;

        public String cohort;
        public String cohortStatsRef;
        public String cohortStatsAlt;
        public String cohortStatsMaf;
        public String cohortStatsMgf;
        public String maf;
        public String mgf;
        public String missingAlleles;
        public String missingGenotypes;
        public Boolean annotationExists;

        public String score;

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
    }

    @POST
    @Path("/query")
    @ApiOperation(value = "Fetch variants from a VCF/gVCF file", response = Variant[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query")
    })
    public Response getVariants(@ApiParam(name = "params", value = "Query parameters", required = true) VariantQueryParams params) {
        return run(() -> {
            logger.info("count {} , limit {} , skip {}", count, limit, skip);
            // Get all query options
            QueryOptions postParams = new QueryOptions(getUpdateObjectMapper().writeValueAsString(params));
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(postParams);

            logger.info("query " + query.toJson());
            logger.info("postParams " + postParams.toJson());
            logger.info("queryOptions " + queryOptions.toJson());

            if (count) {
                return variantManager.count(query, token);
            }else if (StringUtils.isNotEmpty(params.groupBy)) {
                return variantManager.groupBy(params.groupBy, query, queryOptions, token);
            } else {
                return variantManager.get(query, queryOptions, token);
            }
        });
    }

    public static class VariantExportParams extends VariantQueryParams {
        public VariantExportParams() {
        }
        public VariantExportParams(Query query, String outdir, String outputFileName, String outputFormat, boolean compress) {
            super(query);
            this.outdir = outdir;
            this.outputFileName = outputFileName;
            this.outputFormat = outputFormat;
            this.compress = compress;
        }

        public String outdir;
        public String outputFileName;
        public String outputFormat;
        public boolean compress;
    }

    @POST
    @Path("/export/run")
    @ApiOperation(value = "Export variants to a file", response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
    })
    public Response export(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            VariantExportParams params) {
        logger.info("count {} , limit {} , skip {}", count, limit, skip);
        // FIXME: What if exporting from multiple studies?
        return submitJob(study, "variant", "export-run", params, jobName, jobDescription, jobTags);
    }

    @GET
    @Path("/annotation/query")
    @ApiOperation(value = "Query variant annotations from any saved versions", response = VariantAnnotation[].class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query")
    })
    public Response getAnnotation(@ApiParam(value = "Annotation identifier") @DefaultValue(VariantAnnotationManager.CURRENT) @QueryParam("annotationId") String annotationId) {
        logger.debug("limit {} , skip {}", limit, skip);
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);
            logger.debug("query = {}, queryOptions = {}" + query.toJson(), queryOptions.toJson());

            return variantManager.getAnnotation(annotationId, query, queryOptions, token);
        });
    }

    @GET
    @Path("/annotation/metadata")
    @ApiOperation(value = "Read variant annotations metadata from any saved versions", response = VariantAnnotation[].class)
    public Response getAnnotationMetadata(@ApiParam(value = "Annotation identifier") @QueryParam("annotationId") String annotationId,
                                          @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project) {
        return run(() -> variantManager.getAnnotationMetadata(annotationId, project, token));
    }

    public static class StatsRunParams extends RestBodyParams {
        public StatsRunParams() {
        }
        public StatsRunParams(List<String> cohort, List<String> samples, boolean index, String outdir, String outputFileName,
                              String region, String gene, boolean overwriteStats, boolean updateStats, boolean resume,
                              Aggregation aggregated, String aggregationMappingFile) {
            this.cohort = cohort;
            this.samples = samples;
            this.index = index;
            this.outdir = outdir;
            this.outputFileName = outputFileName;
            this.region = region;
            this.gene = gene;
            this.overwriteStats = overwriteStats;
            this.updateStats = updateStats;
            this.resume = resume;
            this.aggregated = aggregated;
            this.aggregationMappingFile = aggregationMappingFile;
        }

        public List<String> cohort;
        public List<String> samples;
        public boolean index;
        public String region;
        public String gene;
        public String outdir;
        public String outputFileName;
        public boolean overwriteStats;
        public boolean updateStats;

        public boolean resume;

        public Aggregation aggregated;
        public String aggregationMappingFile;
    }

    @POST
    @Path("/stats/run")
    @ApiOperation(value = "Create and load stats into a database.", response = Job.class)
    public Response statsRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            StatsRunParams params) {
        return submitJob(study, "variant", "stats-run", params, jobName, jobDescription, jobTags);
    }

    public static class StatsExportParams extends RestBodyParams {
        public StatsExportParams() {
        }
        public StatsExportParams(List<String> cohorts, String output, String region, String gene, String outputFormat) {
            this.cohorts = cohorts;
            this.output = output;
            this.region = region;
            this.gene = gene;
            this.outputFormat = outputFormat;
        }
        public List<String> cohorts;
        public String output;
        public String region;
        public String gene;
        public String outputFormat;
    }

    @POST
    @Path("/stats/export/run")
    @ApiOperation(value = "Export calculated variant stats and frequencies", response = Job.class)
    public Response statsExport(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            StatsExportParams params) {
        return submitJob(study, "variant", "stats-export-run", params, jobName, jobDescription, jobTags);
    }

    public static class StatsDeleteParams extends RestBodyParams {
        public String study;
        public List<String> cohorts;
    }

    @DELETE
    @Path("/stats/delete")
    @ApiOperation(value = PENDING)
    public Response statsDelete(StatsDeleteParams params) {
        return createPendingResponse();
    }

    @GET
    @Path("/familyGenotypes")
    @ApiOperation(value = DEPRECATED + "Use family/genotypes", response = Map.class)
    public Response calculateGenotypes(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
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
                    disorder, penetrance, token));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/family/genotypes")
    @ApiOperation(value = "Calculate the possible genotypes for the members of a family", response = Map.class)
    public Response familyGenotypes(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Family id") @QueryParam("family") String family,
            @ApiParam(value = "Clinical analysis id") @QueryParam("clinicalAnalysis") String clinicalAnalysis,
            @ApiParam(value = "Mode of inheritance", required = true, defaultValue = "MONOALLELIC")
            @QueryParam("modeOfInheritance") ClinicalProperty.ModeOfInheritance moi,
            @ApiParam(value = "Penetrance", defaultValue = "COMPLETE") @QueryParam("penetrance") ClinicalProperty.Penetrance penetrance,
            @ApiParam(value = "Disorder id") @QueryParam("disorder") String disorder) {
        return run(() -> {
            Map<String, List<String>> map = catalogManager.getFamilyManager().calculateFamilyGenotypes(studyStr, clinicalAnalysis, family, moi,
                    disorder, penetrance == null ? ClinicalProperty.Penetrance.COMPLETE : penetrance, token);
            return new OpenCGAResult<>().setResults(Collections.singletonList(map));
        });
    }

    @POST
    @Path("/family/stats/run")
    @ApiOperation(value = PENDING, response = Job.class)
    public Response familyStatsRun(RestBodyParams params) {
        return createPendingResponse();
    }

    @GET
    @Path("/family/stats/query")
    @ApiOperation(value = PENDING)
    public Response familyStatsQuery() {
        return createPendingResponse();
    }

    @DELETE
    @Path("/family/stats/delete")
    @ApiOperation(value = PENDING)
    public Response familyStatsDelete() {
        return createPendingResponse();
    }


    @GET
    @Path("/samples")
    @ApiOperation(value = DEPRECATED + "Use /sample/query", response = Sample.class)
    public Response samples(
            @ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "List of samples to check. By default, all samples") @QueryParam("sample") String samples,
            @ApiParam(value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC) @QueryParam("sampleAnnotation") String sampleAnnotation,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Samples must be present in ALL variants or in ANY variant.") @QueryParam("all") @DefaultValue("false") boolean all
    ) {
        return sampleQuery(studyStr, samples, sampleAnnotation, genotypesStr, all);
    }

    @GET
    @Path("/sample/query")
    @ApiOperation(value = "Get samples given a set of variants", position = 14, response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response sampleQuery(
            @ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "List of samples to check. By default, all samples") @QueryParam("sample") String samples,
            @ApiParam(value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC) @QueryParam("sampleAnnotation") String sampleAnnotation,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Samples must be present in ALL variants or in ANY variant.") @QueryParam("all") @DefaultValue("false") boolean all
    ) {
        try {
            VariantSampleFilter variantSampleFilter = new VariantSampleFilter(variantManager.iterable(token));
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
                List<String> samplesList = catalogManager.getSampleManager().search(studyStr, sampleQuery, options, token)
                        .getResults()
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
            DataResult<Sample> allSamples = catalogManager.getSampleManager().search(studyStr, sampleQuery, queryOptions, token);
            return createOkResponse(allSamples);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/{variant}/sampleData")
    @ApiOperation(value = DEPRECATED + " User sample/data", response = VariantSampleData.class)
    public Response sampleDataOld(
            @ApiParam(value = "Variant") @PathParam("variant") String variant,
            @ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Do not group by genotype. Return all genotypes merged.") @QueryParam(VariantSampleDataManager.MERGE) @DefaultValue("false") boolean merge) {
        return sampleData(variant, studyStr, genotypesStr, merge);
    }

    @GET
    @Path("/sample/data")
    @ApiOperation(value = "Get sample data of a given variant", response = VariantSampleData.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query")
    })
    public Response sampleData(
            @ApiParam(value = "Variant") @QueryParam("variant") String variant,
            @ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Do not group by genotype. Return all genotypes merged.") @QueryParam(VariantSampleDataManager.MERGE) @DefaultValue("false") boolean merge
    ) {
        return run(() -> {
            queryOptions.putAll(query);
            return variantManager.getSampleData(variant, studyStr, queryOptions, token);
        });
    }

    public static class SampleStatsRunParams extends RestBodyParams {
        public SampleStatsRunParams() {
        }
        public SampleStatsRunParams(List<String> sample, String family,
                                    boolean index, String sampleAnnotation, String outdir) {
            this.sample = sample;
            this.family = family;
            this.index = index;
            this.sampleAnnotation = sampleAnnotation;
            this.outdir = outdir;
        }
        public List<String> sample;
        public String family;
        public boolean index;
        public String sampleAnnotation;
        public String outdir;
    }

    @POST
    @Path("/sample/stats/run")
    @ApiOperation(value = SampleVariantStatsAnalysis.DESCRIPTION, response = Job.class)
    public Response sampleStatsRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            SampleStatsRunParams params) {
        return submitJob(study, "variant", "sample-stats-run", params, jobName, jobDescription, jobTags);
    }

    @GET
    @Path("/sample/stats/info")
    @ApiOperation(value = "Read sample variant stats from list of samples.", response = SampleVariantStats.class)
    public Response sampleStatsInfo(@ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                     @ApiParam(value = "Samples") @QueryParam("sample") String sample) {
        return run(() -> {
            ParamUtils.checkParameter(sample, "sample");
            ParamUtils.checkParameter(studyStr, ParamConstants.STUDY_PARAM);
            OpenCGAResult<Sample> result = catalogManager.getSampleManager().get(studyStr, Arrays.asList(sample.split(",")), new QueryOptions(), token);

            List<SampleVariantStats> stats = new ArrayList<>(result.getNumResults());
            for (Sample s : result.getResults()) {

                for (AnnotationSet annotationSet : s.getAnnotationSets()) {
                    if (annotationSet.getVariableSetId().equals(SampleVariantStatsAnalysis.VARIABLE_SET_ID)) {
                        stats.add(AvroToAnnotationConverter.convertAnnotationToAvro(annotationSet, SampleVariantStats.class));
                    }
                }
            }
            OpenCGAResult<SampleVariantStats> statsResult = new OpenCGAResult<>();
            statsResult.setResults(stats);
            statsResult.setNumMatches(result.getNumMatches());
            statsResult.setEvents(result.getEvents());
            statsResult.setTime(result.getTime());
            statsResult.setNode(result.getNode());

            return statsResult;
        });
    }

    @DELETE
    @Path("/sample/stats/delete")
    @ApiOperation(value = "Delete sample variant stats from a sample.", response = SampleVariantStats.class)
    public Response sampleStatsDelete(@ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                      @ApiParam(value = "Sample") @QueryParam("sample") String sample) {
        return run(() -> catalogManager
                .getSampleManager()
                .removeAnnotationSet(studyStr, sample, SampleVariantStatsAnalysis.VARIABLE_SET_ID, queryOptions, token));
    }

    public static class CohortStatsRunParams extends RestBodyParams {
        public CohortStatsRunParams() {
        }
        public CohortStatsRunParams(String cohort, List<String> samples, boolean index, String sampleAnnotation,
                                    String outdir) {
            this.cohort = cohort;
            this.samples = samples;
            this.index = index;
            this.sampleAnnotation = sampleAnnotation;
            this.outdir = outdir;
        }

        public String cohort;
        public List<String> samples;
        public boolean index;
        public String sampleAnnotation;
        public String outdir;
    }

    @POST
    @Path("/cohort/stats/run")
    @ApiOperation(value = CohortVariantStatsAnalysis.DESCRIPTION, response = Job.class)
    public Response cohortStatsRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            CohortStatsRunParams params) {
        return submitJob(study, "variant", "cohort-stats-run", params, jobName, jobDescription, jobTags);
    }

    @GET
    @Path("/cohort/stats/info")
    @ApiOperation(value = "Read cohort variant stats from list of cohorts.", response = VariantSetStats.class)
    public Response cohortStatsQuery(@ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                     @ApiParam(value = "Cohorts list") @QueryParam("cohort") String cohort) {
        return run(() -> {
            ParamUtils.checkParameter(cohort, "cohort");
            ParamUtils.checkParameter(studyStr, ParamConstants.STUDY_PARAM);
            OpenCGAResult<Cohort> result = catalogManager.getCohortManager()
                    .get(studyStr, Arrays.asList(cohort.split(",")), new QueryOptions(), token);

            List<VariantSetStats> stats = new ArrayList<>(result.getNumResults());
            for (Cohort c : result.getResults()) {

                for (AnnotationSet annotationSet : c.getAnnotationSets()) {
                    if (annotationSet.getVariableSetId().equals(CohortVariantStatsAnalysis.VARIABLE_SET_ID)) {
                        stats.add(AvroToAnnotationConverter.convertAnnotationToAvro(annotationSet, VariantSetStats.class));
                    }
                }
            }
            OpenCGAResult<VariantSetStats> statsResult = new OpenCGAResult<>();
            statsResult.setResults(stats);
            statsResult.setNumMatches(result.getNumMatches());
            statsResult.setEvents(result.getEvents());
            statsResult.setTime(result.getTime());
            statsResult.setNode(result.getNode());

            return statsResult;
        });
    }

    @DELETE
    @Path("/cohort/stats/delete")
    @ApiOperation(value = "Delete cohort variant stats from a cohort.", response = SampleVariantStats.class)
    public Response cohortStatsDelete(@ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                      @ApiParam(value = "Cohort") @QueryParam("cohort") String cohort) {
        return run(() -> catalogManager
                .getCohortManager()
                .removeAnnotationSet(studyStr, cohort, CohortVariantStatsAnalysis.VARIABLE_SET_ID, queryOptions, token));
    }

    @Deprecated
    @GET
    @Path("/facet")
    @ApiOperation(value = "This method has been renamed, use endpoint /aggregationStats instead" + DEPRECATED, hidden = true, response = QueryResponse.class)
    public Response getFacets(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: studies>>biotype;type") @QueryParam("facet") String facet,
                              @ApiParam(value = "List of facet ranges separated by semicolons with the format {field_name}:{start}:{end}:{step}, e.g.: sift:0:1:0.2;caddRaw:0:30:1") @QueryParam("facetRange") String facetRange) {
        return getAggregationStats(facet);
    }

    @Deprecated
    @GET
    @Path("/stats")
    @ApiOperation(value = "This method has been renamed, use endpoint /aggregationStats instead" + DEPRECATED, hidden = true, response = QueryResponse.class)
    public Response getStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: studies>>biotype;type") @QueryParam("facet") String facet,
                             @ApiParam(value = "List of facet ranges separated by semicolons with the format {field_name}:{start}:{end}:{step}, e.g.: sift:0:1:0.2;caddRaw:0:30:1") @QueryParam("facetRange") String facetRange) {
        return getAggregationStats(facet);
    }

    @GET
    @Path("/aggregationStats")
    @ApiOperation(value = "Calculate and fetch aggregation stats", response = QueryResponse.class)
    @ApiImplicitParams({
            // Variant filters
//            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reference", value = REFERENCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "alternate", value = ALTERNATE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = ParamConstants.PROJECT_PARAM, value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
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
            @ApiImplicitParam(name = "score", value = SCORE_DESCR, dataType = "string", paramType = "query"),

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
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put(QueryOptions.FACET, fields);
            Query query = getVariantQuery(queryOptions);

            return variantManager.facet(query, queryOptions, token);
        });
    }

    @GET
    @Path("/metadata")
    @ApiOperation(value = "", response = VariantMetadata.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = ParamConstants.PROJECT_PARAM, value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeStudy", value = INCLUDE_STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeFile", value = INCLUDE_FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeSample", value = INCLUDE_SAMPLE_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response metadata() {
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);
            return variantManager.getMetadata(query, queryOptions, token);
        });
    }

    public static class GwasRunParams extends RestBodyParams {
        public GwasRunParams() {
        }
        public GwasRunParams(String phenotype, boolean index, String indexScoreId, GwasConfiguration.Method method,
                             GwasConfiguration.FisherMode fisherMode,
                             String caseCohort, String caseCohortSamplesAnnotation, List<String> caseCohortSamples,
                             String controlCohort, String controlCohortSamplesAnnotation, List<String> controlCohortSamples,
                             String outdir) {
            this.phenotype = phenotype;
            this.index = index;
            this.indexScoreId = indexScoreId;
            this.method = method;
            this.fisherMode = fisherMode;
            this.caseCohort = caseCohort;
            this.caseCohortSamplesAnnotation = caseCohortSamplesAnnotation;
            this.caseCohortSamples = caseCohortSamples;
            this.controlCohort = controlCohort;
            this.controlCohortSamplesAnnotation = controlCohortSamplesAnnotation;
            this.controlCohortSamples = controlCohortSamples;
            this.outdir = outdir;
        }

        public String phenotype;
        public boolean index;
        public String indexScoreId;
        public GwasConfiguration.Method method;
        public GwasConfiguration.FisherMode fisherMode;
        public String caseCohort;
        public String caseCohortSamplesAnnotation;
        public List<String> caseCohortSamples;
        public String controlCohort;
        public String controlCohortSamplesAnnotation;
        public List<String> controlCohortSamples;
        public String outdir;
    }

    @POST
    @Path("/gwas/run")
    @ApiOperation(value = GwasAnalysis.DESCRIPTION, response = Job.class)
    public Response gwasRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            GwasRunParams params) {
        return submitJob(study, "variant", "gwas-run", params, jobName, jobDescription, jobTags);
    }

    @POST
    @Path("/ibs/run")
    @ApiOperation(value = PENDING, response = Job.class)
    public Response ibsRun() {
        return createPendingResponse();
    }

    @GET
    @Path("/ibs/query")
    @ApiOperation(value = PENDING)
    public Response ibsQuery() {
        return createPendingResponse();
    }

    public static class PlinkRunParams extends RestBodyParams {
        public PlinkRunParams() {
        }
        public PlinkRunParams(String tpedFile, String tfamFile, String covarFile, String outdir, Map<String, String> plinkParams) {
            this.tpedFile = tpedFile;
            this.tfamFile = tfamFile;
            this.covarFile = covarFile;
            this.outdir = outdir;
            this.plinkParams = plinkParams;
        }

        public String tpedFile;  // Transpose PED file (.tped) containing SNP and genotype information
        public String tfamFile;  // Transpose FAM file (.tfam) containing individual and family information
        public String covarFile; // Covariate file
        public String outdir;
        public Map<String, String> plinkParams;
    }

    @POST
    @Path("/plink/run")
    @ApiOperation(value = PlinkWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response plinkRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            PlinkRunParams params) {
        return submitJob(study, "variant", PlinkWrapperAnalysis.ID + "-run", params, jobName, jobDescription, jobTags);
    }

    public static class RvtestsRunParams extends RestBodyParams {
        public RvtestsRunParams() {
        }
        public RvtestsRunParams(String command, String vcfFile, String phenoFile, String pedigreeFile, String kinshipFile, String covarFile,
                                String outdir, Map<String, String> rvtestsParams) {
            this.command = command;
            this.vcfFile = vcfFile;
            this.phenoFile = phenoFile;
            this.pedigreeFile = pedigreeFile;
            this.kinshipFile = kinshipFile;
            this.covarFile = covarFile;
            this.outdir = outdir;
            this.rvtestsParams = rvtestsParams;
        }

        public String command;      // Valid values: rvtests or vcf2kinship
        public String vcfFile;      // VCF file
        public String phenoFile;    // Phenotype file
        public String pedigreeFile; // Pedigree file
        public String kinshipFile;  // Kinship file
        public String covarFile;    // Covariate file
        public String outdir;
        public Map<String, String> rvtestsParams;
    }

    @POST
    @Path("/rvtests/run")
    @ApiOperation(value = RvtestsWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response rvtestsRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_NAME_DESCRIPTION) @QueryParam(ParamConstants.JOB_NAME) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            RvtestsRunParams params) {
        return submitJob(study, "variant", RvtestsWrapperAnalysis.ID + "-run", params, jobName, jobDescription, jobTags);
    }

    @POST
    @Path("/hw/run")
    @ApiOperation(value = PENDING, response = Job.class)
    public Response hwRun() {
        return createPendingResponse();
    }

    @POST
    @Path("/validate")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Validate a VCF file" + PENDING, response = QueryResponse.class)
    public Response validate(
            @ApiParam(value = "Study [[user@]project:]study where study and project are the id") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "VCF file id, name or path", required = true) @QueryParam("file") String file) {
        return createPendingResponse();
//        try {
//            Map<String, String> params = new HashMap<>();
//            params.put("input", file);
//
//            OpenCGAResult<Job> result = catalogManager.getJobManager().submitJob(studyStr, "variant", "validate", Enums.Priority.HIGH,
//                    params, token);
//            return createOkResponse(result);
//        } catch(Exception e) {
//            return createErrorResponse(e);
//        }
    }

    // FIXME This method must be deleted once deprecated params are not supported any more
    static Query getVariantQuery(QueryOptions queryOptions) {
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

