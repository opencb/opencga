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

import io.swagger.annotations.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.qc.MutationalSignature;
import org.opencb.biodata.models.clinical.qc.SampleQcVariantStats;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.family.qc.FamilyQcAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcAnalysis;
import org.opencb.opencga.analysis.individual.qc.IndividualQcUtils;
import org.opencb.opencga.analysis.sample.qc.SampleQcAnalysis;
import org.opencb.opencga.analysis.variant.VariantExportTool;
import org.opencb.opencga.analysis.variant.circos.CircosAnalysis;
import org.opencb.opencga.analysis.variant.circos.CircosLocalAnalysisExecutor;
import org.opencb.opencga.analysis.variant.gwas.GwasAnalysis;
import org.opencb.opencga.analysis.variant.inferredSex.InferredSexAnalysis;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysis;
import org.opencb.opencga.analysis.variant.knockout.KnockoutAnalysisResultReader;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.analysis.variant.mendelianError.MendelianErrorAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureAnalysis;
import org.opencb.opencga.analysis.variant.mutationalSignature.MutationalSignatureLocalAnalysisExecutor;
import org.opencb.opencga.analysis.variant.operations.VariantFileDeleteOperationTool;
import org.opencb.opencga.analysis.variant.operations.VariantIndexOperationTool;
import org.opencb.opencga.analysis.variant.relatedness.RelatednessAnalysis;
import org.opencb.opencga.analysis.variant.samples.SampleEligibilityAnalysis;
import org.opencb.opencga.analysis.variant.samples.SampleVariantFilterAnalysis;
import org.opencb.opencga.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.opencga.analysis.variant.stats.VariantStatsAnalysis;
import org.opencb.opencga.analysis.wrappers.*;
import org.opencb.opencga.analysis.wrappers.fastqc.FastqcWrapperAnalysis;
import org.opencb.opencga.analysis.wrappers.samtools.SamtoolsWrapperAnalysis;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.alignment.DeeptoolsWrapperParams;
import org.opencb.opencga.core.models.alignment.FastQcWrapperParams;
import org.opencb.opencga.core.models.alignment.SamtoolsWrapperParams;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByGene;
import org.opencb.opencga.core.models.analysis.knockout.KnockoutByIndividual;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.VariantStatsExportParams;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.sample.SampleAlignmentQualityControlMetrics;
import org.opencb.opencga.core.models.variant.*;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.server.WebServiceException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.QueryParam;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.SAVED_FILTER_DESCR;
import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/analysis/variant")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Variant", description = "Methods for working with 'files' endpoint")
public class VariantWebService extends AnalysisWebService {

    private static final String DEPRECATED = " [DEPRECATED] ";
    public static final String PENDING = " [PENDING] ";
    private static final Map<String, org.opencb.commons.datastore.core.QueryParam> DEPRECATED_VARIANT_QUERY_PARAM;

    static {
        Map<String, org.opencb.commons.datastore.core.QueryParam> map = new LinkedHashMap<>();
        // v2.0.0
        map.put("includeFormat", INCLUDE_SAMPLE_DATA);
        map.put("format", SAMPLE_DATA);
        map.put("info", FILE_DATA);

        // v1.3.0
        map.put("ids", ID);
        map.put(ParamConstants.STUDIES_PARAM, STUDY);
        map.put("files", FILE);
        map.put("samples", SAMPLE);
        map.put("samplesMetadata", SAMPLE_METADATA);
        map.put("cohorts", COHORT);

        map.put("returnedStudies", INCLUDE_STUDY);
        map.put("returnedSamples", INCLUDE_SAMPLE);
        map.put("returnedFiles", INCLUDE_FILE);
        map.put("include-format", INCLUDE_SAMPLE_DATA);
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

    public VariantWebService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public VariantWebService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);
    }

    @Deprecated
    @GET
    @Path("/index")
    @ApiOperation(value = DEPRECATED + "Use via POST", response = RestResponse.class, hidden = true)
    public Response index() {
        return createErrorResponse(new UnsupportedOperationException("Deprecated endpoint. Please, use via POST"));
    }

    @POST
    @Path("/index/run")
    @ApiOperation(value = VariantIndexOperationTool.DESCRIPTION, response = Job.class)
    public Response variantFileIndex(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantIndexParams.DESCRIPTION, required = true) VariantIndexParams params) {
        return submitJob(VariantIndexOperationTool.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @DELETE
    @Path("/file/delete")
    @ApiOperation(value = VariantFileDeleteOperationTool.DESCRIPTION, response = Job.class)
    public Response variantFileDelete(
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = "Files to remove") @QueryParam("file") String file,
            @ApiParam(value = "Resume a previously failed indexation") @QueryParam("resume") boolean resume) throws WebServiceException {
        VariantFileDeleteParams params = new VariantFileDeleteParams(getIdList(file), resume);
        return submitJob(VariantFileDeleteOperationTool.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/query")
    @ApiOperation(value = ParamConstants.VARIANTS_QUERY_DESCRIPTION, response = Variant.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = VariantField.SUMMARY, value = "Fast fetch of main variant parameters", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCount", value = "Get an approximate count, instead of an exact total count. Reduces execution time", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCountSamplingSize", value = "Sampling size to get the approximate count. "
                    + "Larger values increase accuracy but also increase execution time", dataType = "integer", paramType = "query"),

            @ApiImplicitParam(name = "savedFilter", value = SAVED_FILTER_DESCR, dataType = "string", paramType = "query"),

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
            @ApiImplicitParam(name = "fileData", value = FILE_DATA_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleData", value = SAMPLE_DATA_DESCR, dataType = "string", paramType = "query"),
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
            @ApiImplicitParam(name = "cohortStatsPass", value = STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
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
            @ApiImplicitParam(name = "includeSampleData", value = INCLUDE_SAMPLE_DATA_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeGenotype", value = INCLUDE_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "includeSampleId", value = INCLUDE_SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),

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
    public Response getVariants() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);

            return variantManager.get(query, queryOptions, token);
        });
    }

    @Deprecated
    @POST
    @Path("/query")
    @ApiOperation(value = DEPRECATED + ParamConstants.VARIANTS_QUERY_DESCRIPTION, response = Variant.class, hidden = true)
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
            } else {
                return variantManager.get(query, queryOptions, token);
            }
        });
    }

    @POST
    @Path("/export/run")
    @ApiOperation(value = VariantExportTool.DESCRIPTION, response = Job.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
    })
    public Response export(
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantExportParams.DESCRIPTION, required = true) VariantExportParams params) {
        return submitJob(VariantExportTool.ID, project, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/annotation/query")
    @ApiOperation(value = "Query variant annotations from any saved versions", response = VariantAnnotation.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query")
    })
    public Response getAnnotation(@ApiParam(value = "Annotation identifier") @DefaultValue(VariantAnnotationManager.CURRENT) @QueryParam("annotationId") String annotationId) {
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
    @ApiOperation(value = "Read variant annotations metadata from any saved versions")
    public Response getAnnotationMetadata(@ApiParam(value = "Annotation identifier") @QueryParam("annotationId") String annotationId,
                                          @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam(ParamConstants.PROJECT_PARAM) String project) {
        return run(() -> variantManager.getAnnotationMetadata(annotationId, project, token));
    }

    @POST
    @Path("/stats/run")
    @ApiOperation(value = VariantStatsAnalysis.DESCRIPTION, response = Job.class)
    public Response statsRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantStatsAnalysisParams.DESCRIPTION, required = true) VariantStatsAnalysisParams params) {
        return submitJob(VariantStatsAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/stats/export/run")
    @ApiOperation(value = "Export calculated variant stats and frequencies", response = Job.class)
    public Response statsExport(
            @ApiParam(value = ParamConstants.PROJECT_DESCRIPTION) @QueryParam(ParamConstants.PROJECT_PARAM) String project,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = VariantStatsExportParams.DESCRIPTION, required = true) VariantStatsExportParams params) {
        return submitJob("variant-stats-export", project, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

//    public static class StatsDeleteParams extends ToolParams {
//        public String study;
//        public List<String> cohorts;
//    }

//    @DELETE
//    @Path("/stats/delete")
//    @ApiOperation(value = PENDING)
//    public Response statsDelete(StatsDeleteParams params) {
//        return createPendingResponse();
//    }

    @Deprecated
    @GET
    @Path("/familyGenotypes")
    @ApiOperation(value = DEPRECATED + "Use family/genotypes", hidden = true, response = Map.class)
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

//    @POST
//    @Path("/family/stats/run")
//    @ApiOperation(value = PENDING, response = Job.class)
//    public Response familyStatsRun(RestBodyParams params) {
//        return createPendingResponse();
//    }
//
//    @GET
//    @Path("/family/stats/info")
//    @ApiOperation(value = PENDING)
//    public Response familyStatsInfo() {
//        return createPendingResponse();
//    }
//
//    @DELETE
//    @Path("/family/stats/delete")
//    @ApiOperation(value = PENDING)
//    public Response familyStatsDelete() {
//        return createPendingResponse();
//    }


    @Deprecated
    @GET
    @Path("/samples")
    @ApiOperation(value = DEPRECATED + "Use /sample/run", hidden = true, response = Sample.class)
    public Response samples(
            @ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "List of samples to check. By default, all samples") @QueryParam("sample") String samples,
            @ApiParam(value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC) @QueryParam("sampleAnnotation") String sampleAnnotation,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr,
            @ApiParam(value = "Samples must be present in ALL variants or in ANY variant.") @QueryParam("all") @DefaultValue("false") boolean all
    ) {
        return createDeprecatedMovedResponse("/sample/run");
    }

    @POST
    @Path("/sample/run")
    @ApiOperation(value = SampleVariantFilterAnalysis.DESCRIPTION, response = Job.class)
    public Response sampleRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = SampleVariantFilterParams.DESCRIPTION, required = true) SampleVariantFilterParams params) {
        return submitJob(SampleVariantFilterAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/sample/eligibility/run")
    @ApiOperation(value = SampleEligibilityAnalysis.DESCRIPTION, response = Job.class)
    public Response sampleEligibility(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = SampleEligibilityAnalysisParams.DESCRIPTION, required = true) SampleEligibilityAnalysisParams params) {
        return submitJob(SampleEligibilityAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @Deprecated
    @GET
    @Path("/{variant}/sampleData")
    @ApiOperation(value = DEPRECATED + " User sample/query", hidden = true, response = Variant.class)
    public Response sampleDataOld(
            @ApiParam(value = "Variant") @PathParam("variant") String variant,
            @ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") @DefaultValue("0/1,1/1") String genotypesStr) {
        return sampleQuery(variant, studyStr, genotypesStr);
    }

    @GET
    @Path("/sample/query")
    @ApiOperation(value = "Get sample data of a given variant", response = Variant.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query")
    })
    public Response sampleQuery(
            @ApiParam(value = "Variant") @QueryParam("variant") String variant,
            @ApiParam(value = "Study where all the samples belong to") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Genotypes that the sample must have to be selected") @QueryParam("genotype") String genotypesStr
    ) {
        return run(() -> {
            queryOptions.putAll(query);
            return variantManager.getSampleData(variant, studyStr, queryOptions, token);
        });
    }

    @GET
    @Path("/sample/aggregationStats")
    @ApiOperation(value = "Calculate and fetch sample aggregation stats", response = FacetField.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "savedFilter", value = SAVED_FILTER_DESCR, dataType = "string", paramType = "query"),

            // Variant filters
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = ParamConstants.PROJECT_PARAM, value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "family", value = VariantCatalogQueryUtils.FAMILY_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyDisorder", value = VariantCatalogQueryUtils.FAMILY_DISORDER_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familySegregation", value = VariantCatalogQueryUtils.FAMILY_SEGREGATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyMembers", value = VariantCatalogQueryUtils.FAMILY_MEMBERS_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyProband", value = VariantCatalogQueryUtils.FAMILY_PROBAND_DESC, dataType = "string", paramType = "query"),

            // Annotation filters
            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "biotype", value = ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
    })
    public Response sampleAggregationStats(@ApiParam(value =
            "List of facet fields separated by semicolons, e.g.: studies;type."
                    + " For nested faceted fields use >>, e.g.: chromosome>>type ."
                    + " Accepted values: chromosome, type, genotype, consequenceType, biotype, clinicalSignificance, dp, qual, filter") @QueryParam(ParamConstants.FIELD_PARAM) String field) {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            if (StringUtils.isEmpty(field)) {
                String fields = queryOptions.getString("fields");
                queryOptions.put(QueryOptions.FACET, fields);
            } else {
                queryOptions.put(QueryOptions.FACET, field);
            }
            Query query = getVariantQuery(queryOptions);
            return variantManager.facet(query, queryOptions, token);
        });
    }

    @POST
    @Path("/sample/stats/run")
    @ApiOperation(value = SampleVariantStatsAnalysis.DESCRIPTION, response = Job.class)
    public Response sampleStatsRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = SampleVariantStatsAnalysisParams.DESCRIPTION, required = true) SampleVariantStatsAnalysisParams params) {
        return submitJob(SampleVariantStatsAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/sample/stats/query")
    @ApiImplicitParams({
            // Variant filters
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "sampleData", value = SAMPLE_DATA_DESCR, dataType = "string", paramType = "query"),

            // Annotation filters
            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "biotype", value = ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
    })
    @ApiOperation(value = "Obtain sample variant stats from a sample.", response = SampleVariantStats.class)
    public Response sampleStatsQuery(@ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                     @ApiParam(value = ParamConstants.SAMPLE_ID_DESCRIPTION, required = true) @QueryParam("sample") String sample) {
        return run(() -> {
            Query query = getVariantQuery();
            return variantManager.getSampleStats(studyStr, sample, query, token);
        });
    }

    @POST
    @Path("/cohort/stats/run")
    @ApiOperation(value = CohortVariantStatsAnalysis.DESCRIPTION, response = Job.class)
    public Response cohortStatsRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = CohortVariantStatsAnalysisParams.DESCRIPTION, required = true) CohortVariantStatsAnalysisParams params) {
        return submitJob(CohortVariantStatsAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/cohort/stats/info")
    @ApiOperation(value = "Read cohort variant stats from list of cohorts.", response = VariantSetStats.class)
    public Response cohortStatsQuery(@ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                     @ApiParam(value = ParamConstants.COHORTS_DESCRIPTION, required = true) @QueryParam("cohort") String cohort) {
        return run(() -> {
            ParamUtils.checkParameter(cohort, "cohort");
            ParamUtils.checkParameter(studyStr, ParamConstants.STUDY_PARAM);
            OpenCGAResult<Cohort> result = catalogManager.getCohortManager()
                    .get(studyStr, getIdList(cohort), new QueryOptions(), token);

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
            statsResult.setFederationNode(result.getFederationNode());
            return statsResult;
        });
    }

    @DELETE
    @Path("/cohort/stats/delete")
    @ApiOperation(value = "Delete cohort variant stats from a cohort.", response = SampleVariantStats.class)
    public Response cohortStatsDelete(@ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
                                      @ApiParam(value = ParamConstants.COHORT_DESCRIPTION) @QueryParam("cohort") String cohort) {
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
            @ApiImplicitParam(name = "savedFilter", value = SAVED_FILTER_DESCR, dataType = "string", paramType = "query"),

            // Variant filters
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = ParamConstants.PROJECT_PARAM, value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),

//            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "samplesMetadata", value = SAMPLE_METADATA_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "unknownGenotype", value = UNKNOWN_GENOTYPE_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsRef", value = STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsAlt", value = STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMaf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsPass", value = STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "score", value = SCORE_DESCR, dataType = "string", paramType = "query"),

//            @ApiImplicitParam(name = "family", value = VariantCatalogQueryUtils.FAMILY_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familyDisorder", value = VariantCatalogQueryUtils.FAMILY_DISORDER_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familySegregation", value = VariantCatalogQueryUtils.FAMILY_SEGREGATION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familyMembers", value = VariantCatalogQueryUtils.FAMILY_MEMBERS_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familyProband", value = VariantCatalogQueryUtils.FAMILY_PROBAND_DESC, dataType = "string", paramType = "query"),

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
    public Response getAggregationStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: studies;type. For nested faceted fields use >>, e.g.: chromosome>>type;percentile(gerp)") @QueryParam(ParamConstants.FIELD_PARAM) String field) {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            if (StringUtils.isEmpty(field)) {
                String fields = queryOptions.getString("fields");
                queryOptions.put(QueryOptions.FACET, fields);
            } else {
                queryOptions.put(QueryOptions.FACET, field);
            }
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

    @POST
    @Path("/gwas/run")
    @ApiOperation(value = GwasAnalysis.DESCRIPTION, response = Job.class)
    public Response gwasRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = GwasAnalysisParams.DESCRIPTION, required = true) GwasAnalysisParams params) {
        return submitJob(GwasAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

//    @POST
//    @Path("/ibs/run")
//    @ApiOperation(value = PENDING, response = Job.class)
//    public Response ibsRun() {
//        return createPendingResponse();
//    }
//
//    @GET
//    @Path("/ibs/query")
//    @ApiOperation(value = PENDING)
//    public Response ibsQuery() {
//        return createPendingResponse();
//    }

    @POST
    @Path("/mutationalSignature/run")
    @ApiOperation(value = MutationalSignatureAnalysis.DESCRIPTION, response = Job.class)
    public Response mutationalSignatureRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = MutationalSignatureAnalysisParams.DESCRIPTION, required = true) MutationalSignatureAnalysisParams params) {
        return submitJob(MutationalSignatureAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/mutationalSignature/query")
    @ApiOperation(value = MutationalSignatureAnalysis.DESCRIPTION + " Use context index.", response = MutationalSignature.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = "study", value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sample", value = "Sample name", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "biotype", value = ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "fileData", value = FILE_DATA_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "qual", value = QUAL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panel", value = VariantCatalogQueryUtils.PANEL_DESC, dataType = "string", paramType = "query")
    })
    public Response mutationalSignatureQuery(
            @ApiParam(value = "Compute the relative proportions of the different mutational signatures demonstrated by the tumour",
                    defaultValue = "false") @QueryParam("fitting") boolean fitting) {
        File outDir = null;
        try {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = getVariantQuery(queryOptions);

            logger.debug("Mutational Signature query = " + query);

            if (!query.containsKey(SAMPLE.key())) {
                return createErrorResponse(new Exception("Missing sample name"));
            }

            // Create temporal directory
            outDir = Paths.get(configuration.getAnalysis().getScratchDir(), "mutational-signature-" + System.nanoTime()).toFile();
            outDir.mkdir();
            if (!outDir.exists()) {
                return createErrorResponse(new Exception("Error creating temporal directory for mutational-signature/query analysis"));
            }

            MutationalSignatureLocalAnalysisExecutor executor = new MutationalSignatureLocalAnalysisExecutor(variantManager);
            executor.setOpenCgaHome(opencgaHome);

            ObjectMap executorParams = new ObjectMap();
            executorParams.put("opencgaHome", opencgaHome);
            executorParams.put("token", token);
            executorParams.put("fitting", fitting);
            executor.setUp(null, executorParams, outDir.toPath());
            executor.setStudy(query.getString(STUDY.key()));
            executor.setSampleName(query.getString(SAMPLE.key()));

            StopWatch watch = StopWatch.createStarted();
            MutationalSignature signatureResult = executor.query(query, queryOptions);
            watch.stop();
            OpenCGAResult<MutationalSignature> result = new OpenCGAResult<>(((int) watch.getTime()), Collections.emptyList(), 1,
                    Collections.singletonList(signatureResult), 1);
            return createOkResponse(result);
        } catch (CatalogException | ToolException | IOException | StorageEngineException e) {
            return createErrorResponse(e);
        } finally {
            if (outDir != null) {
                // Delete temporal directory
                try {
                    if (outDir.exists()) {
                        FileUtils.deleteDirectory(outDir);
                    }
                } catch (IOException e) {
                    logger.warn("Error cleaning scratch directory " + outDir, e);
                }
            }
        }
    }

    @POST
    @Path("/mendelianError/run")
    @ApiOperation(value = MendelianErrorAnalysis.DESCRIPTION, response = Job.class)
    public Response mendelianErrorRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = MendelianErrorAnalysisParams.DESCRIPTION, required = true) MendelianErrorAnalysisParams params) {
        return submitJob(MendelianErrorAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/inferredSex/run")
    @ApiOperation(value = InferredSexAnalysis.DESCRIPTION, response = Job.class)
    public Response inferredSexRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = InferredSexAnalysisParams.DESCRIPTION, required = true) InferredSexAnalysisParams params) {
        return submitJob(InferredSexAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/relatedness/run")
    @ApiOperation(value = RelatednessAnalysis.DESCRIPTION, response = Job.class)
    public Response relatednessRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = RelatednessAnalysisParams.DESCRIPTION, required = true) RelatednessAnalysisParams params) {
        return submitJob(RelatednessAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/family/qc/run")
    @ApiOperation(value = FamilyQcAnalysis.DESCRIPTION, response = Job.class)
    public Response familyQcRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = FamilyQcAnalysisParams.DESCRIPTION, required = true) FamilyQcAnalysisParams params) {
        return submitJob(FamilyQcAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/individual/qc/run")
    @ApiOperation(value = IndividualQcAnalysis.DESCRIPTION, response = Job.class)
    public Response individualQcRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = IndividualQcAnalysisParams.DESCRIPTION, required = true) IndividualQcAnalysisParams params) {

        return run(() -> {
            List<String> dependsOnList = StringUtils.isEmpty(dependsOn) ? new ArrayList<>() : Arrays.asList(dependsOn.split(","));

            Individual individual = IndividualQcUtils.getIndividualById(study, params.getIndividual(), catalogManager, token);

            // Get samples of that individual, but only germline samples
            List<Sample> childGermlineSamples = IndividualQcUtils.getValidGermlineSamplesByIndividualId(study, individual.getId(),
                    catalogManager, token);
            if (CollectionUtils.isEmpty(childGermlineSamples)) {
                throw new ToolException("Germline sample not found for individual '" +  params.getIndividual() + "'");
            }

            Sample sample = null;
            if (StringUtils.isNotEmpty(params.getSample())) {
                for (Sample germlineSample : childGermlineSamples) {
                    if (params.getSample().equals(germlineSample.getId())) {
                        sample = germlineSample;
                        break;
                    }
                }
                if (sample == null) {
                    throw new ToolException("The provided sample '" + params.getSample() + "' not found in the individual '"
                            + params.getIndividual() + "'");
                }
            } else {
                // If multiple germline samples, we take the first one
                sample = childGermlineSamples.get(0);
            }

            org.opencb.opencga.core.models.file.File catalogBamFile;
            catalogBamFile = AnalysisUtils.getBamFileBySampleId(sample.getId(), study, catalogManager.getFileManager(), token);

            if (catalogBamFile != null) {
                // Check if .bw (coverage file) exists
                OpenCGAResult<org.opencb.opencga.core.models.file.File> fileResult;

                Query query = new Query(FileDBAdaptor.QueryParams.ID.key(), catalogBamFile.getId() + ".bw");
                fileResult = catalogManager.getFileManager().search(study, query, QueryOptions.empty(), token);
                Job deeptoolsJob = null;
                if (fileResult.getNumResults() == 0) {
                    // Coverage file does not exit, a job must be submitted to create the .bw file
                    // but first, check if .bai (bam index file) exist

                    query = new Query(FileDBAdaptor.QueryParams.ID.key(), catalogBamFile.getId() + ".bai");
                    fileResult = catalogManager.getFileManager().search(study, query, QueryOptions.empty(), token);

                    Job samtoolsJob = null;
                    if (fileResult.getNumResults() == 0) {
                        // BAM index file does not exit, a job must be submitted to create the .bai file
                        SamtoolsWrapperParams samtoolsParams = new SamtoolsWrapperParams()
                                .setCommand("index")
                                .setInputFile(catalogBamFile.getId())
                                .setSamtoolsParams(new HashMap<>());

                        DataResult<Job> jobResult = submitJobRaw(SamtoolsWrapperAnalysis.ID, null, study, samtoolsParams, null, null, null,
                                null);
                        samtoolsJob = jobResult.first();
                    }

                    // Coverage file does not exit, a job must be submitted to create the .bw file
                    DeeptoolsWrapperParams deeptoolsParams = new DeeptoolsWrapperParams()
                            .setCommand("bamCoverage")
                            .setBamFile(catalogBamFile.getId());

                    Map<String, String> bamCoverageParams = new HashMap<>();
                    bamCoverageParams.put("bs", "1");
                    bamCoverageParams.put("of", "bigwig");
                    bamCoverageParams.put("minMappingQuality", "20");
                    deeptoolsParams.setDeeptoolsParams(bamCoverageParams);

                    DataResult<Job> jobResult = submitJobRaw(DeeptoolsWrapperAnalysis.ID, null, study, deeptoolsParams, null, null,
                            samtoolsJob == null ? null : samtoolsJob.getId(), null);
                    deeptoolsJob = jobResult.first();
                    dependsOnList.add(deeptoolsJob.getId());
                }
            }

            return submitJobRaw(IndividualQcAnalysis.ID, null, study, params, jobName, jobDescription, StringUtils.join(dependsOnList, ","),
                    jobTags);

        });
    }

    @POST
    @Path("/sample/qc/run")
    @ApiOperation(value = SampleQcAnalysis.DESCRIPTION, response = Job.class)
    public Response sampleQcRun(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = SampleQcAnalysisParams.DESCRIPTION, required = true) SampleQcAnalysisParams params) {

        return run(() -> {
            final String OPENCGA_ALL = "ALL";

            List<String> dependsOnList = StringUtils.isEmpty(dependsOn) ? new ArrayList<>() : Arrays.asList(dependsOn.split(","));

            Sample sample;
            org.opencb.opencga.core.models.file.File catalogBamFile;
            sample = IndividualQcUtils.getValidSampleById(study, params.getSample(), catalogManager, token);
            if (sample == null) {
                throw new ToolException("Sample '" + params.getSample() + "' not found.");
            }
            catalogBamFile = AnalysisUtils.getBamFileBySampleId(sample.getId(), study, catalogManager.getFileManager(), token);

            // Check variant stats
            if (OPENCGA_ALL.equals(params.getVariantStatsId())) {
                throw new ToolException("Invalid parameters: " + OPENCGA_ALL + " is a reserved word, you can not use as a"
                        + " variant stats ID");
            }

            if (StringUtils.isEmpty(params.getVariantStatsId()) && params.getVariantStatsQuery() != null
                    && !params.getVariantStatsQuery().toParams().isEmpty()) {
                throw new ToolException("Invalid parameters: if variant stats ID is empty, variant stats query must be"
                        + " empty");
            }
            if (StringUtils.isNotEmpty(params.getVariantStatsId())
                    && (params.getVariantStatsQuery() == null || params.getVariantStatsQuery().toParams().isEmpty())) {
                throw new ToolException("Invalid parameters: if you provide a variant stats ID, variant stats query"
                        + " can not be empty");
            }
            if (StringUtils.isEmpty(params.getVariantStatsId())) {
                params.setVariantStatsId(OPENCGA_ALL);
            }

            boolean runVariantStats = true;
            if (sample.getQualityControl() != null) {
                if (CollectionUtils.isNotEmpty(sample.getQualityControl().getVariantMetrics().getVariantStats())
                        && OPENCGA_ALL.equals(params.getVariantStatsId())) {
                    runVariantStats = false;
                } else {
                    for (SampleQcVariantStats variantStats : sample.getQualityControl().getVariantMetrics().getVariantStats()) {
                        if (variantStats.getId().equals(params.getVariantStatsId())) {
                            throw new ToolException("Invalid parameters: variant stats ID '" + params.getVariantStatsId()
                                    + "' is already used");
                        }
                    }
                }
            }

            // Check mutational signature
            boolean runSignature = true;
            if (!sample.isSomatic()) {
                runSignature = false;
            } else {
                if (OPENCGA_ALL.equals(params.getSignatureId())) {
                    throw new ToolException("Invalid parameters: " + OPENCGA_ALL + " is a reserved word, you can not use as a"
                            + " signature ID");
                }

                if (StringUtils.isEmpty(params.getSignatureId()) && params.getSignatureQuery() != null
                        && !params.getSignatureQuery().toParams().isEmpty()) {
                    throw new ToolException("Invalid parameters: if signature ID is empty, signature query must be null");
                }
                if (StringUtils.isNotEmpty(params.getSignatureId())
                        && (params.getSignatureQuery() == null || params.getSignatureQuery().toParams().isEmpty())) {
                    throw new ToolException("Invalid parameters: if you provide a signature ID, signature query"
                            + " can not be null");
                }

                if (sample.getQualityControl() != null) {
                    if (CollectionUtils.isNotEmpty(sample.getQualityControl().getVariantMetrics().getSignatures())) {
                        runSignature = false;
                    }
                }
            }

            boolean runFastQc = false;
            if (catalogBamFile != null) {
                if (sample.getQualityControl() == null || CollectionUtils.isEmpty(sample.getQualityControl().getAlignmentMetrics())) {
                    runFastQc = true;
                } else {
                    runFastQc = true;
                    for (SampleAlignmentQualityControlMetrics metrics : sample.getQualityControl().getAlignmentMetrics()) {
                        if (catalogBamFile.getId().equals(metrics.getBamFileId()) && metrics.getFastQc() != null) {
                            runFastQc = false;
                            break;
                        }
                    }
                }
            }

            // Run variant stats if necessary
            if (runVariantStats) {
                SampleVariantStatsAnalysisParams sampleVariantStatsParams = new SampleVariantStatsAnalysisParams(
                        Collections.singletonList(params.getSample()),
                        null,
                        params.getOutdir(), true, false, params.getVariantStatsId(), params.getVariantStatsDescription(), null,
                        params.getVariantStatsQuery()
                );

                DataResult<Job> jobResult = submitJobRaw(SampleVariantStatsAnalysis.ID, null, study,
                        sampleVariantStatsParams, null, null, null, null);
                Job sampleStatsJob = jobResult.first();
                dependsOnList.add(sampleStatsJob.getId());
            }

            // Run signature if necessary
            if (runSignature) {
                MutationalSignatureAnalysisParams mutationalSignatureParams =
                        new MutationalSignatureAnalysisParams(params.getSample(), params.getOutdir());
                DataResult<Job> jobResult = submitJobRaw(MutationalSignatureAnalysis.ID, null, study, mutationalSignatureParams,
                        null, null, null, null);
                Job signatureJob = jobResult.first();
                dependsOnList.add(signatureJob.getId());
            }

            // Run FastQC, if bam file exists
            if (runFastQc) {
                Map<String, String> dynamicParams = new HashMap<>();
                dynamicParams.put("extract", "true");
                FastQcWrapperParams fastQcParams = new FastQcWrapperParams(catalogBamFile.getId(), null, dynamicParams);
                logger.info("fastQcParams = " + fastQcParams);

                DataResult<Job> jobResult = submitJobRaw(FastqcWrapperAnalysis.ID, null, study, fastQcParams,
                        null, null, null, null);
                Job fastqcJob = jobResult.first();
                dependsOnList.add(fastqcJob.getId());
            }

            return submitJobRaw(SampleQcAnalysis.ID, null, study, params, jobName, jobDescription, StringUtils.join(dependsOnList, ","), jobTags);
        });
    }

    @POST
    @Path("/plink/run")
    @ApiOperation(value = PlinkWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response plinkRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = PlinkRunParams.DESCRIPTION, required = true) PlinkRunParams params) {
        return submitJob(PlinkWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/rvtests/run")
    @ApiOperation(value = RvtestsWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response rvtestsRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = RvtestsRunParams.DESCRIPTION, required = true) RvtestsRunParams params) {
        return submitJob(RvtestsWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/gatk/run")
    @ApiOperation(value = GatkWrapperAnalysis.DESCRIPTION, response = Job.class)
    public Response gatkRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = GatkRunParams.DESCRIPTION, required = true) GatkRunParams params) {
        return submitJob(GatkWrapperAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @POST
    @Path("/knockout/run")
    @ApiOperation(value = KnockoutAnalysis.DESCRIPTION, response = Job.class)
    public Response knockoutRun(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobName,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = KnockoutAnalysisParams.DESCRIPTION, required = true) KnockoutAnalysisParams params) {
        return submitJob(KnockoutAnalysis.ID, study, params, jobName, jobDescription, dependsOn, jobTags);
    }

    @GET
    @Path("/knockout/gene/query")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
    })
    @ApiOperation(value = "Fetch values from KnockoutAnalysis result, by genes", response = KnockoutByGene.class)
    public Response knockoutByGeneQuery(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_DESCRIPTION) @QueryParam(ParamConstants.JOB_PARAM) String job) {
        return run(() -> new KnockoutAnalysisResultReader(catalogManager)
                .readKnockoutByGeneFromJob(study, job, limit, (int) skip, p -> true, token));
    }

    @GET
    @Path("/knockout/individual/query")
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
    })
    @ApiOperation(value = "Fetch values from KnockoutAnalysis result, by individuals", response = KnockoutByIndividual.class)
    public Response knockoutByIndividualQuery(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_DESCRIPTION) @QueryParam(ParamConstants.JOB_PARAM) String job) {
        return run(() -> new KnockoutAnalysisResultReader(catalogManager)
                .readKnockoutByIndividualFromJob(study, job, limit, (int) skip, p -> true, token));
    }

    @POST
    @Path("/circos/run")
    @ApiOperation(value = CircosAnalysis.DESCRIPTION, response = String.class)
    public Response circos(
            @ApiParam(value = ParamConstants.STUDY_PARAM) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = CircosAnalysisParams.DESCRIPTION, required = true) CircosAnalysisParams params) {
        File outDir = null;
        try {
            if (StringUtils.isEmpty(params.getTitle())) {
                params.setTitle("UNTITLED");
            }

            // Create temporal directory
            outDir = Paths.get(configuration.getAnalysis().getScratchDir(), "circos-" + System.nanoTime()).toFile();
            outDir.mkdir();
            if (!outDir.exists()) {
                return createErrorResponse(new Exception("Error creating temporal directory for Circos analysis"));
            }

            // Create and set up Circos executor
            CircosLocalAnalysisExecutor executor = new CircosLocalAnalysisExecutor(study, params, variantManager);

            ObjectMap executorParams = new ObjectMap();
            executorParams.put("opencgaHome", opencgaHome);
            executorParams.put("token", token);
            executor.setUp(null, executorParams, outDir.toPath());

            // Run Circos executor
            StopWatch watch = StopWatch.createStarted();
            executor.run();

            // Check results by reading the output file
            File imgFile = outDir.toPath().resolve(params.getTitle() + CircosAnalysis.SUFFIX_FILENAME).toFile();
            if (imgFile.exists()) {
                FileInputStream fileInputStreamReader = new FileInputStream(imgFile);
                byte[] bytes = new byte[(int) imgFile.length()];
                fileInputStreamReader.read(bytes);

                String img = new String(Base64.getEncoder().encode(bytes), StandardCharsets.UTF_8);

                watch.stop();
                OpenCGAResult<String> result = new OpenCGAResult<>(((int) watch.getTime()), Collections.emptyList(), 1,
                        Collections.singletonList(img), 1);

                //System.out.println(result.toString());
                return createOkResponse(result);
            } else {
                return createErrorResponse(new Exception("Error plotting Circos graph"));
            }
        } catch (ToolException | IOException e) {
            return createErrorResponse(e);
        } finally {
            if (outDir != null) {
                // Delete temporal directory
                try {
                    if (outDir.exists() && !params.getTitle().startsWith("no.delete.")) {
                        FileUtils.deleteDirectory(outDir);
                    }
                } catch (IOException e) {
                    logger.warn("Error cleaning scratch directory " + outDir, e);
                }
            }
        }
    }

    //    @POST
//    @Path("/hw/run")
//    @ApiOperation(value = PENDING, response = Job.class)
//    public Response hwRun() {
//        return createPendingResponse();
//    }

//    @POST
//    @Path("/validate")
//    @Consumes(MediaType.APPLICATION_JSON)
//    @ApiOperation(value = "Validate a VCF file" + PENDING, response = QueryResponse.class)
//    public Response validate(
//            @ApiParam(value = "Study [[user@]project:]study where study and project are the id") @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
//            @ApiParam(value = "VCF file id, name or path", required = true) @QueryParam("file") String file) {
//        return createPendingResponse();
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
//    }

    protected Query getVariantQuery() {
        QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
        return getVariantQuery(queryOptions);
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

