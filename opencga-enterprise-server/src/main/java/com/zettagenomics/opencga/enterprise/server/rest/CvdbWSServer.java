package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
import com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalAnalysisQueryParam;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbIndexTask;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalAnalysisQueryParam.*;
import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

@Path("/{apiVersion}/cvdb")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Cvdb", description = "Methods for working with CVDB endpoints")
public class CvdbWSServer extends OpenCGAWSServer {

    protected CvdbSolrEngine cvdbEngine;

    public CvdbWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);

        // Get enterprise configuration to set the CVDB engine
        EnterpriseConfiguration enterpriseConfiguration = EnterpriseConfiguration.load(opencgaHome);
        cvdbEngine = new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), null);
    }

    @GET
    @Path("/{caseId}/info")
    @ApiOperation(value = "Get sample information", response = Sample.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, format = "", example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType =
                    "string", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_PARAM, value =
                    ParamConstants.SAMPLE_INCLUDE_INDIVIDUAL_DESCRIPTION,
                    defaultValue = "false", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = ParamConstants.FLATTEN_ANNOTATIONS, value = "Flatten the annotations?", defaultValue = "false",
                    dataType = "boolean", paramType = "query")
    })
    public Response infoSample(
            @ApiParam(value = ParamConstants.SAMPLES_DESCRIPTION, required = true) @PathParam("caseId") String samplesStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = ParamConstants.SAMPLE_VERSION_DESCRIPTION) @QueryParam(ParamConstants.SAMPLE_VERSION_PARAM) String version,
            @ApiParam(value = ParamConstants.DELETED_DESCRIPTION, defaultValue = "false") @QueryParam(ParamConstants.DELETED_PARAM) boolean deleted) {
        try {
            query.remove(ParamConstants.STUDY_PARAM);
            query.remove("samples");

            return createOkResponse("Pajote!");
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/case/index/run")
    @ApiOperation(value = CvdbIndexTask.DESCRIPTION, response = Job.class)
    public Response indexProjectClinicalAnalyses(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = CvdbIndexTaskParams.DESCRIPTION, required = true) CvdbIndexTaskParams params) {
        try {
            // Execute CVDB index as a job
            return submitJob(CvdbIndexTask.ID, study, params, jobId, jobDescription, dependsOn, jobTags);
        } catch (Exception e) {
            return createErrorResponse(CvdbIndexTask.DESCRIPTION, e.getMessage());
        }
    }

    @GET
    @Path("/case/index")
    @ApiOperation(value = CLINICAL_ANALYSES_INDEX_DESCRIPTION, response = CvdbIndexResult.class)
    public Response indexClinicalAnalsyses(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = CLINICAL_ANALYSIS_PARAM_DESCRIPTION) @QueryParam(CLINICAL_ANALYSIS_PARAM_NAME) String caseIdStr,
            @ApiParam(value = INDEX_OVERWRITE_PARAM_DESCRIPTION) @QueryParam(INDEX_OVERWRITE_PARAM_NAME) boolean overwrite) {
        if (StringUtils.isEmpty(studyStr)) {
            return createErrorResponse("Invalid parameter", "Missing study ID");
        }
        if (StringUtils.isEmpty(caseIdStr)) {
            return createErrorResponse("Invalid parameter", "Missing clinical analysis ID");
        }

        return run(() -> {
            StopWatch stopWatch = StopWatch.createStarted();

            // Get project ID form study
            Query projectQuery = new Query();
            projectQuery.put(ProjectDBAdaptor.QueryParams.STUDY.key(), studyStr);
            OpenCGAResult<Project> projectResult = catalogManager.getProjectManager().search(projectQuery, QueryOptions.empty(), token);
            String projectId = projectResult.first().getId();
            if (!cvdbEngine.existCollections(projectId)) {
                cvdbEngine.createCollections(projectId);
            }

            List<String> clinicalAnalysisIds = Arrays.asList(StringUtils.split(caseIdStr, ','));
            CvdbIndexResult indexResult = cvdbEngine.index(clinicalAnalysisIds, studyStr, catalogManager, overwrite, token);
            int dbTime = (int) stopWatch.getTime(TimeUnit.MILLISECONDS);

            return new DataResult<>(dbTime, null, 1, Collections.singletonList(indexResult), 1);
        });
    }

    @GET
    @Path("/case/query")
    @ApiOperation(value = CLINICAL_ANALYSES_QUERY_DESCRIPTION, response = ClinicalAnalysis.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = VariantField.SUMMARY, value = "Fast fetch of main variant parameters", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "approximateCount", value = "Get an approximate count, instead of an exact total count. Reduces execution time", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "approximateCountSamplingSize", value = "Sampling size to get the approximate count. "
//                    + "Larger values increase accuracy but also increase execution time", dataType = "integer", paramType = "query"),
//
//            @ApiImplicitParam(name = "savedFilter", value = SAVED_FILTER_DESCR, dataType = "string", paramType = "query"),

            // Clinical analysis filters
            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),

            // Variant filters
            @ApiImplicitParam(name = VARIANT_QUERY_PARAM, value = VARIANT_QUERY_DESCRIPTION, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reference", value = REFERENCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "alternate", value = ALTERNATE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
//            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "qual", value = QUAL_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "fileData", value = FILE_DATA_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleData", value = SAMPLE_DATA_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleMetadata", value = SAMPLE_METADATA_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "unknownGenotype", value = UNKNOWN_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleLimit", value = SAMPLE_LIMIT_DESCR, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = "sampleSkip", value = SAMPLE_SKIP_DESCR, dataType = "integer", paramType = "query"),
//
//            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "cohortStatsRef", value = STATS_REF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "cohortStatsAlt", value = STATS_ALT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "cohortStatsMaf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "cohortStatsMgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "cohortStatsPass", value = STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "score", value = SCORE_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "family", value = VariantCatalogQueryUtils.FAMILY_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familyDisorder", value = VariantCatalogQueryUtils.FAMILY_DISORDER_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familySegregation", value = VariantCatalogQueryUtils.FAMILY_SEGREGATION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familyMembers", value = VariantCatalogQueryUtils.FAMILY_MEMBERS_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "familyProband", value = VariantCatalogQueryUtils.FAMILY_PROBAND_DESC, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "includeStudy", value = INCLUDE_STUDY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeFile", value = INCLUDE_FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeSample", value = INCLUDE_SAMPLE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeSampleData", value = INCLUDE_SAMPLE_DATA_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeGenotype", value = INCLUDE_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeSampleId", value = INCLUDE_SAMPLE_ID_DESCR, dataType = "string", paramType = "query"),
//
//            // Annotation filters
//            @ApiImplicitParam(name = "annotationExists", value = ANNOT_EXISTS_DESCR, dataType = "java.lang.Boolean", paramType = "query"),
//            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "xref", value = ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "biotype", value = ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "proteinSubstitution", value = ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "conservation", value = ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyRef", value = ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyMaf", value = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptFlag", value = ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneTraitId", value = ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "go", value = ANNOT_GO_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "expression", value = ANNOT_EXPRESSION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "proteinKeyword", value = ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "drug", value = ANNOT_DRUG_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "functionalScore", value = ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinical", value = ANNOT_CLINICAL_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalConfirmedStatus", value = ANNOT_CLINICAL_CONFIRMED_STATUS_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "customAnnotation", value = CUSTOM_ANNOTATION_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "panel", value = VariantCatalogQueryUtils.PANEL_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "panelModeOfInheritance", value = VariantCatalogQueryUtils.PANEL_MOI_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "panelConfidence", value = VariantCatalogQueryUtils.PANEL_CONFIDENCE_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "panelRoleInCancer", value = VariantCatalogQueryUtils.PANEL_ROLE_IN_CANCER_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "panelFeatureType", value = VariantCatalogQueryUtils.PANEL_FEATURE_TYPE_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "panelIntersection", value = VariantCatalogQueryUtils.PANEL_INTERSECTION_DESC, dataType = "boolean", paramType = "query"),
//
//            // WARN: Only available in Solr
//            @ApiImplicitParam(name = "trait", value = ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
    })
    public Response searchClinicalAnalsyses() {
        String key = CA_ID.key();
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return cvdbEngine.searchClinicalAnalyses(query, queryOptions, token);
        });
    }

    @GET
    @Path("/interpretation/query")
    @ApiOperation(value = CLINICAL_INTERPRETATION_QUERY_DESCRIPTION, response = Interpretation.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            // Variant filters
            @ApiImplicitParam(name = VARIANT_QUERY_PARAM, value = VARIANT_QUERY_DESCRIPTION, dataType = "string", paramType = "query"),
    })
    public Response searchClinicalInterpretations() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return cvdbEngine.searchClinicalInterpretations(query, queryOptions, token);
        });
    }

    @GET
    @Path("/clinicalVariant/query")
    @ApiOperation(value = CLINICAL_VARIANT_QUERY_DESCRIPTION, response = ClinicalVariant.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            // Variant filters
            @ApiImplicitParam(name = VARIANT_QUERY_PARAM, value = VARIANT_QUERY_DESCRIPTION, dataType = "string", paramType = "query"),
    })
    public Response searchClinicalVariants() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return cvdbEngine.searchClinicalVariants(query, queryOptions, token);
        });
    }

    @GET
    @Path("/clinicalVariantEvidence/query")
    @ApiOperation(value = CLINICAL_VARIANT_EVIDENCE_QUERY_DESCRIPTION, response = ClinicalVariantEvidence.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            // Variant filters
            @ApiImplicitParam(name = VARIANT_QUERY_PARAM, value = VARIANT_QUERY_DESCRIPTION, dataType = "string", paramType = "query"),
    })
    public Response searchClinicalVariantEvidences() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, token);
        });
    }
}
