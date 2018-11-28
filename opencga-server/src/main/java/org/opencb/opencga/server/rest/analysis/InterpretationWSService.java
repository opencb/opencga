package org.opencb.opencga.server.rest.analysis;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.swagger.annotations.*;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.InterpretationManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.server.rest.ClinicalAnalysisWSServer;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;
import static org.opencb.opencga.storage.core.clinical.ReportedVariantQueryParam.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

@Path("/{apiVersion}/analysis/interpretation")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Clinical Interpretation", position = 4, description = "Methods for working with Clinical Interpretations")
public class InterpretationWSService extends AnalysisWSService {

    private InterpretationManager catalogInterpretationManager;
    private ClinicalInterpretationManager clinicalInterpretationManager;

    public InterpretationWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                   @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);

        clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager, storageEngineFactory);
        catalogInterpretationManager = catalogManager.getInterpretationManager();
    }

    public InterpretationWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                   @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);

        clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager, storageEngineFactory);
        catalogInterpretationManager = catalogManager.getInterpretationManager();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Create a new clinical interpretation", position = 1,
            response = org.opencb.biodata.models.clinical.interpretation.Interpretation.class)
    public Response create(
            @ApiParam(value = "[[user@]project:]study id") @QueryParam("study") String studyStr,
            @ApiParam(value = "Clinical analysis the interpretation belongs to") @QueryParam("clinicalAnalysis") String clinicalAnalysis,
            @ApiParam(name = "params", value = "JSON containing clinical interpretation information", required = true)
                    ClinicalInterpretationParameters params) {
        try {
            return createOkResponse(catalogInterpretationManager.create(studyStr, params.toClinicalInterpretation(), queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/{interpretation}/update")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Update clinical interpretation information", position = 1,
            response = org.opencb.biodata.models.clinical.interpretation.Interpretation.class)
    public Response update(
            @ApiParam(value = "[[user@]project:]study id") @QueryParam("study") String studyStr,
            @ApiParam(value = "Interpretation id") @PathParam("interpretation") String interpretationId,
            @ApiParam(value = "Create a new version of clinical interpretation", defaultValue = "false")
                @QueryParam(Constants.INCREMENT_VERSION) boolean incVersion,
            @ApiParam(name = "params", value = "JSON containing clinical interpretation information", required = true)
                    ClinicalInterpretationParameters params) {
        try {
            return createOkResponse(catalogInterpretationManager.update(studyStr, interpretationId, params.toInterpretationObjectMap(),
                    queryOptions, sessionId));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/index")
    @ApiOperation(value = "Index clinical analysis interpretations in the clinical variant database", position = 14, response = QueryResponse.class)
    public Response index(@ApiParam(value = "Comma separated list of interpretation IDs to be indexed in the clinical variant database") @QueryParam(value = "interpretationId") String interpretationId,
                          @ApiParam(value = "Comma separated list of clinical analysis IDs to be indexed in the clinical variant database") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
                          @ApiParam(value = "Reset the clinical variant database and import the specified interpretations") @QueryParam("false") boolean reset,
                          @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String study) {
        try {
            clinicalInterpretationManager.index(study, sessionId);
            return Response.ok().build();
        } catch (IOException | ClinicalVariantException | CatalogException e) {
            return createErrorResponse(e);
        }
    }

    @GET
    @Path("/query")
    @ApiOperation(value = "Query for reported variants", position = 14, response = QueryResponse.class)
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

            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "maf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "mgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),

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
            @ApiImplicitParam(name = "transcriptionFlag", value = ANNOT_TRANSCRIPTION_FLAG_DESCR, dataType = "string", paramType = "query"),
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

            // Clinical analysis
            @ApiImplicitParam(name = "clinicalAnalysisId", value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalAnalysisName", value = CA_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalAnalysisDescr", value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalAnalysisFiles", value = CA_FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalAnalysisProbandId", value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalAnalysisFamilyId", value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalAnalysisFamPhenotypeNames", value = CA_FAMILY_PHENOTYPE_NAMES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalAnalysisFamMemberIds", value = CA_FAMILY_MEMBER_IDS_DESCR, dataType = "string", paramType = "query"),

            // Interpretation
            @ApiImplicitParam(name = "interpretationId", value = INT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationSoftwareName", value = INT_SOFTWARE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationSoftwareVersion", value = INT_SOFTWARE_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationAnalystName", value = INT_ANALYST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationPanelNames", value = INT_PANEL_NAMES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationDescription", value = INT_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationDependencies", value = INT_DEPENDENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationFilters", value = INT_FILTERS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationComments", value = INT_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "interpretationCreationDate", value = INT_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),

            // Reported variant
            @ApiImplicitParam(name = "reportedVariantDeNovoQualityScore", value = RV_DE_NOVO_QUALITY_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedVariantComments", value = RV_COMMENTS_DESCR, dataType = "string", paramType = "query"),

            // Reported event
            @ApiImplicitParam(name = "reportedEventPhenotypeNames", value = RE_PHENOTYPE_NAMES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventConsequenceTypeIds", value = RE_CONSEQUENCE_TYPE_IDS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventXrefs", value = RE_XREFS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventPanelIds", value = RE_PANEL_IDS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventAcmg", value = RE_ACMG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventClinicalSignificance", value = RE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventDrugResponse", value = RE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventTraitAssociation", value = RE_TRAIT_ASSOCIATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventFunctionalEffect", value = RE_FUNCTIONAL_EFFECT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventTumorigenesis", value = RE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventOtherClassification", value = RE_OTHER_CLASSIFICATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "reportedEventRolesInCancer", value = RE_ROLES_IN_CANCER_DESCR, dataType = "string", paramType = "query")
    })
    public Response query(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String study) {
        return Response.ok().build();
    }

    @GET
    @Path("/stats")
    @ApiOperation(value = "Clinical interpretation analysis", position = 14, response = QueryResponse.class)
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

            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "maf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "mgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),

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
            @ApiImplicitParam(name = "transcriptionFlag", value = ANNOT_TRANSCRIPTION_FLAG_DESCR, dataType = "string", paramType = "query"),
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

            // Facet fields
            @ApiImplicitParam(name = "field", value = "Facet field for categorical fields", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "fieldRange", value = "Facet field range for continuous fields", dataType = "string", paramType = "query")
    })
    public Response stats(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
                          @ApiParam(value = "Clinical Analysis Id") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
                          @ApiParam(value = "Disease (HPO term)") @QueryParam("disease") String disease,
                          @ApiParam(value = "Family ID") @QueryParam("familyId") String familyId,
                          @ApiParam(value = "Comma separated list of subject IDs") @QueryParam("subjectIds") List<String> subjectIds,
                          @ApiParam(value = "Clinical analysis type, e.g. DUO, TRIO, ...") @QueryParam("type") String type,
                          @ApiParam(value = "Panel ID") @QueryParam("panelId") String panelId,
                          @ApiParam(value = "Panel version") @QueryParam("panelVersion") String panelVersion,
                          @ApiParam(value = "Save interpretation in Catalog") @QueryParam("save") Boolean save,
                          @ApiParam(value = "ID of the stored interpretation") @QueryParam("interpretationId") String interpretationId,
                          @ApiParam(value = "Name of the stored interpretation") @QueryParam("interpretationName") String interpretationName) {
        return Response.ok().build();
    }



    @GET
    @Path("/tools/team")
    @ApiOperation(value = "TEAM interpretation analysis (PENDING)", position = 14, response = QueryResponse.class)
    public Response team(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
                         @ApiParam(value = "Clinical Analysis ID") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
                         @ApiParam(value = "Disease (HPO term)") @QueryParam("disease") String disease,
                         @ApiParam(value = "Family ID") @QueryParam("familyId") String familyId,
                         @ApiParam(value = "Proband ID, if family exist this must be a family member") @QueryParam("probandId") String probandId,
//                         @ApiParam(value = "Clinical analysis type, e.g. DUO, TRIO, ...") @QueryParam("type") String type,
                         @ApiParam(value = "Panel ID") @QueryParam("panelId") String panelId,
                         @ApiParam(value = "Panel version") @QueryParam("panelVersion") String panelVersion,
                         @ApiParam(value = "Save interpretation in Catalog") @QueryParam("save") boolean save,
                         @ApiParam(value = "ID of the stored interpretation") @QueryParam("interpretationId") String interpretationId,
                         @ApiParam(value = "Description of the stored interpretation") @QueryParam("description") String description) {

        return Response.ok().build();
    }

    @GET
    @Path("/tools/tiering")
    @ApiOperation(value = "GEL Tiering interpretation analysis (PENDING)", position = 14, response = QueryResponse.class)
    public Response tiering(@ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias") @QueryParam("study") String studyStr,
                            @ApiParam(value = "Clinical Analysis ID") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
                            @ApiParam(value = "Disease (HPO term)") @QueryParam("disease") String disease,
                            @ApiParam(value = "Family ID") @QueryParam("familyId") String familyId,
                            @ApiParam(value = "Proband ID, if family exist this must be a family member") @QueryParam("probandId") String probandId,
//                         @ApiParam(value = "Clinical analysis type, e.g. DUO, TRIO, ...") @QueryParam("type") String type,
                            @ApiParam(value = "Panel ID") @QueryParam("panelId") String panelId,
                            @ApiParam(value = "Panel version") @QueryParam("panelVersion") String panelVersion,
                            @ApiParam(value = "Save interpretation in Catalog") @QueryParam("save") boolean save,
                            @ApiParam(value = "ID of the stored interpretation") @QueryParam("interpretationId") String interpretationId,
                            @ApiParam(value = "Description of the stored interpretation") @QueryParam("description") String description) {

        return Response.ok().build();
    }

    private static class ClinicalInterpretationParameters {
        public String id;
        public String description;
        public String clinicalAnalysisId;
        public List<DiseasePanel> panels;
        public Software software;
        public Analyst analyst;
        public List<Software> dependencies;
        public Map<String, Object> filters;
        public String creationDate;
        public List<ReportedVariant> reportedVariants;
        public List<ReportedLowCoverage> reportedLowCoverages;
        public List<Comment> comments;
        public Map<String, Object> attributes;

        public Interpretation toClinicalInterpretation() {
            return new Interpretation(id, description, clinicalAnalysisId, panels, software, analyst, dependencies, filters, creationDate,
                    reportedVariants, reportedLowCoverages, comments, attributes);
        }

        public ObjectMap toInterpretationObjectMap() throws JsonProcessingException {
            return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this.toClinicalInterpretation()));
        }

    }

}
