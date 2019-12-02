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
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import java.io.IOException;

@Path("/{apiVersion}/analysis/xx")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Clinical Interpretation", position = 4, description = "Methods for working with Clinical Analysis")
public class ClinicalAnalysisWSService extends AnalysisWSService {

    private ClinicalInterpretationManager clinicalInterpretationManager;

    public ClinicalAnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);

        clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager, storageEngineFactory);
    }

    public ClinicalAnalysisWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);

        clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager, storageEngineFactory);
    }

//    @GET
//    @Path("/index")
//    @ApiOperation(value = "Index clinical analysis interpretations in the clinical variant database", position = 14, response = QueryResponse.class)
//    public Response index(@ApiParam(value = "Comma separated list of interpretation IDs to be indexed in the clinical variant database") @QueryParam(value = "interpretationId") String interpretationId,
//                          @ApiParam(value = "Comma separated list of clinical analysis IDs to be indexed in the clinical variant database") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
//                          @ApiParam(value = "Reset the clinical variant database and import the specified interpretations") @QueryParam("false") boolean reset,
//                          @ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM) String study) {
//        try {
//            clinicalInterpretationManager.index(study, sessionId);
//            return Response.ok().build();
//        } catch (IOException | ClinicalVariantException | CatalogException e) {
//            return createErrorResponse(e);
//        }
//    }
//
//    @GET
//    @Path("/query")
//    @ApiOperation(value = "Query for reported variants", position = 14, response = QueryResponse.class)
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = Params.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = Params.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.LIMIT, value = Params.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SKIP, value = Params.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.COUNT, value = Params.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SKIP_COUNT, value = "Do not count total number of results", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = VariantField.SUMMARY, value = "Fast fetch of main variant parameters", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "approximateCount", value = "Get an approximate count, instead of an exact total count. Reduces execution time", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "approximateCountSamplingSize", value = "Sampling size to get the approximate count. "
//                    + "Larger values increase accuracy but also increase execution time", dataType = "integer", paramType = "query"),
//
//            // Variant filters
//            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reference", value = REFERENCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "alternate", value = ALTERNATE_DESCR, dataType = "string", paramType = "query"),
//
//            // Study filters
//            @ApiImplicitParam(name = Params.PROJECT_PARAM, value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = Params.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "qual", value = QUAL_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "info", value = INFO_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "format", value = FORMAT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleMetadata", value = SAMPLE_METADATA_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "unknownGenotype", value = UNKNOWN_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "maf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "mgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "includeStudy", value = INCLUDE_STUDY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeFile", value = INCLUDE_FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeSample", value = INCLUDE_SAMPLE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeFormat", value = INCLUDE_FORMAT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeGenotype", value = INCLUDE_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//
//            // Annotation filters
//            @ApiImplicitParam(name = "annotationExists", value = ANNOT_EXISTS_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "xref", value = ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "biotype", value = ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "proteinSubstitution", value = ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "conservation", value = ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyRef", value = ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyMaf", value = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptionFlag", value = ANNOT_TRANSCRIPTION_FLAG_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneTraitId", value = ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "go", value = ANNOT_GO_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "expression", value = ANNOT_EXPRESSION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "proteinKeyword", value = ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "drug", value = ANNOT_DRUG_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "functionalScore", value = ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "customAnnotation", value = CUSTOM_ANNOTATION_DESCR, dataType = "string", paramType = "query"),
//
//            // WARN: Only available in Solr
//            @ApiImplicitParam(name = "trait", value = ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
//
//            // Clinical analysis
//            @ApiImplicitParam(name = "clinicalAnalysisId", value = CA_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalAnalysisName", value = CA_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalAnalysisDescr", value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalAnalysisFiles", value = CA_FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalAnalysisProbandId", value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalAnalysisFamilyId", value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalAnalysisFamPhenotypeNames", value = CA_FAMILY_PHENOTYPE_NAMES_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalAnalysisFamMemberIds", value = CA_FAMILY_MEMBER_IDS_DESCR, dataType = "string", paramType = "query"),
//
//            // Interpretation
//            @ApiImplicitParam(name = "interpretationId", value = INT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationSoftwareName", value = INT_SOFTWARE_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationSoftwareVersion", value = INT_SOFTWARE_VERSION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationAnalystName", value = INT_ANALYST_NAME_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationPanelNames", value = INT_PANEL_NAMES_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationDescription", value = INT_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationDependencies", value = INT_DEPENDENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationFilters", value = INT_FILTERS_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationComments", value = INT_COMMENTS_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "interpretationCreationDate", value = INT_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
//
//            // Reported variant
//            @ApiImplicitParam(name = "reportedVariantDeNovoQualityScore", value = RV_DE_NOVO_QUALITY_SCORE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedVariantComments", value = RV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
//
//            // Reported event
//            @ApiImplicitParam(name = "reportedEventPhenotypeNames", value = RE_PHENOTYPE_NAMES_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventConsequenceTypeIds", value = RE_CONSEQUENCE_TYPE_IDS_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventXrefs", value = RE_XREFS_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventPanelIds", value = RE_PANEL_IDS_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventAcmg", value = RE_ACMG_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventClinicalSignificance", value = RE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventDrugResponse", value = RE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventTraitAssociation", value = RE_TRAIT_ASSOCIATION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventFunctionalEffect", value = RE_FUNCTIONAL_EFFECT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventTumorigenesis", value = RE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventOtherClassification", value = RE_OTHER_CLASSIFICATION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reportedEventRolesInCancer", value = RE_ROLES_IN_CANCER_DESCR, dataType = "string", paramType = "query")
//    })
//    public Response query(@ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM) String study) {
//        return Response.ok().build();
//    }
//
//    @GET
//    @Path("/stats")
//    @ApiOperation(value = "Clinical interpretation analysis", position = 14, response = QueryResponse.class)
//    @ApiImplicitParams({
//            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = Params.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = Params.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.LIMIT, value = Params.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SKIP, value = Params.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.COUNT, value = Params.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SKIP_COUNT, value = "Do not count total number of results", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = VariantField.SUMMARY, value = "Fast fetch of main variant parameters", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "approximateCount", value = "Get an approximate count, instead of an exact total count. Reduces execution time", dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "approximateCountSamplingSize", value = "Sampling size to get the approximate count. "
//                    + "Larger values increase accuracy but also increase execution time", dataType = "integer", paramType = "query"),
//
//            // Variant filters
//            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "reference", value = REFERENCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "alternate", value = ALTERNATE_DESCR, dataType = "string", paramType = "query"),
//
//            // Study filters
//            @ApiImplicitParam(name = Params.PROJECT_PARAM, value = VariantCatalogQueryUtils.PROJECT_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = Params.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "qual", value = QUAL_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "info", value = INFO_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "genotype", value = GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "format", value = FORMAT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleAnnotation", value = VariantCatalogQueryUtils.SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "sampleMetadata", value = SAMPLE_METADATA_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "unknownGenotype", value = UNKNOWN_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "maf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "mgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),
//
//            @ApiImplicitParam(name = "includeStudy", value = INCLUDE_STUDY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeFile", value = INCLUDE_FILE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeSample", value = INCLUDE_SAMPLE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeFormat", value = INCLUDE_FORMAT_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "includeGenotype", value = INCLUDE_GENOTYPE_DESCR, dataType = "string", paramType = "query"),
//
//            // Annotation filters
//            @ApiImplicitParam(name = "annotationExists", value = ANNOT_EXISTS_DESCR, dataType = "boolean", paramType = "query"),
//            @ApiImplicitParam(name = "gene", value = GENE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "ct", value = ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "xref", value = ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "biotype", value = ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "proteinSubstitution", value = ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "conservation", value = ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyAlt", value = ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyRef", value = ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "populationFrequencyMaf", value = ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "transcriptionFlag", value = ANNOT_TRANSCRIPTION_FLAG_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "geneTraitId", value = ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "go", value = ANNOT_GO_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "expression", value = ANNOT_EXPRESSION_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "proteinKeyword", value = ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "drug", value = ANNOT_DRUG_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "functionalScore", value = ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "customAnnotation", value = CUSTOM_ANNOTATION_DESCR, dataType = "string", paramType = "query"),
//
//            // WARN: Only available in Solr
//            @ApiImplicitParam(name = "trait", value = ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
//
//            // Facet fields
//            @ApiImplicitParam(name = "field", value = "Facet field for categorical fields", dataType = "string", paramType = "query"),
//            @ApiImplicitParam(name = "fieldRange", value = "Facet field range for continuous fields", dataType = "string", paramType = "query")
//    })
//    public Response stats(@ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM) String studyId,
//                          @ApiParam(value = "Clinical Analysis Id") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
//                          @ApiParam(value = "Disease (HPO term)") @QueryParam("disease") String disease,
//                          @ApiParam(value = "Family ID") @QueryParam("familyId") String familyId,
//                          @ApiParam(value = "Comma separated list of subject IDs") @QueryParam("subjectIds") List<String> subjectIds,
//                          @ApiParam(value = "Clinical analysis type, e.g. DUO, TRIO, ...") @QueryParam("type") String type,
//                          @ApiParam(value = "Panel ID") @QueryParam("panelId") String panelId,
//                          @ApiParam(value = "Panel version") @QueryParam("panelVersion") String panelVersion,
//                          @ApiParam(value = "Save interpretation in Catalog") @QueryParam("save") Boolean save,
//                          @ApiParam(value = "ID of the stored interpretation") @QueryParam("interpretationId") String interpretationId,
//                          @ApiParam(value = "Name of the stored interpretation") @QueryParam("interpretationName") String interpretationName) {
//        return Response.ok().build();
//    }
//
//
//
//    @GET
//    @Path("/interpretation/team")
//    @ApiOperation(value = "TEAM interpretation analysis (PENDING)", position = 14, response = QueryResponse.class)
//    public Response team(@ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM) String studyId,
//                         @ApiParam(value = "Clinical Analysis ID") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
//                         @ApiParam(value = "Disease (HPO term)") @QueryParam("disease") String disease,
//                         @ApiParam(value = "Family ID") @QueryParam("familyId") String familyId,
//                         @ApiParam(value = "Proband ID, if family exist this must be a family member") @QueryParam("probandId") String probandId,
////                         @ApiParam(value = "Clinical analysis type, e.g. DUO, TRIO, ...") @QueryParam("type") String type,
//                         @ApiParam(value = "Panel ID") @QueryParam("panelId") String panelId,
//                         @ApiParam(value = "Panel version") @QueryParam("panelVersion") String panelVersion,
//                         @ApiParam(value = "Save interpretation in Catalog") @QueryParam("save") boolean save,
//                         @ApiParam(value = "ID of the stored interpretation") @QueryParam("interpretationId") String interpretationId,
//                         @ApiParam(value = "Description of the stored interpretation") @QueryParam("description") String description) {
//
//        return Response.ok().build();
//    }
//
//    @GET
//    @Path("/interpretation/tiering")
//    @ApiOperation(value = "GEL Tiering interpretation analysis (PENDING)", position = 14, response = QueryResponse.class)
//    public Response tiering(@ApiParam(value = Params.STUDY_DESCRIPTION) @QueryParam(Params.STUDY_PARAM) String studyId,
//                            @ApiParam(value = "Clinical Analysis ID") @QueryParam("clinicalAnalysisId") String clinicalAnalysisId,
//                            @ApiParam(value = "Disease (HPO term)") @QueryParam("disease") String disease,
//                            @ApiParam(value = "Family ID") @QueryParam("familyId") String familyId,
//                            @ApiParam(value = "Proband ID, if family exist this must be a family member") @QueryParam("probandId") String probandId,
////                         @ApiParam(value = "Clinical analysis type, e.g. DUO, TRIO, ...") @QueryParam("type") String type,
//                            @ApiParam(value = "Panel ID") @QueryParam("panelId") String panelId,
//                            @ApiParam(value = "Panel version") @QueryParam("panelVersion") String panelVersion,
//                            @ApiParam(value = "Save interpretation in Catalog") @QueryParam("save") boolean save,
//                            @ApiParam(value = "ID of the stored interpretation") @QueryParam("interpretationId") String interpretationId,
//                            @ApiParam(value = "Description of the stored interpretation") @QueryParam("description") String description) {
//
//        return Response.ok().build();
//    }
}
