package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.catalog.managers.EnterpriseFactory;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbUtils;
import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.tools.annotations.Api;
import org.opencb.opencga.core.tools.annotations.ApiImplicitParam;
import org.opencb.opencga.core.tools.annotations.ApiImplicitParams;
import org.opencb.opencga.core.tools.annotations.ApiOperation;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CI_STATUS_ID_DESCR;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.CI_STATUS_ID_NAME;
import static org.opencb.opencga.core.api.ParamConstants.INCLUDE_INTERPRETATION;
import static org.opencb.opencga.core.models.variant.VariantQueryParams.*;

@Path("/{apiVersion}/analysis/clinical")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Analysis - Clinical", position = 4, description = "Methods for working with Clinical Interpretations")
public class EnterpriseClinicalWebService extends ClinicalWebService {

    public static final AtomicReference<CvdbSolrEngine> cvdbEngineAtomicRef = new AtomicReference();
    public static final AtomicReference<ClinicalInterpretationManager> clinicalInterpretationManagerAtomicRef = new AtomicReference<>();

    public EnterpriseClinicalWebService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                        @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
        EnterpriseFactory.init(catalogManager, opencgaHome);
    }

    private CvdbSolrEngine getCvdbEngine() {
        CvdbSolrEngine cvdbEngine = cvdbEngineAtomicRef.get();
        if (cvdbEngine == null) {
            synchronized(cvdbEngineAtomicRef) {
                cvdbEngine = cvdbEngineAtomicRef.get();
                if (cvdbEngine == null) {
                    logger.info("Initializing CVDB Solr Engine");
                    EnterpriseConfiguration enterpriseConfiguration = EnterpriseConfiguration.load(opencgaHome);
                    cvdbEngine = new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), catalogManager, new VariantStorageMetadataManager(
                            new DummyVariantStorageMetadataDBAdaptorFactory()));
                    cvdbEngineAtomicRef.set(cvdbEngine);
                }
            }
        }
        return cvdbEngine;
    }

    private ClinicalInterpretationManager getClinicalInterpretationManager() throws IOException {
        ClinicalInterpretationManager clinicalInterpretationManager = clinicalInterpretationManagerAtomicRef.get();
        if (clinicalInterpretationManager == null) {
            synchronized(clinicalInterpretationManagerAtomicRef) {
                clinicalInterpretationManager = clinicalInterpretationManagerAtomicRef.get();
                if (clinicalInterpretationManager == null) {
                    logger.info("Initializing clinical interpretation manager");
                    clinicalInterpretationManager = new ClinicalInterpretationManager(catalogManager, storageEngineFactory, opencgaHome);
                    clinicalInterpretationManagerAtomicRef.set(clinicalInterpretationManager);
                }
            }
        }
        return clinicalInterpretationManager;
    }

    //-------------------------------------------------------------------------
    // C L I N I C A L     V A R I A N T      Q U E R Y
    //-------------------------------------------------------------------------
    @GET
    @Path("/variant/query")
    @ApiOperation(value = "Fetch clinical variants", response = ClinicalVariant.class)
    @ApiImplicitParams({

            // Query options
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCount", value = "Get an approximate count, instead of an exact total count. Reduces execution time", dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "approximateCountSamplingSize", value = "Sampling size to get the approximate count. "
                    + "Larger values increase accuracy but also increase execution time", dataType = "integer", paramType = "query"),

            @ApiImplicitParam(name = "savedFilter", value = SAVED_FILTER_DESCR, dataType = "string", paramType = "query"),

            // Interpretation ID to include fields related to
            @ApiImplicitParam(name = ParamConstants.INCLUDE_INTERPRETATION, value = ParamConstants.INCLUDE_INTERPRETATION_DESCRIPTION, dataType = "string", paramType = "query"),
            // Variant filters
            @ApiImplicitParam(name = "id", value = ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "region", value = REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "type", value = TYPE_DESCR, dataType = "string", paramType = "query"),

            // Study filters
            @ApiImplicitParam(name = ParamConstants.STUDY_PARAM, value = STUDY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "file", value = FILE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "filter", value = FILTER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "qual", value = QUAL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "fileData", value = FILE_DATA_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "sample", value = SAMPLE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleData", value = SAMPLE_DATA_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "sampleAnnotation", value = SAMPLE_ANNOTATION_DESC, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "cohort", value = COHORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsRef", value = STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsAlt", value = STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMaf", value = STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsMgf", value = STATS_MGF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "cohortStatsPass", value = STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingAlleles", value = MISSING_ALLELES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "missingGenotypes", value = MISSING_GENOTYPES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "score", value = SCORE_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "family", value = FAMILY_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyDisorder", value = FAMILY_DISORDER_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familySegregation", value = FAMILY_SEGREGATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyMembers", value = FAMILY_MEMBERS_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "familyProband", value = FAMILY_PROBAND_DESC, dataType = "string", paramType = "query"),

            // Annotation filters
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
            @ApiImplicitParam(name = "clinical", value = ANNOT_CLINICAL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalSignificance", value = ANNOT_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "clinicalConfirmedStatus", value = ANNOT_CLINICAL_CONFIRMED_STATUS_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = "customAnnotation", value = CUSTOM_ANNOTATION_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = "panel", value = PANEL_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelModeOfInheritance", value = PANEL_MOI_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelConfidence", value = PANEL_CONFIDENCE_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelRoleInCancer", value = PANEL_ROLE_IN_CANCER_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelFeatureType", value = PANEL_FEATURE_TYPE_DESC, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "panelIntersection", value = PANEL_INTERSECTION_DESC, dataType = "boolean", paramType = "query"),

            @ApiImplicitParam(name = "trait", value = ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
    })
    public Response variantQuery() {
        // Get all query options
        return run(() -> {
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            Query query = VariantWebService.getVariantQuery(queryOptions);

            // Because of the parameter includeInterpretation is not a standard variant query parameter, it is added to the query
            // after parsing the query parameters
            if (uriInfo.getQueryParameters().containsKey(INCLUDE_INTERPRETATION)) {
                String includeInterpretation = uriInfo.getQueryParameters().get(INCLUDE_INTERPRETATION).get(0);
                logger.info("Adding the includeInterpretation ({}) to the variant query", includeInterpretation);
                query.put(INCLUDE_INTERPRETATION, includeInterpretation);
            }
            if (uriInfo.getQueryParameters().containsKey(CI_STATUS_ID_NAME)) {
                String interpretationStatusId = uriInfo.getQueryParameters().get(CI_STATUS_ID_NAME).get(0);
                if ( StringUtils.isNotEmpty(interpretationStatusId)) {
                    logger.info("Adding the interpretation status ID ({}) to the variant query", interpretationStatusId);
                    query.put(CI_STATUS_ID_NAME, interpretationStatusId);
                }
            }

            return CvdbUtils.getClinicalVariant(query, queryOptions, getClinicalInterpretationManager(), getCvdbEngine(), token);
        });
    }
}
