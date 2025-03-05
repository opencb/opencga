package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.CvdbIndexTask;
import com.zettagenomics.opencga.enterprise.cvdb.tasks.params.CvdbIndexTaskParams;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariantEvidence;
import org.opencb.biodata.models.clinical.interpretation.stats.ClinicalVariantSummaryStats;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exceptions.VersionException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.job.JobType;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParser.*;
import static org.opencb.opencga.core.api.ParamConstants.JOB_DEPENDS_ON;

@Path("/{apiVersion}/analysis/cvdb")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "CVDB", position = 4, description = "Methods for working with CVDB (clinical variant database)")
public class EnterpriseCvdbWebService extends EnterpriseOpenCGAWSServer {

    public static final AtomicReference<CvdbSolrEngine> cvdbEngineAtomicRef = new AtomicReference();
    public static final AtomicReference<ClinicalInterpretationManager> clinicalInterpretationManagerAtomicRef = new AtomicReference<>();

    public EnterpriseCvdbWebService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
                                    @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
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
    // I N D E X
    //-------------------------------------------------------------------------

    @POST
    @Path("/index/run")
    @ApiOperation(value = CvdbIndexTask.DESCRIPTION, response = Job.class)
    public Response indexProjectClinicalAnalyses(
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @QueryParam(ParamConstants.STUDY_PARAM) String study,
            @ApiParam(value = ParamConstants.JOB_ID_CREATION_DESCRIPTION) @QueryParam(ParamConstants.JOB_ID) String jobId,
            @ApiParam(value = ParamConstants.JOB_DESCRIPTION_DESCRIPTION) @QueryParam(ParamConstants.JOB_DESCRIPTION) String jobDescription,
            @ApiParam(value = ParamConstants.JOB_DEPENDS_ON_DESCRIPTION) @QueryParam(JOB_DEPENDS_ON) String dependsOn,
            @ApiParam(value = ParamConstants.JOB_TAGS_DESCRIPTION) @QueryParam(ParamConstants.JOB_TAGS) String jobTags,
            @ApiParam(value = ParamConstants.JOB_SCHEDULED_START_TIME_DESCRIPTION) @QueryParam(ParamConstants.JOB_SCHEDULED_START_TIME) String scheduledStartTime,
            @ApiParam(value = ParamConstants.JOB_PRIORITY_DESCRIPTION) @QueryParam(ParamConstants.SUBMIT_JOB_PRIORITY_PARAM) String jobPriority,
            @ApiParam(value = ParamConstants.JOB_DRY_RUN_DESCRIPTION) @QueryParam(ParamConstants.JOB_DRY_RUN) Boolean dryRun,
            @ApiParam(value = CvdbIndexTaskParams.DESCRIPTION, required = true) CvdbIndexTaskParams params) {
        try {
            // Execute CVDB index as a job
            return submitJob(study, JobType.NATIVE, CvdbIndexTask.ID, params, jobId, jobDescription, dependsOn, jobTags, scheduledStartTime, jobPriority, dryRun);

        } catch (Exception e) {
            return createErrorResponse(CvdbIndexTask.DESCRIPTION, e.getMessage());
        }
    }

    //-------------------------------------------------------------------------
    // Q U E R Y
    //-------------------------------------------------------------------------

    @GET
    @Path("/analysis/query")
    @ApiOperation(value = CLINICAL_ANALYSES_QUERY_DESCRIPTION, response = ClinicalAnalysis.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "interpretation,panels",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean",
            // paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response searchClinicalAnalsyses() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return getCvdbEngine().searchClinicalAnalyses(query, queryOptions, token);
        });
    }

    @GET
    @Path("/interpretation/query")
    @ApiOperation(value = CLINICAL_INTERPRETATION_QUERY_DESCRIPTION, response = Interpretation.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "primaryFindings",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean",
            // paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response searchClinicalInterpretations() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return getCvdbEngine().searchClinicalInterpretations(query, queryOptions, token);
        });
    }

    @GET
    @Path("/variant/query")
    @ApiOperation(value = CLINICAL_VARIANT_QUERY_DESCRIPTION, response = ClinicalVariant.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean",
            // paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response searchClinicalVariants() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return getCvdbEngine().searchClinicalVariants(query, queryOptions, token);
        });
    }

    @GET
    @Path("/evidence/query")
    @ApiOperation(value = CLINICAL_VARIANT_EVIDENCE_QUERY_DESCRIPTION, response = ClinicalVariantEvidence.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.INCLUDE, value = ParamConstants.INCLUDE_DESCRIPTION, example = "name,attributes",
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION,
                    example = "genomicFeature,attributes", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer",
                    paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean",
            // paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response searchClinicalVariantEvidences() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return getCvdbEngine().searchClinicalVariantEvidences(query, queryOptions, token);
        });
    }

    //-------------------------------------------------------------------------
    // A G G R E G A T I O N
    //-------------------------------------------------------------------------

    @GET
    @Path("/analysis/aggregate")
    @ApiOperation(value = "Calculate and fetch clinical analysis aggregation stats", response = FacetField.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response clinicalAnalsysAggregationStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: type;disorderId"
            + ". For nested faceted fields use >>, e.g.: type>>disorderId. Accepted values: "
            + CA_FACET_FIELDS) @QueryParam(ParamConstants.FIELD_PARAM) String field) {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put(QueryOptions.FACET, field);

            return getCvdbEngine().facetClinicalAnalyses(query, queryOptions, token);
        });
    }

    @GET
    @Path("/interpretation/aggregate")
    @ApiOperation(value = "Calculate and fetch clinical interpretation aggregation stats", response = FacetField.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response clinicalInterpretationAggregationStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: "
            + "panelIds;methodName. For nested faceted fields use >>, e.g.: panelIds>>methodName. Accepted values: "
            + CI_FACET_FIELDS) @QueryParam(ParamConstants.FIELD_PARAM) String field) {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put(QueryOptions.FACET, field);

            return getCvdbEngine().facetClinicalInterpretations(query, queryOptions, token);
        });
    }

    @GET
    @Path("/variant/aggregate")
    @ApiOperation(value = "Calculate and fetch clinical variant aggregation stats", response = FacetField.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response clinicalVariantAggregationStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: "
            + "type;biotypes. For nested faceted fields use >>, e.g.: type>>biotypes. Accepted values: "
            + CV_FACET_FIELDS) @QueryParam(ParamConstants.FIELD_PARAM) String field) {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put(QueryOptions.FACET, field);

            return getCvdbEngine().facetClinicalVariants(query, queryOptions, token);
        });
    }

    @GET
    @Path("/evidence/aggregate")
    @ApiOperation(value = "Calculate and fetch clinical variant evidence aggregation stats", response = FacetField.class)
    @ApiImplicitParams({
            @ApiImplicitParam(name = PROJECT_PARAM_NAME, value = PROJECT_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = STUDY_PARAM_NAME, value = STUDY_PARAM_DESCRIPTION, dataType = "string", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DESCRIPTION_NAME, value = CA_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_DISORDER_ID_NAME, value = CA_PROBAND_DISORDER_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_PHENOTYPE_NAME_NAME, value = CA_PROBAND_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_REPORT_NAME, value = CA_REPORT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_LOCKED_NAME, value = CA_LOCKED_DESCR, dataType = "boolean", paramType = "query"),

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PRIMARY_NAME, value = CI_PRIMARY_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_DESCRIPTION_NAME, value = CI_DESCRIPTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_DATE_NAME, value = CI_ANALYIST_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_DEPENDENCIES_NAME, value = CI_METHOD_DEPENDENCIES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_COMMENTS_NAME, value = CI_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_LOCKED_NAME, value = CI_LOCKED_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DESCRIPTION_NAME, value = CI_STATUS_DESCRIPTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_DATE_NAME, value = CI_STATUS_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_CREATION_DATE_NAME, value = CI_CREATION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_MODIFICATION_DATE_NAME, value = CI_MODIFICATION_DATE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CI_VERSION_NAME, value = CI_VERSION_DESCR, dataType = "integer", paramType = "query"),

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_VARIANT_ID_NAME, value = CV_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_FINDING_NAME, value = CV_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_PRIMARY_INTERPRETATION_NAME, value = CV_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CV_COMMENTS_NAME, value = CV_COMMENTS_DESCR, dataType = "string", paramType = "query"),
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_DATE_NAME, value = CV_DISCUSSION_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_DISCUSSION_TEXT_NAME, value = CV_DISCUSSION_TEXT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_DATE_NAME, value = CV_CONFIDENCE_DATE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME,
                    value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string",
                    paramType = "query"),

            // Clinical variant evidence filters

            @ApiImplicitParam(name = CVE_VARIANT_ID_NAME, value = CVE_VARIANT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_FINDING_NAME, value = CVE_PRIMARY_FINDING_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PRIMARY_INTERPRETATION_NAME, value = CVE_PRIMARY_INTERPRETATION_DESCR, dataType = "boolean", paramType = "query"),
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRANSCRIPT_ID_NAME, value = CVE_TRANSCRIPT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_SO_TERM_NAME_NAME, value = CVE_SO_TERM_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CVE_ROLE_IN_CANCER_NAME, value = CVE_ROLE_IN_CANCER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_ACGM_NAME, value = CVE_REVIEW_ACGM_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TIER_NAME, value = CVE_REVIEW_TIER_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_CLINICAL_SIGNIFICANCE_NAME, value = CVE_REVIEW_CLINICAL_SIGNIFICANCE_DESCR,
                    dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CVE_REVIEW_TEXT_NAME, value = CVE_REVIEW_TEXT_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response clinicalVariantEvidenceAggregationStats(@ApiParam(value = "List of facet fields separated by semicolons, e.g.: "
            + "geneName;tier. For nested faceted fields use >>, e.g.: geneName>>tier. Accepted values: "
            + CVE_FACET_FIELDS) @QueryParam(ParamConstants.FIELD_PARAM) String field) {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.put(QueryOptions.FACET, field);

            return getCvdbEngine().facetClinicalVariantEvidences(query, queryOptions, token);
        });
    }

    //-------------------------------------------------------------------------
    // G E T    C L I N I C A L     V A R I A N T     S U M M A R Y
    //-------------------------------------------------------------------------

    @GET
    @Path("/variant/{variantId}/stats")
    @ApiOperation(value = CLINICAL_VARIANT_SUMMARY_DESCRIPTION, response = ClinicalVariantSummaryStats.class)
    public Response getClinicalVariantSummaryStats(
            @ApiParam(value = "Variant ID (or comma separated list of variant IDs)") @PathParam(value = "variantId") String variantId,
            @ApiParam(value = PROJECT_PARAM_DESCRIPTION + "(or command separated list of project IDs)") @QueryParam(PROJECT_PARAM_NAME)
                    String projectId) {
        return run(() -> {
            return getCvdbEngine().getClinicalVariantSummaryStats(variantId, projectId, token);
        });
    }
}
