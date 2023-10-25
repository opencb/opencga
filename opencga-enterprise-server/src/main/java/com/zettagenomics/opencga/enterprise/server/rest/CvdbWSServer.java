package com.zettagenomics.opencga.enterprise.server.rest;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import com.zettagenomics.opencga.enterprise.cvdb.CvdbSolrEngine;
import com.zettagenomics.opencga.enterprise.cvdb.dummy.DummyVariantStorageMetadataDBAdaptorFactory;
import com.zettagenomics.opencga.enterprise.cvdb.models.CvdbIndexResult;
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
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.*;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.zettagenomics.opencga.enterprise.core.api.ParamConstants.*;
import static com.zettagenomics.opencga.enterprise.cvdb.parsers.ClinicalQueryParam.*;
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
        cvdbEngine = new CvdbSolrEngine(enterpriseConfiguration.getCvdb(), new VariantStorageMetadataManager(
                new DummyVariantStorageMetadataDBAdaptorFactory()));
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
            @ApiParam(value = CA_ID_DESCR) @QueryParam(CA_ID_NAME) String caseIdStr,
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
            // @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="report" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="description" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string", paramType = "query"),
            // <field name="analystDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            // <field name="methodDependencies" type="string" indexed="true" stored="true" multiValued="true"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            // <field name="statusDescription" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="statusDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="creationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="modificationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="version" type="int" indexed="true" stored="true" multiValued="false"/>

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),

            // Clinical variant evidence filters

            // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="consequenceTypeIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_CONSEQUENCE_TYPE_ID_NAME, value = CVE_CONSEQUENCE_TYPE_ID_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="xrefIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="panelId" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="mois" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),

            // <field name="penetrance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),

            // <field name="acmgs" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),

            // <field name="tier" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),

            // <field name="clinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="drugResponse" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),

            // <field name="traitAssociation" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="functionalEffect" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="tumorigenesis" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),

            // <field name="otherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="rolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ROL_IN_CANCER_NAME, value = CVE_ROL_IN_CANCER_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
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
            // @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="report" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="description" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string", paramType = "query"),
            // <field name="analystDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            // <field name="methodDependencies" type="string" indexed="true" stored="true" multiValued="true"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            // <field name="statusDescription" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="statusDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="creationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="modificationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="version" type="int" indexed="true" stored="true" multiValued="false"/>

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),

            // Clinical variant evidence filters

            // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="consequenceTypeIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_CONSEQUENCE_TYPE_ID_NAME, value = CVE_CONSEQUENCE_TYPE_ID_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="xrefIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="panelId" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="mois" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),

            // <field name="penetrance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),

            // <field name="acmgs" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),

            // <field name="tier" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),

            // <field name="clinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="drugResponse" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),

            // <field name="traitAssociation" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="functionalEffect" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="tumorigenesis" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),

            // <field name="otherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="rolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ROL_IN_CANCER_NAME, value = CVE_ROL_IN_CANCER_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
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
            // @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="report" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="description" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string", paramType = "query"),
            // <field name="analystDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            // <field name="methodDependencies" type="string" indexed="true" stored="true" multiValued="true"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            // <field name="statusDescription" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="statusDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="creationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="modificationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="version" type="int" indexed="true" stored="true" multiValued="false"/>

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),

            // Clinical variant evidence filters

            // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="consequenceTypeIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_CONSEQUENCE_TYPE_ID_NAME, value = CVE_CONSEQUENCE_TYPE_ID_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="xrefIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="panelId" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="mois" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),

            // <field name="penetrance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),

            // <field name="acmgs" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),

            // <field name="tier" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),

            // <field name="clinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="drugResponse" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),

            // <field name="traitAssociation" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="functionalEffect" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="tumorigenesis" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),

            // <field name="otherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="rolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ROL_IN_CANCER_NAME, value = CVE_ROL_IN_CANCER_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
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
            // @ApiImplicitParam(name = QueryOptions.EXCLUDE, value = ParamConstants.EXCLUDE_DESCRIPTION, example = "id,status", dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = QueryOptions.LIMIT, value = ParamConstants.LIMIT_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SKIP, value = ParamConstants.SKIP_DESCRIPTION, dataType = "integer", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.COUNT, value = ParamConstants.COUNT_DESCRIPTION, dataType = "boolean", paramType = "query"),
            // @ApiImplicitParam(name = QueryOptions.SORT, value = "Sort the results", dataType = "boolean", paramType = "query"),

            // Clinical analysis filters

            @ApiImplicitParam(name = CA_ID_NAME, value = CA_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="description" type="text_en" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_TYPE_NAME, value = CA_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_DISORDER_ID_NAME, value = CA_DISORDER_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FILENAME_NAME, value = CA_FILENAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_PROBAND_ID_NAME, value = CA_PROBAND_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_ID_NAME, value = CA_FAMILY_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_PHENOTYPE_NAME_NAME, value = CA_FAMILY_PHENOTYPE_NAME_DESCR, dataType = "string",
                    paramType = "query"),
            @ApiImplicitParam(name = CA_FAMILY_MEMBER_ID_NAME, value = CA_FAMILY_MEMBER_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="report" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CA_STATUS_NAME, value = CA_STATUS_DESCR, dataType = "string", paramType = "query"),
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>

            // Clinical interpretation filters

            @ApiImplicitParam(name = CI_ID_NAME, value = CI_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="description" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_PANEL_ID_NAME, value = CI_PANEL_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ID_NAME, value = CI_ANALYIST_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_NAME_NAME, value = CI_ANALYIST_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_EMAIL_NAME, value = CI_ANALYIST_EMAIL_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_ANALYIST_ASSIGNED_BY_NAME, value = CI_ANALYIST_ASSIGNED_BY_DESCR, dataType = "string", paramType = "query"),
            // <field name="analystDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_METHOD_NAME_NAME, value = CI_METHOD_NAME_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_VERSION_NAME, value = CI_METHOD_VERSION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_METHOD_COMMIT_NAME, value = CI_METHOD_COMMIT_DESCR, dataType = "string", paramType = "query"),
            // <field name="methodDependencies" type="string" indexed="true" stored="true" multiValued="true"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <field name="locked" type="boolean" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CI_STATUS_ID_NAME, value = CI_STATUS_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CI_STATUS_NAME_NAME, value = CI_STATUS_NAME_DESCR, dataType = "string", paramType = "query"),
            // <field name="statusDescription" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="statusDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="creationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="modificationDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="version" type="int" indexed="true" stored="true" multiValued="false"/>

            // Clinical variant filters

            @ApiImplicitParam(name = CV_ID_NAME, value = CV_ID_DESCR, dataType = "string", paramType = "query"),
            // <field name="primary" type="boolean" indexed="true" stored="true" multiValued="false"/>
            // <field name="comments" type="text_en" indexed="true" stored="true" multiValued="true"/>
            // <dynamicField name="annotations_*" type="string" indexed="false" stored="true" multiValued="false"/>
            // <dynamicField name="annotationScores_*" type="float" indexed="false" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_DISCUSSION_AUTHOR_NAME, value = CV_DISCUSSION_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="discussionDate" type="string" indexed="true" stored="true" multiValued="false"/>
            // <field name="discussionText" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_CONFIDENCE_VALUE_NAME, value = CV_CONFIDENCE_VALUE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_CONFIDENCE_AUTHOR_NAME, value = CV_CONFIDENCE_AUTHOR_DESCR, dataType = "string", paramType = "query"),
            // <field name="confidenceDate" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CV_TAG_NAME, value = CV_TAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATUS_NAME, value = CV_STATUS_DESCR, dataType = "string", paramType = "query"),

            @ApiImplicitParam(name = CV_REGION_NAME, value = CV_REGION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_BIOTYPE_NAME, value = CV_ANNOT_BIOTYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSEQUENCE_TYPE_NAME, value = CV_ANNOT_CONSEQUENCE_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRANSCRIPT_FLAG_NAME, value = CV_ANNOT_TRANSCRIPT_FLAG_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_GENE_NAME, value = CV_GENE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_XREF_NAME, value = CV_ANNOT_XREF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_NAME, value = CV_ANNOT_GENE_ROLE_IN_CANER_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_TYPE_NAME, value = CV_TYPE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_SUBSTITUTION_NAME, value = CV_ANNOT_PROTEIN_SUBSTITUTION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_CONSERVATION_NAME, value = CV_ANNOT_CONSERVATION_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_FUNCTIONAL_SCORE_NAME, value = CV_ANNOT_FUNCTIONAL_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_ALTERNATE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_NAME, value = CV_ANNOT_POPULATION_REFERENCE_FREQUENCY_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_ALT_NAME, value = CV_STATS_ALT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_MAF_NAME, value = CV_STATS_MAF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_REF_NAME, value = CV_STATS_REF_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_STATS_PASS_FREQ_NAME, value = CV_STATS_PASS_FREQ_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_SCORE_NAME, value = CV_SCORE_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GO_GENES_NAME, value = CV_ANNOT_GO_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_EXPRESSION_GENES_NAME, value = CV_ANNOT_EXPRESSION_GENES_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_GENE_TRAIT_ID_NAME, value = CV_ANNOT_GENE_TRAIT_ID_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_TRAIT_NAME, value = CV_ANNOT_TRAIT_DESCR, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = CV_ANNOT_PROTEIN_KEYWORD_NAME, value = CV_ANNOT_PROTEIN_KEYWORD_DESCR, dataType = "string", paramType = "query"),

            // Clinical variant evidence filters

            // <field name="phenotypeNames" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_PHENOTYPE_NAME_NAME, value = CVE_PHENOTYPE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="geneName" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_GENE_NAME_NAME, value = CVE_GENE_NAME_DESCR, dataType = "string", paramType = "query"),

            // <field name="consequenceTypeIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_CONSEQUENCE_TYPE_ID_NAME, value = CVE_CONSEQUENCE_TYPE_ID_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="xrefIds" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_XREF_ID_NAME, value = CVE_XREF_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="panelId" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PANEL_ID_NAME, value = CVE_PANEL_ID_DESCR, dataType = "string", paramType = "query"),

            // <field name="mois" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_MOI_NAME, value = CVE_MOI_DESCR, dataType = "string", paramType = "query"),

            // <field name="penetrance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_PENETRANCE_NAME, value = CVE_PENETRANCE_DESCR, dataType = "string", paramType = "query"),

            // <field name="acmgs" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ACGM_NAME, value = CVE_ACGM_DESCR, dataType = "string", paramType = "query"),

            // <field name="tier" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TIER_NAME, value = CVE_TIER_DESCR, dataType = "string", paramType = "query"),

            // <field name="clinicalSignificance" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_CLINICAL_SIGNIFICANCE_NAME, value = CVE_CLINICAL_SIGNIFICANCE_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="drugResponse" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_DRUG_RESPONSE_NAME, value = CVE_DRUG_RESPONSE_DESCR, dataType = "string", paramType = "query"),

            // <field name="traitAssociation" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TRAIT_ASSOCIATION_NAME, value = CVE_TRAIT_ASSOCIATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="functionalEffect" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_FUNCTIONAL_EFFECT_NAME, value = CVE_FUNCTIONAL_EFFECT_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="tumorigenesis" type="string" indexed="true" stored="true" multiValued="false"/>
            @ApiImplicitParam(name = CVE_TUMORIGENESIS_NAME, value = CVE_TUMORIGENESIS_DESCR, dataType = "string", paramType = "query"),

            // <field name="otherClassifications" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_OTHER_CLASSIFICATION_NAME, value = CVE_OTHER_CLASSIFICATION_DESCR, dataType = "string",
                    paramType = "query"),

            // <field name="rolesInCancer" type="string" indexed="true" stored="true" multiValued="true"/>
            @ApiImplicitParam(name = CVE_ROL_IN_CANCER_NAME, value = CVE_ROL_IN_CANCER_DESCR, dataType = "string", paramType = "query")

            // <dynamicField name="score_*" type="double" indexed="true" stored="true" multiValued="false"/>
    })
    public Response searchClinicalVariantEvidences() {
        return run(() -> {
            // Get all query options
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);

            return cvdbEngine.searchClinicalVariantEvidences(query, queryOptions, token);
        });
    }
}
