package org.opencb.opencga.app.cli.main.executors.analysis;

import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
import org.opencb.opencga.core.models.clinical.TeamInterpretationAnalysisParams;
import org.opencb.opencga.core.models.clinical.TieringInterpretationAnalysisParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.RestResponse;

import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTeamCommandOptions.TEAM_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.InterpretationTieringCommandOptions.TIERING_RUN_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.VariantActionableCommandOptions.VARIANT_ACTIONABLE_COMMAND;
import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.VariantQueryCommandOptions.VARIANT_QUERY_COMMAND;

public class ClinicalCommandExecutor extends OpencgaCommandExecutor {

    private ClinicalCommandOptions clinicalCommandOptions;

    public ClinicalCommandExecutor(ClinicalCommandOptions clinicalCommandOptions) {
        super(clinicalCommandOptions.commonCommandOptions);
        this.clinicalCommandOptions = clinicalCommandOptions;
    }

    @Override
    public void execute() throws Exception {
        logger.debug("Executing clinical analysis command line");

        String subCommandString = getParsedSubCommand(clinicalCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
            case "info":
                queryResponse = info();
                break;
            case "search":
                queryResponse = search();
                break;
            case "acl":
                queryResponse = acl();
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;

            case VARIANT_QUERY_COMMAND:
                queryResponse = query();
                break;

            case VARIANT_ACTIONABLE_COMMAND:
                queryResponse = actionable();
                break;

            case TIERING_RUN_COMMAND:
                queryResponse = tiering();
                break;

            case TEAM_RUN_COMMAND:
                queryResponse = team();
                break;

            default:
                logger.error("Subcommand not valid");
                break;
        }

        createOutput(queryResponse);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = clinicalCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getClinicalAnalysisClient().acl(commandOptions.id, params);
    }

    private RestResponse<ClinicalAnalysis> search() throws ClientException {
        logger.debug("Clinical analysis search");

        ClinicalCommandOptions.SearchCommandOptions commandOptions = clinicalCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.TYPE.key(), commandOptions.type);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.INTERNAL_STATUS.key(), commandOptions.status);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.FAMILY.key(), commandOptions.family);
        params.putIfNotNull(ClinicalAnalysisDBAdaptor.QueryParams.PRIORITY.key(), commandOptions.priority);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.CREATION_DATE.key(), commandOptions.creationDate);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE.key(), commandOptions.modificationDate);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.DUE_DATE.key(), commandOptions.dueDate);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.DISORDER.key(), commandOptions.disorder);
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.FLAGS.key(), commandOptions.flags);
        params.putIfNotEmpty("analystAssignee", commandOptions.assignee);
        params.putIfNotNull("proband", commandOptions.proband);
        params.putIfNotEmpty("sample", commandOptions.sample);
        params.putAll(commandOptions.commonOptions.params);

        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);
        params.put(QueryOptions.COUNT, commandOptions.numericOptions.count);

        return openCGAClient.getClinicalAnalysisClient().search(params);
    }

    private RestResponse<ClinicalAnalysis> info() throws ClientException {
        logger.debug("Getting clinical analysis information");

        ClinicalCommandOptions.InfoCommandOptions commandOptions = clinicalCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(ClinicalAnalysisDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        return openCGAClient.getClinicalAnalysisClient().info(commandOptions.clinicalAnalysis, params);
    }

    private RestResponse<ObjectMap> updateAcl() throws ClientException, CatalogException {
        AclCommandOptions.AclsUpdateCommandOptions commandOptions = clinicalCommandOptions.aclsUpdateCommandOptions;

        ObjectMap queryParams = new ObjectMap();
        queryParams.putIfNotNull("study", commandOptions.study);

        ClinicalAnalysisAclUpdateParams updateParams = new ClinicalAnalysisAclUpdateParams()
                .setClinicalAnalysis(extractIdsFromListOrFile(commandOptions.id))
                .setAction(commandOptions.action)
                .setPermissions(commandOptions.permissions);

        return openCGAClient.getClinicalAnalysisClient().updateAcl(commandOptions.memberId, updateParams, queryParams);
    }

    //-------------------------------------------------------------------------
    // Clinical variant
    //-------------------------------------------------------------------------

    private RestResponse<ClinicalVariant> query() throws ClientException {
        ClinicalCommandOptions.VariantQueryCommandOptions commandOptions = clinicalCommandOptions.variantQueryCommandOptions;

        ObjectMap queryParams = new ObjectMap();

        // Query options
        queryParams.putIfNotNull(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        queryParams.putIfNotNull(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.include);
        queryParams.putIfNotNull(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        queryParams.putIfNotNull(QueryOptions.SKIP, commandOptions.numericOptions.skip);
        queryParams.putIfNotNull(QueryOptions.COUNT, commandOptions.numericOptions.count);

        // Variant filters
        queryParams.putIfNotNull("id", commandOptions.basicQueryOptions.id);
        queryParams.putIfNotNull("region", commandOptions.basicQueryOptions.region);
        queryParams.putIfNotNull("type", commandOptions.basicQueryOptions.type);

        // Study filters
        queryParams.putIfNotNull(ParamConstants.STUDY_PARAM, commandOptions.study);
        queryParams.putIfNotNull("file", commandOptions.file);
        queryParams.putIfNotNull("filter", commandOptions.filter);
        queryParams.putIfNotNull("qual", commandOptions.qual);
        queryParams.putIfNotNull("fileData", commandOptions.fileData);

        queryParams.putIfNotNull("sample", commandOptions.samples);
        queryParams.putIfNotNull("sampleData", commandOptions.sampleData);
        queryParams.putIfNotNull("sampleAnnotation", commandOptions.sampleAnnotation);

        queryParams.putIfNotNull("cohort", commandOptions.cohort);
        queryParams.putIfNotNull("cohortStatsRef", commandOptions.basicQueryOptions.rf);
        queryParams.putIfNotNull("cohortStatsAlt", commandOptions.basicQueryOptions.af);
        queryParams.putIfNotNull("cohortStatsMaf", commandOptions.basicQueryOptions.maf);
        queryParams.putIfNotNull("cohortStatsMgf", commandOptions.mgf);
        queryParams.putIfNotNull("cohortStatsPass", commandOptions.cohortStatsPass);
        queryParams.putIfNotNull("missingAlleles", commandOptions.missingAlleleCount);
        queryParams.putIfNotNull("missingGenotypes", commandOptions.missingGenotypeCount);
        queryParams.putIfNotNull("score", commandOptions.score);

        // Annotation filters
        queryParams.putIfNotNull("gene", commandOptions.basicQueryOptions.gene);
        queryParams.putIfNotNull("ct", commandOptions.basicQueryOptions.consequenceType);
        queryParams.putIfNotNull("xref", commandOptions.xref);
        queryParams.putIfNotNull("biotype", commandOptions.geneBiotype);
        queryParams.putIfNotNull("proteinSubstitution", commandOptions.basicQueryOptions.proteinSubstitution);
        queryParams.putIfNotNull("conservation", commandOptions.basicQueryOptions.conservation);
        queryParams.putIfNotNull("populationFrequencyAlt", commandOptions.basicQueryOptions.populationFreqAlt);
        queryParams.putIfNotNull("populationFrequencyRef", commandOptions.populationFreqRef);
        queryParams.putIfNotNull("populationFrequencyMaf", commandOptions.populationFreqMaf);
        queryParams.putIfNotNull("transcriptFlag", commandOptions.flags);
        queryParams.putIfNotNull("geneTraitId", commandOptions.geneTraitId);
        queryParams.putIfNotNull("go", commandOptions.go);
        queryParams.putIfNotNull("expression", commandOptions.expression);
        queryParams.putIfNotNull("proteinKeyword", commandOptions.proteinKeywords);
        queryParams.putIfNotNull("drug", commandOptions.drugs);
        queryParams.putIfNotNull("functionalScore", commandOptions.basicQueryOptions.functionalScore);
        queryParams.putIfNotNull("clinicalSignificance", commandOptions.clinicalSignificance);
        queryParams.putIfNotNull("customAnnotation", commandOptions.annotations);

        queryParams.putIfNotNull("panel", commandOptions.panel);

        queryParams.putIfNotNull("trait", commandOptions.trait);

        return openCGAClient.getClinicalAnalysisClient().queryVariant(queryParams);
    }

    private RestResponse<ClinicalVariant> actionable() throws ClientException {
        ObjectMap queryParams = new ObjectMap();

        queryParams.putIfNotNull(ParamConstants.STUDY_PARAM, clinicalCommandOptions.variantActionableCommandOptions.study);
        queryParams.putIfNotNull(ParamConstants.SAMPLE_PARAM, clinicalCommandOptions.variantActionableCommandOptions.sample);

        return openCGAClient.getClinicalAnalysisClient().actionableVariant(queryParams);
    }

    //-------------------------------------------------------------------------
    // Interpretation
    //-------------------------------------------------------------------------

    private RestResponse<Job> tiering() throws ClientException {
        return openCGAClient.getClinicalAnalysisClient().runInterpretationTiering(
                new TieringInterpretationAnalysisParams(clinicalCommandOptions.tieringCommandOptions.clinicalAnalysis,
                        clinicalCommandOptions.tieringCommandOptions.panels,
                        clinicalCommandOptions.tieringCommandOptions.penetrance,
                        clinicalCommandOptions.tieringCommandOptions.maxLowCoverage,
                        clinicalCommandOptions.tieringCommandOptions.includeLowCoverage),
                getParams(clinicalCommandOptions.tieringCommandOptions.study)
        );
    }

    private RestResponse<Job> team() throws ClientException {
        return openCGAClient.getClinicalAnalysisClient().runInterpretationTeam(
                new TeamInterpretationAnalysisParams(clinicalCommandOptions.teamCommandOptions.clinicalAnalysis,
                        clinicalCommandOptions.teamCommandOptions.panels,
                        clinicalCommandOptions.teamCommandOptions.familySeggregation,
                        clinicalCommandOptions.teamCommandOptions.maxLowCoverage,
                        clinicalCommandOptions.teamCommandOptions.includeLowCoverage),
                getParams(clinicalCommandOptions.teamCommandOptions.study)
        );
    }

    private RestResponse<Job> custom() throws ClientException {
        return null;
    }

    private RestResponse<Job> cancerTiering() throws ClientException {
        return null;
    }

    private ObjectMap getParams(String study) {
        return getParams(null, study);
    }

    private ObjectMap getParams(String project, String study) {
        ObjectMap params = new ObjectMap(clinicalCommandOptions.commonCommandOptions.params);
        params.putIfNotEmpty(ParamConstants.PROJECT_PARAM, project);
        params.putIfNotEmpty(ParamConstants.STUDY_PARAM, study);
        params.putIfNotEmpty(ParamConstants.JOB_ID, clinicalCommandOptions.commonJobOptions.jobId);
        params.putIfNotEmpty(ParamConstants.JOB_DESCRIPTION, clinicalCommandOptions.commonJobOptions.jobDescription);
        if (clinicalCommandOptions.commonJobOptions.jobDependsOn != null) {
            params.put(ParamConstants.JOB_DEPENDS_ON, String.join(",", clinicalCommandOptions.commonJobOptions.jobDependsOn));
        }
        if (clinicalCommandOptions.commonJobOptions.jobTags != null) {
            params.put(ParamConstants.JOB_TAGS, String.join(",", clinicalCommandOptions.commonJobOptions.jobTags));
        }
        if (clinicalCommandOptions.commonNumericOptions.limit > 0) {
            params.put(QueryOptions.LIMIT, clinicalCommandOptions.commonNumericOptions.limit);
        }
        if (clinicalCommandOptions.commonNumericOptions.skip > 0) {
            params.put(QueryOptions.SKIP, clinicalCommandOptions.commonNumericOptions.skip);
        }
        if (clinicalCommandOptions.commonNumericOptions.count) {
            params.put(QueryOptions.COUNT, clinicalCommandOptions.commonNumericOptions.count);
        }
        return params;
    }
}
