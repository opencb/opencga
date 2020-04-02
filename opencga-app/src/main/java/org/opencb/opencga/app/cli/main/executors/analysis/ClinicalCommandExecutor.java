package org.opencb.opencga.app.cli.main.executors.analysis;

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
import org.opencb.opencga.core.models.clinical.TieringInterpretationAnalysisParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.RestResponse;

import static org.opencb.opencga.app.cli.main.options.ClinicalCommandOptions.TieringCommandOptions.TIERING_RUN_COMMAND;

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

            case TIERING_RUN_COMMAND:
                queryResponse = tiering();
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
        return openCGAClient.getClinicalAnalysisClient().info(commandOptions.clinical, params);
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

    private RestResponse<Job> tiering() throws ClientException {
        return openCGAClient.getClinicalAnalysisClient().runInterpretationTiering(
                new TieringInterpretationAnalysisParams(clinicalCommandOptions.tieringCommandOptions.clinicalAnalysisId,
                        clinicalCommandOptions.tieringCommandOptions.diseasePanelIds,
                        clinicalCommandOptions.tieringCommandOptions.penetrance,
                        clinicalCommandOptions.tieringCommandOptions.maxLowCoverage,
                        clinicalCommandOptions.tieringCommandOptions.includeLowCoverage),
                getParams(clinicalCommandOptions.tieringCommandOptions.studyId)
        );
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
