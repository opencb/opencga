package org.opencb.opencga.ws;

import org.opencb.opencga.account.beans.Acl;
import org.opencb.opencga.account.beans.AnalysisPlugin;
import org.opencb.opencga.account.db.AccountManagementException;
import org.opencb.opencga.account.io.IOManagementException;
import org.opencb.opencga.lib.analysis.AnalysisExecutionException;
import org.opencb.opencga.lib.analysis.AnalysisJobExecuter;
import org.opencb.opencga.lib.analysis.SgeManager;
import org.opencb.opencga.lib.analysis.beans.Analysis;
import org.opencb.opencga.lib.analysis.beans.Execution;
import org.opencb.opencga.lib.analysis.beans.InputParam;
import org.opencb.opencga.common.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Path("/account/{accountId}/analysis/{analysis}")
public class AnalysisWSServer extends GenericWSServer {
	private AnalysisJobExecuter aje;
	private String baseUrl;
	private String accountId;
	private String analysis;
	private boolean analysisError;
	private String analysisErrorMsg;
	private String projectId;

	public AnalysisWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
			@DefaultValue("") @PathParam("analysis") String analysis,
			@DefaultValue("") @PathParam("accountId") String accountId,
			@DefaultValue("default") @QueryParam("projectId") String projectId) throws IOException {
		super(uriInfo, httpServletRequest);
		baseUrl = uriInfo.getBaseUri().toString();

		this.accountId = accountId;
		this.analysis = analysis;
		this.projectId = projectId;

		analysisError = false;
		analysisErrorMsg = "analysis not found.";
		try {
			aje = new AnalysisJobExecuter(analysis);
		} catch (Exception e) {
			logger.error(e.toString());
			analysisError = true;
		}
		
		
	}

	@GET
	public Response help1() {
		if (analysisError) {
			return createErrorResponse(analysisErrorMsg);
		}
		return createOkResponse(aje.help(baseUrl));
	}

	@GET
	@Path("/help")
	public Response help2() {
		if (analysisError) {
			return createErrorResponse(analysisErrorMsg);
		}
		return createOkResponse(aje.help(baseUrl));
	}

	@GET
	@Path("/params")
	public Response showParams() {
		if (analysisError) {
			return createErrorResponse(analysisErrorMsg);
		}
		return createOkResponse(aje.params());
	}

	@GET
	@Path("/test")
	public Response test() throws IOException {
		if (analysisError) {
			return createErrorResponse(analysisErrorMsg);
		}

		// Create job
		String jobId;
		try {
			jobId = cloudSessionManager.createJob("", projectId, null, "",
					new ArrayList<String>(), "", sessionId);
			String jobFolder = "/tmp/";
			return createOkResponse(aje.test(jobId, jobFolder));
		} catch (AccountManagementException | IOManagementException | AnalysisExecutionException e) {
			logger.error(e.toString());
			return createErrorResponse("could not create job.");
		}
	}

	@GET
	@Path("/status")
	public Response status(@DefaultValue("") @QueryParam("jobid") String jobId) throws Exception {
		if (analysisError) {
			return createErrorResponse(analysisErrorMsg);
		}
		try {
			return createOkResponse(SgeManager.status(analysis + "_" + jobId));
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse("job id not found.");
		}

	}

	@GET
	@Path("/run")
	public Response analysisGet() throws IOException, AccountManagementException {
		return this.analysis(params);
	}

	@POST
	@Path("/run")
	public Response analysisPost(MultivaluedMap<String, String> postParams) throws IOException, AccountManagementException {
		return this.analysis(postParams);
	}

	private Response analysis(MultivaluedMap<String, String> params) throws IOException, AccountManagementException {
		if (params.containsKey("sessionid")) {
			sessionId = params.get("sessionid").get(0);
			params.remove("sessionid");
		} else {
			return createErrorResponse("session is not initialized yet.");
		}

		// String accountId = null;
		// if (params.containsKey("accountid")) {
		// accountId = params.get("accountid").get(0);
		// params.remove("accountid");
		// } else {
		// return createErrorResponse("unknown account.");
		// }

		// Jquery put this parameter and it is sent to the tool
		if (params.containsKey("_")) {
			params.remove("_");
		}

		String analysisName = analysis;
        String toolName = analysis;
		if (analysis.contains(".")) {
			String[] split = analysis.split("\\.");
			analysisName = split[0];
		}

		String analysisOwner = "system";
		boolean hasPermission = false;
		try {
			List<AnalysisPlugin> userAnalysis = cloudSessionManager.getUserAnalysis(sessionId);
			for (AnalysisPlugin a : userAnalysis) {
				if (a.getName().equals(analysisName)) {
					analysisOwner = a.getOwnerId();
					// get execution permissions
					for (Acl acl : a.getAcl()) {
						if (acl.getAccountId().equals(accountId) && acl.isExecute()) {
							hasPermission = true;
							break;
						}
					}
					break;
				}
			}
		} catch (AccountManagementException e) {
			logger.error(e.toString());
			return createErrorResponse("invalid session id.");
		}

		// check execution permissions
		if (!analysisOwner.equals("system") && !hasPermission) {
			return createErrorResponse("invalid session id.");
		}

		Analysis analysisObj = null;
		try {
			aje = new AnalysisJobExecuter(analysis, analysisOwner);
			analysisObj = aje.getAnalysis();
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse("analysis not found.");
		}

		Execution execution = null;
		try {
			execution = aje.getExecution();
		} catch (AnalysisExecutionException e) {
			logger.error(e.toString());
			return createErrorResponse("executable not found.");
		}

		String jobName = "";
		if (params.containsKey("jobname")) {
			jobName = params.get("jobname").get(0);
			params.remove("jobname");
		}

		String jobFolder = null;
		if (params.containsKey("outdir")) {
			jobFolder = "buckets:" + params.get("outdir").get(0);
			jobFolder = StringUtils.parseObjectId(jobFolder).toString();
			params.remove("outdir");
		}

		boolean example = false;
		if (params.containsKey("example")) {
			example = Boolean.parseBoolean(params.get("example").get(0));
			params.remove("example");
		}


		// Set input param
		List<String> dataList = new ArrayList<String>();
		for (InputParam inputParam : execution.getInputParams()) {
			if (params.containsKey(inputParam.getName())) {
				List<String> dataIds = Arrays.asList(params.get(inputParam.getName()).get(0).split(","));
				List<String> dataPaths = new ArrayList<String>();
				for (String dataId : dataIds) {
					String dataPath = null;
					if (example) { // is a example
						dataPath = aje.getExamplePath(dataId);
					} else { // is a dataId
                        // TO FIX
                        dataPath = cloudSessionManager.getObjectPath(accountId, null, StringUtils.parseObjectId(dataId)).toString();
                    }

					if (dataPath.contains("ERROR")) {
						return createErrorResponse(dataPath);
					} else {
						dataPaths.add(dataPath);
						dataList.add(dataPath);
					}
				}
				params.put(inputParam.getName(), dataPaths);
			}
		}

		String jobId;
		try {
			jobId = cloudSessionManager.createJob(jobName, projectId, jobFolder, toolName,
					dataList, "", sessionId);
		} catch (AccountManagementException | IOManagementException e) {
			logger.error(e.toString());
			return createErrorResponse("could not create job.");
		}

		if (jobFolder == null) {
			jobFolder = cloudSessionManager.getJobFolder(accountId, jobId, sessionId);
		} else {
			jobFolder = cloudSessionManager.getAccountPath(accountId).resolve(jobFolder).toString();
		}

		// Set output param
		params.put(execution.getOutputParam(), Arrays.asList(jobFolder));

		// Create commmand line
		String commandLine = null;
		try {
			commandLine = aje.createCommandLine(execution.getExecutable(), params);
			cloudSessionManager.setJobCommandLine(accountId, jobId, commandLine, sessionId);
		} catch (AnalysisExecutionException | AccountManagementException e) {
			logger.error(e.toString());
			return createErrorResponse(e.getMessage());
		}

		try {
			aje.execute(jobId, jobFolder, commandLine);
		} catch (AnalysisExecutionException e) {
			logger.error(e.toString());
			return createErrorResponse("execution failed.");
		}

		return createOkResponse(jobId);
	}
}
