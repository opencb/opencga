package org.opencb.opencga.ws;

import org.opencb.opencga.account.beans.Job;
import org.opencb.opencga.lib.analysis.AnalysisJobExecuter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

@Path("/account/{accountId}/analysis/job/{jobId}")
public class JobAnalysisWSServer extends GenericWSServer {
	private String accountId;
	private String projectId;
	private String jobId;

	public JobAnalysisWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest,
			@DefaultValue("") @PathParam("accountId") String accountId,
			@DefaultValue("default") @QueryParam("projectId") String projectId,
			@DefaultValue("") @PathParam("jobId") String jobId) throws IOException {
		super(uriInfo, httpServletRequest);

		this.accountId = accountId;
		this.projectId = projectId;
		this.jobId = jobId;
	}

	@GET
	@Path("/result.{format}")
	public Response getResultFile(@PathParam("format") String format) {
		try {
			String res = cloudSessionManager.getJobResult(accountId, jobId, sessionId);
			return createOkResponse(res);
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse(e.getMessage());
		}
	}

	@GET
	@Path("/table")
	public Response table(@DefaultValue("") @QueryParam("filename") String filename,
			@DefaultValue("") @QueryParam("start") String start, @DefaultValue("") @QueryParam("limit") String limit,
			@DefaultValue("") @QueryParam("colNames") String colNames,
			@DefaultValue("") @QueryParam("colVisibility") String colVisibility,
			@DefaultValue("") @QueryParam("callback") String callback,
			@QueryParam("sort") @DefaultValue("false") String sort) {

		try {
			String res = cloudSessionManager.getFileTableFromJob(accountId, jobId, filename, start, limit, colNames,
					colVisibility, callback, sort, sessionId);
			return createOkResponse(res, MediaType.valueOf("text/javascript"));
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse(e.getMessage());
		}
	}

	@GET
	@Path("/poll")
	public Response pollJobFile(@DefaultValue("") @QueryParam("filename") String filename,
			@DefaultValue("true") @QueryParam("zip") String zip) {

		try {
			DataInputStream is = cloudSessionManager.getFileFromJob(accountId, jobId, filename, zip, sessionId);
			String name = null;
			if (zip.compareTo("true") != 0) {// PAKO zip != true
				name = filename;
			} else {
				name = filename + ".zip";
			}
			return createOkResponse(is, MediaType.APPLICATION_OCTET_STREAM_TYPE, name);
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse(e.getMessage());
		}
	}

	@GET
	@Path("/status")
	public Response getJobStatus() {
		try {
			String res = cloudSessionManager.checkJobStatus(accountId, jobId, sessionId);
			return createOkResponse(res);
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse(e.getMessage());
		}
	}

	@GET
	@Path("/delete")
	public Response deleteJob() {
		try {
			cloudSessionManager.deleteJob(accountId, projectId, jobId, sessionId);
			return createOkResponse("OK");
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse(e.getMessage());
		}
	}

	@GET
	@Path("/download")
	public Response downloadJob() {
		try {
			InputStream is = cloudSessionManager.getJobZipped(accountId, jobId, sessionId);
			return createOkResponse(is, MediaType.valueOf("application/zip"), jobId + ".zip");
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse(e.getMessage());
		}
	}

	@GET
	@Path("/result.js")
	public Response getResult() {
		try {

			// AnalysisJobExecuter aje = new AnalysisJobExecuter(analysis);
			// cloudSessionManager.get
			Job job = cloudSessionManager.getJob(accountId, jobId, sessionId);
			AnalysisJobExecuter aje = new AnalysisJobExecuter(job.getToolName());
			InputStream is = aje.getResultInputStream();
			cloudSessionManager.incJobVisites(accountId, jobId, sessionId);
			return createOkResponse(is, MediaType.valueOf("text/javascript"), "result.js");

			// String resultToUse = aje.getResult();
			// String jobObj = cloudSessionManager.getJobObject(accountId,
			// jobId);
			// StringBuilder sb = new StringBuilder();
			// String c = "\"";
			// sb.append("{");
			// sb.append(c + "result" + c + ":"+ c + resultJson + c + ",");
			// sb.append(c + "resultToUse" + c + ":"+ c + resultToUse + c +
			// ",");
			// sb.append(c + "job" + c + ":" + c + jobObj + c);
			// sb.append("}");
			// return createOkResponse(sb.toString());
		} catch (Exception e) {
			logger.error(e.toString());
			return createErrorResponse("can not get result json.");
		}
	}

}
