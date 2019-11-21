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

import org.opencb.opencga.catalog.managers.JobManager;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/analysis")
@Produces(MediaType.APPLICATION_JSON)
public class AnalysisWSService extends OpenCGAWSServer {

    public static final String JOB_ID = "jobId";
    public static final String JOB_ID_DESCRIPTION = "Job Id";
    public static final String JOB_NAME = "jobName";
    public static final String JOB_NAME_DESCRIPTION = "Job Name";
    public static final String JOB_DESCRIPTION = "jobDescription";
    public static final String JOB_DESCRIPTION_DESCRIPTION = "Job Description";
    public static final String JOB_TAGS = "jobTags";
    public static final String JOB_TAGS_DESCRIPTION = "Job Tags";

    protected JobManager jobManager;

    public AnalysisWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        this(uriInfo.getPathParameters().getFirst("apiVersion"), uriInfo, httpServletRequest, httpHeaders);
    }

    public AnalysisWSService(String apiVersion, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(apiVersion, uriInfo, httpServletRequest, httpHeaders);

        this.jobManager = catalogManager.getJobManager();
    }

}
