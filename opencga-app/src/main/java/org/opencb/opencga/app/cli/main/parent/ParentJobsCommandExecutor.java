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
package org.opencb.opencga.app.cli.main.parent;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.app.cli.main.utils.JobsTopManager;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.catalog.db.api.JobDBAdaptor;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.models.job.JobTop;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;

public class ParentJobsCommandExecutor extends CustomExecutor {

    public ParentJobsCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                     SessionManager session, Logger logger) {
        super(options, token, clientConfiguration, session, logger);
    }

    public ParentJobsCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                     SessionManager session, Logger logger, OpenCGAClient openCGAClient) {
        super(options, token, clientConfiguration, session, logger, openCGAClient);
    }

    public RestResponse<JobTop> top() throws Exception {
        // JobsCommandOptions.TopCommandOptions c = jobsCommandOptions.topCommandOptions;

        Query query = new Query();
        query.putIfNotEmpty(JobDBAdaptor.QueryParams.STUDY.key(), String.valueOf(options.get("study")));
        query.putIfNotEmpty(ParamConstants.JOB_TOOL_ID_PARAM, String.valueOf(options.get("toolId")));
        query.putIfNotEmpty(ParamConstants.INTERNAL_STATUS_PARAM, String.valueOf(options.get("internalStatus")));
        query.putIfNotEmpty(ParamConstants.JOB_USER_PARAM, String.valueOf(options.get("userId")));
        query.putIfNotEmpty(ParamConstants.JOB_PRIORITY_PARAM, String.valueOf(options.get("priority")));
        query.putAll(options);

        new JobsTopManager(openCGAClient, query, 2, 20, 2, false).run();
        RestResponse<JobTop> res = new RestResponse<>();
        res.setType(QueryType.VOID);
        return res;
    }
/*
    public RestResponse<JobTop> log(JobsCommandOptions.LogCommandOptions c) throws Exception {
        //  JobsCommandOptions.LogCommandOptions c = jobsCommandOptions.logCommandOptions;
        new JobsLog(openCGAClient, c, System.out).run();
        RestResponse<JobTop> res = new RestResponse<>();
        res.setType(QueryType.VOID);
        return res;
    }
*/
}
