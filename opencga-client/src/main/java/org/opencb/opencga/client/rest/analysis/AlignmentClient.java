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

package org.opencb.opencga.client.rest.analysis;

import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.tools.alignment.stats.AlignmentGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.models.Job;

import java.io.IOException;

/**
 * Created by pfurio on 11/11/16.
 */
public class AlignmentClient extends AbstractParentClient {

    private static final String ALIGNMENT_URL = "analysis/alignment";

    public AlignmentClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    public DataResponse<Job> index(String fileIds, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty("file", fileIds);
        return execute(ALIGNMENT_URL, "index", params, GET, Job.class);
    }

    public DataResponse<ReadAlignment> query(String fileIds, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty("file", fileIds);
        return execute(ALIGNMENT_URL, "query", params, GET, ReadAlignment.class);
    }

    public DataResponse<AlignmentGlobalStats> stats(String fileIds, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty("file", fileIds);
        return execute(ALIGNMENT_URL, "stats", params, GET, AlignmentGlobalStats.class);
    }

    public DataResponse<RegionCoverage> coverage(String fileIds, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty("file", fileIds);
        return execute(ALIGNMENT_URL, "coverage", params, GET, RegionCoverage.class);
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    public DataResponse<Job> bwaRun(String study, ObjectMap params) throws IOException {
        return execute(ALIGNMENT_URL, "/bwa/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> samtoolsRun(String study, ObjectMap params) throws IOException {
        return execute(ALIGNMENT_URL, "/samtools/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }
}
