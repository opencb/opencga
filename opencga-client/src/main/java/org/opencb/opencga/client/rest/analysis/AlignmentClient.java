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
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;

import static org.opencb.opencga.core.api.ParamConstants.*;

/**
 * Created by pfurio on 11/11/16.
 */
public class AlignmentClient extends AbstractParentClient {

    private static final String ALIGNMENT_URL = "analysis/alignment";

    public AlignmentClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    //-------------------------------------------------------------------------
    // INDEX
    //-------------------------------------------------------------------------

    public RestResponse<Job> index(String study, String inputFile) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, inputFile);

        return execute(ALIGNMENT_URL, "index", params, POST, Job.class);
    }

    //-------------------------------------------------------------------------
    // QUERY
    //-------------------------------------------------------------------------

    public RestResponse<ReadAlignment> query(String study, String file, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, file);

        return execute(ALIGNMENT_URL, "query", params, GET, ReadAlignment.class);
    }

    //-------------------------------------------------------------------------

    public RestResponse<Long> count(String study, String file, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, file);

        return execute(ALIGNMENT_URL, "query", params, GET, Long.class);
    }

    //-------------------------------------------------------------------------
    // S T A T S
    //-------------------------------------------------------------------------

    public RestResponse<Job> statsRun(String study, String file) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, file);

        return execute(ALIGNMENT_URL, "stats/run", params, POST, Job.class);
    }

    public RestResponse<String> statsInfo(String study, String file) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, file);

        return execute(ALIGNMENT_URL, "stats/info", params, GET, String.class);
    }

    public RestResponse<File> statsQuery(ObjectMap params) throws IOException {
        return execute(ALIGNMENT_URL, "stats/query", params, GET, File.class);
    }

    //-------------------------------------------------------------------------
    // C O V E R A G E
    //-------------------------------------------------------------------------

    public RestResponse<Job> coverageRun(String study, String inputFile, int windowSize) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, inputFile);
        params.putIfNotNull(COVERAGE_WINDOW_SIZE_PARAM, windowSize);

        return execute(ALIGNMENT_URL, "coverage/run", params, POST, Job.class);
    }

    public RestResponse<RegionCoverage> coverageQuery(String study, String file, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, file);

        return execute(ALIGNMENT_URL, "coverage/query", params, GET, RegionCoverage.class);
    }

    public RestResponse<RegionCoverage> coverageRatio(String study, String file1, String file2, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_1_PARAM, file1);
        params.putIfNotEmpty(FILE_ID_2_PARAM, file2);

        return execute(ALIGNMENT_URL, "coverage/ratio", params, GET, RegionCoverage.class);
    }

    //-------------------------------------------------------------------------
    // W R A P P E R S     A N A L Y S I S
    //-------------------------------------------------------------------------

    public RestResponse<Job> bwaRun(String study, ObjectMap params) throws IOException {
        return execute(ALIGNMENT_URL, "/bwa/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public RestResponse<Job> samtoolsRun(String study, ObjectMap params) throws IOException {
        return execute(ALIGNMENT_URL, "/samtools/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public RestResponse<Job> deeptoolsRun(String study, ObjectMap params) throws IOException {
        return execute(ALIGNMENT_URL, "/deeptools/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public RestResponse<Job> fastqcRun(String study, ObjectMap params) throws IOException {
        return execute(ALIGNMENT_URL, "/fastqc/run", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }
}
