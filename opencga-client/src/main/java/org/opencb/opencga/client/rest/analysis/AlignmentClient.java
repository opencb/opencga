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
import org.opencb.opencga.core.rest.RestResponse;

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

    public RestResponse<Job> index(String fileIds, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty("file", fileIds);

        return execute(ALIGNMENT_URL, "index", params, POST, Job.class);
    }

    public RestResponse<ReadAlignment> query(String fileIds, ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        params.putIfNotEmpty("file", fileIds);

        return execute(ALIGNMENT_URL, "query", params, GET, ReadAlignment.class);
    }

    //-------------------------------------------------------------------------
    // S T A T S
    //-------------------------------------------------------------------------

    public RestResponse<Job> statsRun(String study, String file) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY_PARAM, study);
        params.putIfNotEmpty("inputFile", file);

        return execute(ALIGNMENT_URL, "stats/run", params, POST, Job.class);
    }

    public RestResponse<String> statsInfo(String study, String file) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY_PARAM, study);
        params.putIfNotEmpty("inputFile", file);

        return execute(ALIGNMENT_URL, "stats/info", params, GET, String.class);
    }

    public RestResponse<File> statsQuery(ObjectMap params) throws IOException {
        if (params == null) {
            params = new ObjectMap();
        }
        return execute(ALIGNMENT_URL, "stats/query", params, GET, File.class);
    }

    //-------------------------------------------------------------------------
    // C O V E R A G E
    //-------------------------------------------------------------------------

    public RestResponse<Job> coverageRun(String study, String inputFile, int windowSize) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY_PARAM, study);
        params.putIfNotEmpty("inputFile", inputFile);
        params.putIfNotNull("windowSize", windowSize);

        return execute(ALIGNMENT_URL, "coverage/run", params, POST, Job.class);
    }

    public RestResponse<RegionCoverage> coverageQuery(String study, String inputFile, String region, String gene, int geneOffset,
                                                      boolean onlyExons, int exonOffset, String range, int windowSize) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_PARAM, inputFile);
        params.putIfNotEmpty(REGION_PARAM, region);
        params.putIfNotEmpty(GENE_PARAM, gene);
        params.putIfNotNull(GENE_OFFSET_PARAM, geneOffset);
        params.putIfNotNull(ONLY_EXONS_PARAM, onlyExons);
        params.putIfNotNull(EXON_OFFSET_PARAM, exonOffset);
        params.putIfNotEmpty(COVERAGE_RANGE_PARAM, range);
        params.putIfNotNull(COVERAGE_WINDOW_SIZE_PARAM, windowSize);

        return execute(ALIGNMENT_URL, "coverage/query", params, GET, RegionCoverage.class);
    }

    public RestResponse<RegionCoverage> coverageLog2Ratio(String study, String inputFile1, String inputFile2, String region, String gene,
                                                  int geneOffset, boolean onlyExons, int exonOffset, int windowSize) throws IOException {
        ObjectMap params = new ObjectMap();
        params.putIfNotNull(STUDY_PARAM, study);
        params.putIfNotEmpty(FILE_ID_1_PARAM, inputFile1);
        params.putIfNotEmpty(FILE_ID_2_PARAM, inputFile2);
        params.putIfNotEmpty(REGION_PARAM, region);
        params.putIfNotEmpty(GENE_PARAM, gene);
        params.putIfNotNull(GENE_OFFSET_PARAM, geneOffset);
        params.putIfNotNull(ONLY_EXONS_PARAM, onlyExons);
        params.putIfNotNull(EXON_OFFSET_PARAM, exonOffset);
        params.putIfNotNull(COVERAGE_WINDOW_SIZE_PARAM, windowSize);

        return execute(ALIGNMENT_URL, "coverage/log2Ratio", params, GET, RegionCoverage.class);
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
}
