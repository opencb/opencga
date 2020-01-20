/*
* Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.client.rest;

import org.ga4gh.models.ReadAlignment;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Alignment webservices.
 *    Client version: 2.0.0
 *    PATH: analysis/alignment
 */
public class AlignmentClient extends AbstractParentClient {

    public AlignmentClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Compute coverage for a list of alignment files.
     * @param file File ID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runCoverage(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file", file);
        return execute("analysis/alignment", null, "coverage", null, "run", params, POST, Job.class);
    }

    /**
     * Query the coverage of an alignment file for regions or genes.
     * @param file File ID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<RegionCoverage> queryCoverage(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file", file);
        return execute("analysis/alignment", null, "coverage", null, "query", params, GET, RegionCoverage.class);
    }

    /**
     * Compute coverage ratio from file #1 vs file #2, (e.g. somatic vs germline).
     * @param file1 Input file #1 (e.g. somatic file).
     * @param file2 Input file #2 (e.g. germline file).
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<RegionCoverage> ratioCoverage(String file1, String file2, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file1", file1);
        params.putIfNotNull("file2", file2);
        return execute("analysis/alignment", null, "coverage", null, "ratio", params, GET, RegionCoverage.class);
    }

    /**
     * Compute stats for a given alignment file.
     * @param file File ID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runStats(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file", file);
        return execute("analysis/alignment", null, "stats", null, "run", params, POST, Job.class);
    }

    /**
     * Show the stats for a given alignment file.
     * @param file File ID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<String> infoStats(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file", file);
        return execute("analysis/alignment", null, "stats", null, "info", params, GET, String.class);
    }

    /**
     * Fetch alignment files according to their stats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> queryStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/alignment", null, "stats", null, "query", params, GET, File.class);
    }

    /**
     * BWA is a software package for mapping low-divergent sequences against a large reference genome.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runBwa(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/alignment", null, "bwa", null, "run", params, POST, Job.class);
    }

    /**
     * Samtools is a program for interacting with high-throughput sequencing data in SAM, BAM and CRAM formats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runSamtools(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/alignment", null, "samtools", null, "run", params, POST, Job.class);
    }

    /**
     * Deeptools is a suite of python tools particularly developed for the efficient analysis of high-throughput sequencing data, such as
     *     ChIP-seq, RNA-seq or MNase-seq.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runDeeptools(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/alignment", null, "deeptools", null, "run", params, POST, Job.class);
    }

    /**
     * A quality control tool for high throughput sequence data.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runFastqc(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/alignment", null, "fastqc", null, "run", params, POST, Job.class);
    }

    /**
     * Index alignment file.
     * @param file File ID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> index(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file", file);
        return execute("analysis/alignment", null, null, null, "index", params, POST, Job.class);
    }

    /**
     * Search over indexed alignments.
     * @param file File ID.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ReadAlignment> query(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("file", file);
        return execute("analysis/alignment", null, null, null, "query", params, GET, ReadAlignment.class);
    }
}
