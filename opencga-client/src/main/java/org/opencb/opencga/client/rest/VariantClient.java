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

import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.models.operations.variant.VariantStatsExportParams;
import org.opencb.opencga.core.models.variant.CohortVariantStatsAnalysisParams;
import org.opencb.opencga.core.models.variant.GatkRunParams;
import org.opencb.opencga.core.models.variant.GwasAnalysisParams;
import org.opencb.opencga.core.models.variant.KnockoutAnalysisParams;
import org.opencb.opencga.core.models.variant.PlinkRunParams;
import org.opencb.opencga.core.models.variant.RvtestsRunParams;
import org.opencb.opencga.core.models.variant.SampleVariantFilterParams;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.models.variant.VariantExportParams;
import org.opencb.opencga.core.models.variant.VariantIndexParams;
import org.opencb.opencga.core.models.variant.VariantStatsAnalysisParams;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Variant webservices.
 *    Client version: 2.0.0
 *    PATH: analysis/variant
 */
public class VariantClient extends AbstractParentClient {

    public VariantClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Filter and export variants from the variant storage to a file.
     * @param data Variant export params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> export(VariantExportParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, null, null, "export", params, POST, Job.class);
    }

    /**
     * .
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<VariantMetadata> metadata(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, null, null, "metadata", params, GET, VariantMetadata.class);
    }

    /**
     * Calculate and fetch aggregation stats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<QueryResponse> aggregationStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, null, null, "aggregationStats", params, GET, QueryResponse.class);
    }

    /**
     * Compute variant stats for any cohort and any set of variants. Optionally, index the result in the variant storage database.
     * @param data Variant stats params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runStats(VariantStatsAnalysisParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "stats", null, "run", params, POST, Job.class);
    }

    /**
     * Remove variant files from the variant storage.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> deleteFile(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "file", null, "delete", params, DELETE, Job.class);
    }

    /**
     * Read variant annotations metadata from any saved versions.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<VariantAnnotation> metadataAnnotation(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "annotation", null, "metadata", params, GET, VariantAnnotation.class);
    }

    /**
     * Export calculated variant stats and frequencies.
     * @param data Variant stats export params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> exportStats(VariantStatsExportParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "stats", null, "export", params, POST, Job.class);
    }

    /**
     * Calculate the possible genotypes for the members of a family.
     * @param modeOfInheritance Mode of inheritance.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> genotypesFamily(String modeOfInheritance, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.putIfNotNull("modeOfInheritance", modeOfInheritance);
        return execute("analysis/variant", null, "family", null, "genotypes", params, GET, ObjectMap.class);
    }

    /**
     * Get samples given a set of variants.
     * @param data Sample variant filter params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runSample(SampleVariantFilterParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "sample", null, "run", params, POST, Job.class);
    }

    /**
     * Get sample data of a given variant.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Variant> querySample(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "sample", null, "query", params, GET, Variant.class);
    }

    /**
     * Compute sample variant stats for the selected list of samples.
     * @param data Sample variant stats params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runSampleStats(SampleVariantStatsAnalysisParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "sample/stats", null, "run", params, POST, Job.class);
    }

    /**
     * Read sample variant stats from list of samples.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<SampleVariantStats> infoSampleStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "sample/stats", null, "info", params, GET, SampleVariantStats.class);
    }

    /**
     * Delete sample variant stats from a sample.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<SampleVariantStats> deleteSampleStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "sample/stats", null, "delete", params, DELETE, SampleVariantStats.class);
    }

    /**
     * Compute cohort variant stats for the selected list of samples.
     * @param data Cohort variant stats params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runCohortStats(CohortVariantStatsAnalysisParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "cohort/stats", null, "run", params, POST, Job.class);
    }

    /**
     * Read cohort variant stats from list of cohorts.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<VariantSetStats> infoCohortStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "cohort/stats", null, "info", params, GET, VariantSetStats.class);
    }

    /**
     * Delete cohort variant stats from a cohort.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<SampleVariantStats> deleteCohortStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "cohort/stats", null, "delete", params, DELETE, SampleVariantStats.class);
    }

    /**
     * Run a Genome Wide Association Study between two cohorts.
     * @param data Gwas analysis params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runGwas(GwasAnalysisParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "gwas", null, "run", params, POST, Job.class);
    }

    /**
     * Plink is a whole genome association analysis toolset, designed to perform a range of basic, large-scale analyses.
     * @param data Plink params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runPlink(PlinkRunParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "plink", null, "run", params, POST, Job.class);
    }

    /**
     * Rvtests is a flexible software package for genetic association studies.
     * @param data rvtest params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runRvtests(RvtestsRunParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "rvtests", null, "run", params, POST, Job.class);
    }

    /**
     * GATK is a Genome Analysis Toolkit for variant discovery in high-throughput sequencing data.
     * @param data gatk params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runGatk(GatkRunParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "gatk", null, "run", params, POST, Job.class);
    }

    /**
     * .
     * @param data Gene knockout analysis params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runKnockout(KnockoutAnalysisParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, "knockout", null, "run", params, POST, Job.class);
    }

    /**
     * Index variant files into the variant storage.
     * @param data Variant index params.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> index(VariantIndexParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/variant", null, null, null, "index", params, POST, Job.class);
    }

    /**
     * Query variant annotations from any saved versions.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<VariantAnnotation> queryAnnotation(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, "annotation", null, "query", params, GET, VariantAnnotation.class);
    }

    /**
     * Filter and fetch variants from indexed VCF files in the variant storage.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Variant> query(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/variant", null, null, null, "query", params, GET, Variant.class);
    }
}
