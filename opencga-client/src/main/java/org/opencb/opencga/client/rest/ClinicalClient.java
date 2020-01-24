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

import java.util.List;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisAclUpdateParams;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysisCreateParams;
import org.opencb.opencga.core.models.clinical.ClinicalUpdateParams;
import org.opencb.opencga.core.models.clinical.InterpretationCreateParams;
import org.opencb.opencga.core.models.clinical.InterpretationUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the Clinical webservices.
 *    Client version: 2.0.0
 *    PATH: analysis/clinical
 */
public class ClinicalClient extends AbstractParentClient {

    public ClinicalClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Index clinical analysis interpretations in the clinical variant database.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> indexInterpretation(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation", null, "index", params, GET, ObjectMap.class);
    }

    /**
     * Update clinical analysis attributes.
     * @param clinicalAnalyses Comma separated list of clinical analysis ids.
     * @param data JSON containing clinical analysis information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ClinicalAnalysis> update(String clinicalAnalyses, ClinicalUpdateParams data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/clinical", clinicalAnalyses, null, null, "update", params, POST, ClinicalAnalysis.class);
    }

    /**
     * Update Interpretation fields.
     * @param clinicalAnalysis Clinical analysis id.
     * @param interpretation Interpretation id.
     * @param data JSON containing clinical interpretation information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Interpretation> updateInterpretation(String clinicalAnalysis, String interpretation, InterpretationUpdateParams
        data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/clinical", clinicalAnalysis, "interpretations", interpretation, "update", params, POST,
                Interpretation.class);
    }

    /**
     * Create a new clinical analysis.
     * @param data JSON containing clinical analysis information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ClinicalAnalysis> create(ClinicalAnalysisCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/clinical", null, null, null, "create", params, POST, ClinicalAnalysis.class);
    }

    /**
     * Query for reported variants.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> queryInterpretation(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation", null, "query", params, GET, ObjectMap.class);
    }

    /**
     * Clinical analysis search.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ClinicalAnalysis> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, null, null, "search", params, GET, ClinicalAnalysis.class);
    }

    /**
     * Clinical interpretation analysis.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> statsInterpretation(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation", null, "stats", params, GET, ObjectMap.class);
    }

    /**
     * Clinical analysis info.
     * @param clinicalAnalyses Comma separated list of clinical analysis IDs or names up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ClinicalAnalysis> info(String clinicalAnalyses, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", clinicalAnalyses, null, null, "info", params, GET, ClinicalAnalysis.class);
    }

    /**
     * Returns the acl of the clinical analyses. If member is provided, it will only return the acl for the member.
     * @param clinicalAnalyses Comma separated list of clinical analysis IDs or names up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String clinicalAnalyses, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", clinicalAnalyses, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to add ACLs.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, ClinicalAnalysisAclUpdateParams data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/clinical", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Add or remove Interpretations to/from a Clinical Analysis.
     * @param clinicalAnalysis Clinical analysis ID.
     * @param data JSON containing clinical analysis information.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ClinicalAnalysis> updateInterpretations(String clinicalAnalysis, InterpretationCreateParams data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/clinical", clinicalAnalysis, "interpretations", null, "update", params, POST, ClinicalAnalysis.class);
    }

    /**
     * Update comments of an Interpretation.
     * @param clinicalAnalysis Clinical analysis id.
     * @param interpretation Interpretation id.
     * @param data JSON containing a list of comments.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Interpretation> updateComments(String clinicalAnalysis, String interpretation, List data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/clinical", clinicalAnalysis, "interpretations", interpretation, "comments/update", params, POST,
                Interpretation.class);
    }

    /**
     * Update reported variants of an interpretation.
     * @param clinicalAnalysis Clinical analysis id.
     * @param interpretation Interpretation id.
     * @param data JSON containing a list of reported variants.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Interpretation> updatePrimaryFindings(String clinicalAnalysis, String interpretation, List data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("analysis/clinical", clinicalAnalysis, "interpretations", interpretation, "primaryFindings/update", params, POST,
                Interpretation.class);
    }

    /**
     * TEAM interpretation analysis.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runInterpretationTeam(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation/team", null, "run", params, POST, Job.class);
    }

    /**
     * GEL Tiering interpretation analysis.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runInterpretationTiering(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation/tiering", null, "run", params, POST, Job.class);
    }

    /**
     * Interpretation custom analysis.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runInterpretationCustom(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation/custom", null, "run", params, POST, Job.class);
    }

    /**
     * Cancer Tiering interpretation analysis.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> runInterpretationCancerTiering(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation/cancerTiering", null, "run", params, POST, Job.class);
    }

    /**
     * Search for secondary findings for a given query.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ReportedVariant> primaryFindingsInterpretation(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation", null, "primaryFindings", params, GET, ReportedVariant.class);
    }

    /**
     * Search for secondary findings for a given sample.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ReportedVariant> secondaryFindingsInterpretation(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("analysis/clinical", null, "interpretation", null, "secondaryFindings", params, GET, ReportedVariant.class);
    }
}
