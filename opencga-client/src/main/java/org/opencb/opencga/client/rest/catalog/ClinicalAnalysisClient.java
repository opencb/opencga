package org.opencb.opencga.client.rest.catalog;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.acls.permissions.ClinicalAnalysisAclEntry;

import java.io.IOException;

public class ClinicalAnalysisClient extends CatalogClient<ClinicalAnalysis, ClinicalAnalysisAclEntry> {

    private static final String CLINICAL_URL = "analysis/clinical";

    public ClinicalAnalysisClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = CLINICAL_URL;
        this.clazz = ClinicalAnalysis.class;
        this.aclClass = ClinicalAnalysisAclEntry.class;
    }

    public DataResponse<ClinicalAnalysis> create(String studyId, ObjectMap bodyParams) throws IOException, ClientException {
        if (bodyParams == null || bodyParams.size() == 0) {
            throw new ClientException("Missing body parameters");
        }

        ObjectMap params = new ObjectMap("body", bodyParams);
        params.append("study", studyId);
        return execute(CLINICAL_URL, "create", params, POST, ClinicalAnalysis.class);
    }

    public DataResponse<ClinicalAnalysis> groupBy(String studyId, String fields, ObjectMap params) throws IOException {
        params = addParamsToObjectMap(params, "study", studyId, "fields", fields);
        return execute(CLINICAL_URL, "groupBy", params, GET, ClinicalAnalysis.class);
    }

}
