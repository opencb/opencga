package org.opencb.opencga.client.rest.analysis;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.models.Job;

import java.io.IOException;

public class ToolClient extends AbstractParentClient {

    private static final String TOOL_URL = "analysis/tool";

    public ToolClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    public DataResponse<Job> execute(String study, ObjectMap bodyParams) throws IOException {
        if (bodyParams == null) {
            bodyParams = new ObjectMap();
        }
        ObjectMap params = new ObjectMap()
                .append("body", bodyParams)
                .append("study", study);
        return execute(TOOL_URL, "execute", params, POST, Job.class);
    }

}
