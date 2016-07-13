package org.opencb.opencga.client.rest;


import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.catalog.models.acls.ToolAcl;


/**
 * Created by sgallego on 6/30/16.
 */
public class ToolClient extends AbstractParentClient<Tool, ToolAcl> {

    private static final String TOOLS_URL = "tools";

    protected ToolClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = TOOLS_URL;
        this.clazz = Tool.class;
        this.aclClass = ToolAcl.class;
    }
}
