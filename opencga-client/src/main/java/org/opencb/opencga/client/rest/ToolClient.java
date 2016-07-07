package org.opencb.opencga.client.rest;


import org.opencb.opencga.catalog.models.Tool;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.catalog.models.acls.ToolAclEntry;


/**
 * Created by sgallego on 6/30/16.
 */
public class ToolClient extends AbstractParentClient<Tool, ToolAclEntry> {

    private static final String TOOLS_URL = "tools";

    protected ToolClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = TOOLS_URL;
        this.clazz = Tool.class;
        this.aclClass = ToolAclEntry.class;
    }
}
