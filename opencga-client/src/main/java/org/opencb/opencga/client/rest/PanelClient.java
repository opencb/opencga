package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.DiseasePanel;
import org.opencb.opencga.catalog.models.acls.DiseasePanelAclEntry;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by pfurio on 10/06/16.
 */
public class PanelClient extends AbstractParentClient<DiseasePanel, DiseasePanelAclEntry> {
    private static final String PANEL_URL = "panels";

    protected PanelClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);

        this.category = PANEL_URL;
        this.clazz = DiseasePanel.class;
        this.aclClass = DiseasePanelAclEntry.class;
    }

    public QueryResponse<DiseasePanel> create(String studyId, String name, String disease, ObjectMap params)
            throws CatalogException, IOException {
        params = addParamsToObjectMap(params, "studyId", studyId, "name", name, "disease", disease);
        return execute(PANEL_URL, "create", params, GET, DiseasePanel.class);
    }
}
