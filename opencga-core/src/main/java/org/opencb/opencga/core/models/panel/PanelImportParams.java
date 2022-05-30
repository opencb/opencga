package org.opencb.opencga.core.models.panel;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

public class PanelImportParams extends ToolParams {

    @DataField(id = ParamConstants.PANEL_SOURCE_PARAM, description = ParamConstants.PANEL_IMPORT_SOURCE_DESCRIPTION)
    private String source;

    @DataField(id = ParamConstants.PANEL_SOURCE_ID, description = ParamConstants.PANEL_SOURCE_ID_DESCRIPTION)
    private String id;

    public PanelImportParams() {
    }

    public PanelImportParams(String source, String id) {
        this.source = source;
        this.id = id;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PanelImportParams{");
        sb.append("source='").append(source).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getSource() {
        return source;
    }

    public PanelImportParams setSource(String source) {
        this.source = source;
        return this;
    }

    public String getId() {
        return id;
    }

    public PanelImportParams setId(String id) {
        this.id = id;
        return this;
    }
}
