package org.opencb.opencga.core.models.panel;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.tools.ToolParams;

import java.util.Arrays;
import java.util.List;

public class PanelImportParams extends ToolParams {

    @DataField(id = ParamConstants.PANEL_SOURCE_PARAM, description = ParamConstants.PANEL_IMPORT_SOURCE_DESCRIPTION)
    private Source source;

    @Deprecated
    @DataField(id = ParamConstants.PANEL_SOURCE_ID, description = "Deprecated parameter. Use 'panelIds' parameter instead.")
    private String id;

    @DataField(id = ParamConstants.PANEL_IMPORT_IDS_PARAM, description = ParamConstants.PANEL_IMPORT_IDS_DESCRIPTION)
    private List<String> panelIds;

    @DataField(id = ParamConstants.PANEL_IMPORT_CONTENT_PARAM, description = ParamConstants.PANEL_IMPORT_CONTENT_DESCRIPTION)
    private String content;

    public enum Source {
        PANEL_APP,
        CANCER_GENE_CENSUS
    }

    public PanelImportParams() {
    }

    public PanelImportParams(Source source, String id, List<String> panelIds, String content) {
        this.source = source;
        this.id = id;
        this.panelIds = panelIds;
        this.content = content;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("PanelImportParams{");
        sb.append("source='").append(source).append('\'');
        sb.append(", id='").append(id).append('\'');
        sb.append(", panelIds=").append(panelIds);
        sb.append(", content='").append(content).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public Source getSource() {
        return source;
    }

    public PanelImportParams setSource(Source source) {
        this.source = source;
        return this;
    }

    // For backwards compatibility
    @Deprecated
    public PanelImportParams setSource(String source) {
        if (StringUtils.isNotEmpty(source)) {
            if ("panelapp".equals(source)) {
                this.source = Source.PANEL_APP;
            } else if ("cancer-gene-census".equals(source)) {
                this.source = Source.CANCER_GENE_CENSUS;
            } else {
                this.source = Source.valueOf(source);
            }
        }
        return this;
    }

    @Deprecated
    public String getId() {
        return getPanelIds() != null ? String.join(",", getPanelIds()) : id;
    }

    @Deprecated
    public PanelImportParams setId(String id) {
        setPanelIds(Arrays.asList(id.split(",")));
        return this;
    }

    public List<String> getPanelIds() {
        return panelIds;
    }

    public PanelImportParams setPanelIds(List<String> panelIds) {
        this.panelIds = panelIds;
        return this;
    }

    public String getContent() {
        return content;
    }

    public PanelImportParams setContent(String content) {
        this.content = content;
        return this;
    }
}
