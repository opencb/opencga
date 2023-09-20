package org.opencb.opencga.analysis.panel;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.panel.PanelImportParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;

@Tool(id = PanelImportTask.ID, resource = Enums.Resource.DISEASE_PANEL, description = PanelImportTask.DESCRIPTION)
public class PanelImportTask extends OpenCgaToolScopeStudy {
    public final static String ID = "panel-import";
    public static final String DESCRIPTION = "Import panels from other sources.";

    private String studyFqn;

    @ToolParams
    protected PanelImportParams params = new PanelImportParams();

    @Override
    protected void check() throws Exception {
        super.check();
        studyFqn = getStudy();

        if (StringUtils.isEmpty(params.getSource())) {
            throw new ToolException("Missing source from where to import panels.");
        }
    }

    @Override
    protected void run() throws Exception {
        step(() -> catalogManager.getPanelManager().importFromSource(studyFqn, params.getSource(), params.getId(), token));
    }

}
