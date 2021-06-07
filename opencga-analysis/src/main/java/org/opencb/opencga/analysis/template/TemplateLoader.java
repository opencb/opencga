package org.opencb.opencga.analysis.template;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = TemplateLoader.ID, resource = Enums.Resource.USER, type = Tool.Type.OPERATION, description = "Load OpenCGA template metadata.")
public class TemplateLoader extends OpenCgaTool {

    public final static String ID = "templateLoader";

    @Override
    protected void run() throws Exception {

    }
}
