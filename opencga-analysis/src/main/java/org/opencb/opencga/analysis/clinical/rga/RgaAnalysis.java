package org.opencb.opencga.analysis.clinical.rga;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = RgaAnalysis.ID, resource = Enums.Resource.CLINICAL, type = Tool.Type.OPERATION, description = "Index RGA study.")
public class RgaAnalysis extends OpenCgaTool {

    public final static String ID = "rga-index";
    public final static String DESCRIPTION = "Generate Recessive Gene Analysis secondary index";

    public final static String STUDY_PARAM = "study";
    public final static String FILE_PARAM = "file";

    private RgaManager rgaManager;

    private String study;
    private String file;

    @Override
    protected void check() throws Exception {
        this.rgaManager = new RgaManager(configuration, storageConfiguration);
    }

    @Override
    protected void run() throws Exception {
        // Get all the studies
        this.rgaManager.index(study, file, token);
    }

}
