package org.opencb.opencga.analysis.file;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

@Tool(id = AssociateAlignmentFiles.ID, resource = Enums.Resource.FILE, type = Tool.Type.OPERATION,
        description = "Automatically associate alignment files with its index and coverage files.", priority = Enums.Priority.LOW)
public class AssociateAlignmentFiles extends OpenCgaTool {

    public final static String ID = "associate-alignment-files";

    @Override
    protected void run() throws Exception {
        catalogManager.getFileManager().associateAlignmentFiles(getStudyFqn(), token);
    }

}
