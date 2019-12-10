package org.opencb.opencga.analysis.alignment;

import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.core.annotations.Tool;

@Tool(id = AlignmentIndexOperation.ID, type = Tool.ToolType.ALIGNMENT,
        description = "Index alignment.")
public class AlignmentIndexOperation extends OpenCgaTool {

    public final static String ID = "alignment-index";
    public final static String DESCRIPTION = "Index a given alignment file, e.g., create a .bai file from a .bam file";

    @Override
    protected void run() throws Exception {
        // TODO: move here the code
    }
}
