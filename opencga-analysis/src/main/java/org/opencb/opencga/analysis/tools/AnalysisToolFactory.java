package org.opencb.opencga.analysis.tools;

import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.tools.OpenCgaTool;
import org.opencb.opencga.core.tools.ToolFactory;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class AnalysisToolFactory extends ToolFactory {

    public Tool getTool(String toolId) throws ToolException {
        return getToolClass(toolId).getAnnotation(Tool.class);
    }

    public final OpenCgaAnalysisTool createTool(String toolId) throws ToolException {
        return createTool(getToolClass(toolId));
    }

    public final OpenCgaAnalysisTool createTool(Class<? extends OpenCgaTool> aClass) throws ToolException {
        Tool annotation = aClass.getAnnotation(Tool.class);
        if (annotation == null) {
            throw new ToolException("Class " + aClass + " does not have the required java annotation @" + Tool.class.getSimpleName());
        }
        try {
            return (OpenCgaAnalysisTool) aClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new ToolException("Can't instantiate class " + aClass + " from tool '" + annotation.id() + "'", e);
        }
    }

    public Collection<Class<? extends OpenCgaTool>> getTools() {
        loadTools();
        return toolsList;
    }

    public Map<String, Set<Class<? extends OpenCgaTool>>> getDuplicatedTools() {
        loadTools();
        return duplicatedTools;
    }
}
