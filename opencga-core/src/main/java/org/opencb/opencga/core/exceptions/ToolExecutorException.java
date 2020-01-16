package org.opencb.opencga.core.exceptions;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.annotations.ToolExecutor;

import java.util.List;

/**
 * Created by jtarraga on 30/01/17.
 */
public class ToolExecutorException extends ToolException {

    public ToolExecutorException(String msg) {
        super(msg);
    }

    public ToolExecutorException(Exception e) {
        super(e);
    }

    public ToolExecutorException(String msg, Exception e) {
        super(msg, e);
    }

    public ToolExecutorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ToolExecutorException(Throwable cause) {
        super(cause);
    }

    public static ToolExecutorException executorNotFound(Class<?> clazz, String tool, String executorId,
                                                         List<ToolExecutor.Source> sourceTypes,
                                                         List<ToolExecutor.Framework> frameworks) {

        String requirements = "";
        if (clazz != null) {
            requirements = " extending class " + clazz;
        }
        if (StringUtils.isNotEmpty(executorId)) {
            requirements = " with executorId='" + executorId + "'";
        }
        if (CollectionUtils.isNotEmpty(sourceTypes)) {
            requirements = " for source ='" + sourceTypes + "'";
        }
        if (CollectionUtils.isNotEmpty(frameworks)) {
            requirements = " for frameworks=" + frameworks;
        }
        requirements += ".";
        return new ToolExecutorException("Could not find a valid OpenCgaAnalysis executor for the tool '" + tool + "'" + requirements);
    }

    public static ToolExecutorException cantInstantiate(Class<?> clazz, Exception cause) {
        return new ToolExecutorException("Could not create class an instance of class " + clazz, cause);
    }

    public static ToolException exceptionAtToolExecutor(String executorId, String toolId, Exception e) {
        return new ToolException("Catch exception at tool executor " + executorId + " from tool " + toolId, e);
    }
}
