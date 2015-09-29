package org.opencb.opencga.core.common;

/**
 * Created by jmmut on 2015-07-02.
 *
 * @author Jose Miguel Mut Lopez &lt;jmmut@ebi.ac.uk&gt;
 */
public class ExceptionUtils {
    public static String getExceptionString(Exception e) {
        StringBuilder stack = new StringBuilder(e.toString()).append("\n");
        for (StackTraceElement stackTraceElement : e.getStackTrace()) {
            stack.append("    ").append(stackTraceElement).append("\n");
        }
        return stack.toString();
    }
}
