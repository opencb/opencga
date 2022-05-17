package org.opencb.opencga.core.common;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ExceptionUtils {

    public static String prettyExceptionMessage(Throwable exception) {
        return prettyExceptionMessage(exception, new StringBuilder()).toString();
    }

    public static String prettyExceptionMessage(Throwable exception, boolean multiline, boolean includeClassName) {
        return prettyExceptionMessage(exception, new StringBuilder(), multiline, includeClassName).toString();
    }

    public static StringBuilder prettyExceptionMessage(Throwable exception, StringBuilder message) {
        return prettyExceptionMessage(exception, message, false);
    }

    public static StringBuilder prettyExceptionMessage(Throwable exception, StringBuilder message, boolean multiline) {
        return prettyExceptionMessage(exception, message, multiline, multiline);
    }

    public static StringBuilder prettyExceptionMessage(Throwable exception, StringBuilder message, boolean multiline,
                                                       boolean includeClassName) {
        Set<String> messages = new HashSet<>();
        do {
            if (exception.getMessage() != null && !messages.add(exception.getMessage())) {
                // Duplicated message. Skip this cause
                exception = exception.getCause();
                continue;
            }
            if (message.length() != 0) {
                if (multiline) {
                    message.append("\n");
                } else {
                    message.append(" ; ");
                }
            }
            String exMessage = exception.getMessage();
            if (StringUtils.isBlank(exMessage)) {
                if (!includeClassName) {
                    exMessage = exception.getClass().getSimpleName();
                }
            }
            if (includeClassName) {
                message.append("[").append(exception.getClass().getSimpleName()).append("] ");
            }
            message.append(exMessage);

            exception = exception.getCause();
        } while (exception != null);
        return message;
    }

    public static String prettyExceptionStackTrace(Throwable exception) {
        return org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(exception);
    }

    public static List<StackTraceElement> getOpencbStackTrace() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        List<StackTraceElement> relevant = new LinkedList<>();
        int skip = 2; // remove Thread.getStackTrace and ExceptionUtils.getRelevantStackTrace
        int context = 3;
        int actualContext = context;
        for (StackTraceElement element : stackTrace) {
            if (skip > 0) {
                skip--;
                continue;
            }
            relevant.add(element);
            if (!element.getClassName().startsWith("org.opencb.opencga")) {
                actualContext--;
            } else {
                actualContext = context;
            }
            if (actualContext == 0) {
                break;
            }
        }

        return relevant;
    }
}
