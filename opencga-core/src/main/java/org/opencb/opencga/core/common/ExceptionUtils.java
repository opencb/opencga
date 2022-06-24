package org.opencb.opencga.core.common;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
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
        String separator;
        if (multiline) {
            separator = "\n";
        } else {
            separator = " ; ";
        }
        return prettyExceptionMessage(exception, message, multiline, includeClassName, separator);
    }

    private static StringBuilder prettyExceptionMessage(Throwable exception, StringBuilder message, boolean multiline,
                                                       boolean includeClassName, String separator) {
        Set<String> messages = new HashSet<>();
        do {
            if (exception.getMessage() != null && !messages.add(exception.getMessage())
                    && exception.getSuppressed().length == 0) {
                // Duplicated message. Skip this cause
                exception = exception.getCause();
                continue;
            }
            if (message.length() != 0) {
                message.append(separator);
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
            if (exception.getSuppressed().length > 0) {
                StringBuilder sb = new StringBuilder();
                String intraSeparator = multiline ? separator + "  " : separator;
                for (Throwable suppressed : exception.getSuppressed()) {
                    prettyExceptionMessage(suppressed, sb, multiline, includeClassName, intraSeparator);
                }
                message.append(" <suppressed(");
                if (multiline) {
                    message.append(intraSeparator);
                } else {
                    message.append(" ");
                }
                message.append(sb);
                if (multiline) {
                    message.append(separator);
                } else {
                    message.append(" ");
                }
                message.append(")>");
            }

            exception = exception.getCause();
        } while (exception != null);
        return message;
    }

    public static String prettyExceptionStackTrace(Throwable exception) {
        return org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(exception);
    }
}
