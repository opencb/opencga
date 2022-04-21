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
}
