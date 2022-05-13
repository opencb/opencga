package org.opencb.opencga.test.config;

public class Logger {

    private String logLevel;

    public Logger() {
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Logger{\n");
        sb.append("logLevel='").append(logLevel).append('\'').append("\n");
        sb.append("}\n");
        return sb.toString();
    }

    public String getLogLevel() {
        return logLevel;
    }

    public Logger setLogLevel(String logLevel) {
        this.logLevel = logLevel;
        return this;
    }
}
