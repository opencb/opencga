package org.opencb.opencga.server.generator.config;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class Options {

    private String outputDir;

    private List<String> ignoreTypes;

    public Options() {
    }

    public String getOutputDir() {
        return outputDir;
    }

    public Options setOutputDir(String outputDir) {
        this.outputDir = outputDir;
        return this;
    }

    private String getFolderAsPackage(String inputDir) {
        String res = "";
        if (inputDir.contains("/src/main/java/")) {
            inputDir = inputDir.substring(inputDir.lastIndexOf("/src/main/java/") + "/src/main/java/".length());
        }
        res = inputDir.replace("/", ".");
        if (res.endsWith(".")) {
            res = res.substring(0, res.length() - 1);
        }
        return res;
    }

    public String getOptionsOutputDir() {
        String res = "";
        if (outputDir.endsWith("/")) {
            res = outputDir + "options";
        } else {
            res = outputDir + "/options";
        }
        return res;
    }

    public String getExecutorsOutputDir() {
        String res = "";
        if (outputDir.endsWith("/")) {
            res = outputDir + "executors";
        } else {
            res = outputDir + "/executors";
        }
        return res;
    }

    public String getExecutorsPackage() {
        String res = "";
        if (outputDir != null && !StringUtils.isEmpty(outputDir)) {
            res = getFolderAsPackage(outputDir) + ".executors";
        }
        return res;
    }

    public String getOptionsPackage() {
        String res = "";
        if (outputDir != null && !StringUtils.isEmpty(outputDir)) {
            res = getFolderAsPackage(outputDir) + ".options";
        }
        return res;
    }

    public String getParserPackage() {
        String res = "";
        if (outputDir != null && !StringUtils.isEmpty(outputDir)) {
            res = getFolderAsPackage(outputDir);
        }
        return res;
    }

    public List<String> getIgnoreTypes() {
        return ignoreTypes;
    }

    public Options setIgnoreTypes(List<String> ignoreTypes) {
        this.ignoreTypes = ignoreTypes;
        return this;
    }
}
