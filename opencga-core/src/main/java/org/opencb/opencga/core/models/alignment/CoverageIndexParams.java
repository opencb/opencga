package org.opencb.opencga.core.models.alignment;

public class CoverageIndexParams {
    public static final String DESCRIPTION = "Coverage computation parameters";

    private String file;
    private int windowSize;

    public CoverageIndexParams() {
        windowSize = 1;
    }

    public CoverageIndexParams(String file, int windowSize) {
        this.file = file;
        this.windowSize = windowSize;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CoverageIndexParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", windowSize=").append(windowSize);
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public CoverageIndexParams setFile(String file) {
        this.file = file;
        return this;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public CoverageIndexParams setWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }
}
