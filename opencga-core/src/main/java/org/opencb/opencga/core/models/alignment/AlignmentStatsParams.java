package org.opencb.opencga.core.models.alignment;

public class AlignmentStatsParams {
    public static final String DESCRIPTION = "Alignment stats params";

    private String file;

    public AlignmentStatsParams() {
    }

    public AlignmentStatsParams(String file) {
        this.file = file;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("AlignmentStatsParams{");
        sb.append("file='").append(file).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public AlignmentStatsParams setFile(String file) {
        this.file = file;
        return this;
    }
}
