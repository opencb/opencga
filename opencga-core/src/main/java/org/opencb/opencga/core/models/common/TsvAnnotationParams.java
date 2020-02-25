package org.opencb.opencga.core.models.common;

public class TsvAnnotationParams {

    /**
     * Content of the TSV file if this does has not been registered yet in OpenCGA.
     */
    private String content;


    public TsvAnnotationParams() {
    }

    public TsvAnnotationParams(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TsvAnnotationParams{");
        sb.append("content='").append(content).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getContent() {
        return content;
    }

    public TsvAnnotationParams setContent(String content) {
        this.content = content;
        return this;
    }
}
