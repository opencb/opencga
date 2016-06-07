package org.opencb.opencga.catalog.config;

import java.util.List;

/**
 * Created by pfurio on 02/06/16.
 */
public class Audit {

    private long maxDocuments;
    private long maxSize;
    private String javaClass;
    private List<String> exclude;

    public Audit() {
    }

    public Audit(long maxDocuments, long maxSize, String javaClass, List<String> exclude) {
        this.maxDocuments = maxDocuments;
        this.maxSize = maxSize;
        this.javaClass = javaClass;
        this.exclude = exclude;
    }

    public long getMaxDocuments() {
        return maxDocuments;
    }

    public Audit setMaxDocuments(long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public Audit setMaxSize(long maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public String getJavaClass() {
        return javaClass;
    }

    public Audit setJavaClass(String javaClass) {
        this.javaClass = javaClass;
        return this;
    }

    public List<String> getExclude() {
        return exclude;
    }

    public Audit setExclude(List<String> exclude) {
        this.exclude = exclude;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Audit{");
        sb.append("maxDocuments=").append(maxDocuments);
        sb.append(", maxSize=").append(maxSize);
        sb.append(", javaClass='").append(javaClass).append('\'');
        sb.append(", exclude=").append(exclude);
        sb.append('}');
        return sb.toString();
    }
}
