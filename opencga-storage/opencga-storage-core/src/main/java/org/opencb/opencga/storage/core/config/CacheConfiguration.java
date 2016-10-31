package org.opencb.opencga.storage.core.config;

/**
 * Created by wasim on 26/10/16.
 */
public class CacheConfiguration {

    /**
     * This field contain the host and port, ie. host[:port].
     */
    private String host;
    private boolean active;

    /**
     * Accepted values are: JSON, Kryo.
     */
    private String serialization;
    private int slowThreshold;
    private int maxFileSize;
    private String password;

    /**
     * Accepted values are: aln(alignment), var(variant).
     */
    private String allowedTypes;

    public static final boolean DEFAULT_ACTVE = true;
    public static final String DEFAULT_SERIALIZATION = "json";
    public static final String DEFAULT_ALLOWED_TYPE = "aln";
    public static final String DEFAULT_HOST = "localhost:6379";
    public static final String DEFAULT_PASSWORD = "";
    public static final int DEFAULT_MAX_FILE_SIZE = 500;

    public CacheConfiguration() {
        this(DEFAULT_HOST, DEFAULT_ACTVE, DEFAULT_SERIALIZATION, 50, DEFAULT_MAX_FILE_SIZE, DEFAULT_PASSWORD, DEFAULT_ALLOWED_TYPE);
    }

    public CacheConfiguration(String host, boolean active, String serialization, int slowThreshold, int maxFileSize,
                              String password, String allowedTypes) {
        this.host = host;
        this.active = active;
        this.serialization = serialization;
        this.slowThreshold = slowThreshold;
        this.maxFileSize = maxFileSize;
        this.password = password;
        this.allowedTypes = allowedTypes;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheConfiguration{"
                + "host='" + host + '\''
                + ", active=" + active
                + ", serialization='" + serialization + '\''
                + ", slowThreshold=" + slowThreshold
                + ", maxFileSize=" + maxFileSize
               // Password should not be in toString
                + ", allowedTypes='" + allowedTypes + '\''
                + '}');
        return sb.toString();
    }

    public String getHost() {
        return host;
    }

    public CacheConfiguration setHost(String host) {
        this.host = host;
        return this;
    }

    public boolean isActive() {
        return active;
    }

    public CacheConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public String getSerialization() {
        return serialization;
    }

    public CacheConfiguration setSerialization(String serialization) {
        this.serialization = serialization;
        return this;
    }

    public int getSlowThreshold() {
        return slowThreshold;
    }

    public CacheConfiguration setSlowThreshold(int slowThreshold) {
        this.slowThreshold = slowThreshold;
        return this;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    public CacheConfiguration setMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public CacheConfiguration setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getAllowedTypes() {
        return allowedTypes;
    }

    public CacheConfiguration setAllowedTypes(String allowedTypes) {
        this.allowedTypes = allowedTypes;
        return this;
    }
}
