/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.core.config.storage;

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
    private int maxResultSize;
    private String password;

    /**
     * Accepted values are: aln(alignment), var(variant).
     */
    private String allowedTypes;

    public static final boolean DEFAULT_ACTVE = true;
    public static final String DEFAULT_SERIALIZATION = "json";
    public static final String DEFAULT_ALLOWED_TYPE = "aln,var";
    public static final String DEFAULT_HOST = "localhost:6379";
    public static final String DEFAULT_PASSWORD = "";
    public static final int DEFAULT_MAX_FILE_SIZE = 500;

    public CacheConfiguration() {
        this(DEFAULT_HOST, DEFAULT_ACTVE, DEFAULT_SERIALIZATION, 50, DEFAULT_MAX_FILE_SIZE, DEFAULT_PASSWORD,
                DEFAULT_ALLOWED_TYPE);
    }

    public CacheConfiguration(String host, boolean active, String serialization, int slowThreshold, int maxFileSize,
                              String password, String allowedTypes) {
        this.host = host;
        this.active = active;
        this.serialization = serialization;
        this.slowThreshold = slowThreshold;
        this.maxResultSize = maxFileSize;
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
                + ", maxResultSize=" + maxResultSize
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

    public int getMaxResultSize() {
        return maxResultSize;
    }

    public CacheConfiguration setMaxResultSize(int maxResultSize) {
        this.maxResultSize = maxResultSize;
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
