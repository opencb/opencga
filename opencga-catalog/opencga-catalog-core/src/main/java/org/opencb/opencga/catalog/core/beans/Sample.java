package org.opencb.opencga.catalog.core.beans;

import java.util.Map;

/**
 * Created by jacobo on 11/09/14.
 */
public class Sample {
    private String name;
    private String description;
    private String source;
    private Individual individual;
    private Map<String, Object> annotation;

}
