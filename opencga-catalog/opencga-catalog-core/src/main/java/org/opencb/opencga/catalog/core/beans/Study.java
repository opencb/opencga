package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Study {
    private String studyId;
    private String studyName;
    private String creationDate;

    private List<Acl> acl;
    private List<File> files;

}
