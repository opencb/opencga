package org.opencb.opencga.catalog.core.beans;

import java.util.List;

/**
 * Created by jacobo on 11/09/14.
 */
public class Analysis {

    private int id;
    private String name;
    private String date;
    private String ownerId;
    private int studyId;
    private String description;

    private List<Job> jobs;


}
