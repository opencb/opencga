package org.opencb.opencga.client.rest;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.client.config.ClientConfiguration;

import java.io.IOException;

/**
 * Created by swaathi on 10/05/16.
 */
public class StudyClient extends AbstractParentClient{
    private static final String STUDY_URL = "studies";

    protected StudyClient(String sessionId, ClientConfiguration configuration) {
        super(sessionId, configuration);
    }

    public QueryResponse<Study> get(String studyId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Study> studies = execute(STUDY_URL, studyId, "info", options, Study.class);
        return studies;
    }

    public QueryResponse<Sample> getSamples(String studyId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Sample> samples = execute(STUDY_URL, studyId, "samples", options, Sample.class);
        return samples;
    }

    public QueryResponse<File> getFiles(String studyId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<File> files = execute(STUDY_URL, studyId, "files", options, File.class);
        return files;
    }

    public QueryResponse<Job> getJobs(String studyId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Job> jobs = execute(STUDY_URL, studyId, "jobs", options, Job.class);
        return jobs;
    }

    public QueryResponse<ObjectMap> getStatus(String studyId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<ObjectMap> status = execute(STUDY_URL, studyId, "status", options, ObjectMap.class);
        return status;
    }

    public QueryResponse<Study> update(String studyId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<Study> updatedStudy = execute(STUDY_URL, studyId, "update", options, Study.class);
        return updatedStudy;
    }

    public QueryResponse<ObjectMap> delete(String studyId, QueryOptions options) throws CatalogException, IOException {
        QueryResponse<ObjectMap> study = execute(STUDY_URL, studyId, "delete", options, ObjectMap.class);
        return study;
    }
}
