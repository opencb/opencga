package org.opencb.opencga.client.rest.operations;

import org.opencb.commons.datastore.core.DataResponse;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.AbstractParentClient;
import org.opencb.opencga.core.models.Job;

import java.io.IOException;
import java.util.List;

public class OperationClient extends AbstractParentClient {

    public OperationClient(String userId, String sessionId, ClientConfiguration configuration) {
        super(userId, sessionId, configuration);
    }

    private static final String OPERATION_URL = "operation";

    public DataResponse<Job> variantFileDelete(String study, List<String> file, boolean resume, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/file/delete",
                copy(params)
                .append("study", study)
                .append("file", file)
                .append("resume", resume), DELETE, Job.class);
    }


    public DataResponse<Job> variantSecondaryIndex(String study, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/secondaryIndex", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> variantSecondaryIndexDelete(String study, List<String> sample, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/secondaryIndex/delete",
                copy(params)
                .append("study", study)
                .append("sample", sample), DELETE, Job.class);
    }

    public DataResponse<Job> variantAnnotationIndex(String study, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/annotation/index", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> variantAnnotationDelete(String project, String annotationId,  ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/annotation/delete",
                copy(params)
                .append("project", project)
                .append("annotationId", annotationId), DELETE, Job.class);
    }

    public DataResponse<Job> variantAnnotationSave(ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/annotation/save", new ObjectMap("body", params), POST, Job.class);
    }

    public DataResponse<Job> variantScoreIndex(String study, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/score/index", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> variantScoreDelete(String study, String score, boolean resume, boolean force, ObjectMap params)
            throws IOException {
        return execute(OPERATION_URL, "/variant/score/delete", copy(params)
                        .append("study", study)
                        .append("scoreName", score)
                        .append("resume", resume)
                        .append("force", force), DELETE, Job.class);
    }

    public DataResponse<Job> variantSampleGenotypeIndex(String study, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/sample/genotype/index",
                new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> variantFamilyGenotypeIndex(String study, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/family/genotype/index",
                new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> variantAggregateFamily(String study, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/family/aggregate", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    public DataResponse<Job> variantAggregate(String study, ObjectMap params) throws IOException {
        return execute(OPERATION_URL, "/variant/aggregate", new ObjectMap("body", params).append("study", study), POST, Job.class);
    }

    private ObjectMap copy(ObjectMap params) {
        if (params == null) {
            params = new ObjectMap();
        } else {
            params = new ObjectMap(params);
        }
        return params;
    }

}
