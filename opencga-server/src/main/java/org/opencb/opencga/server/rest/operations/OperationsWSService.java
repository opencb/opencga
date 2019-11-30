package org.opencb.opencga.server.rest.operations;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.metadata.Aggregation;
import org.opencb.opencga.analysis.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.Job;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.server.rest.analysis.RestBodyParams;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotatorFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by imedina on 17/08/16.
 */
@Path("/{apiVersion}/operation")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "Operations", description = "General operations for catalog and storage")
public class OperationsWSService extends OpenCGAWSServer {

    public static final String TASK_NAME = "taskName";
    public static final String TASK_NAME_DESCRIPTION = "Task Name";
    public static final String TASK_DESCRIPTION = "taskDescription";
    public static final String TASK_DESCRIPTION_DESCRIPTION = "Task Description";
    public static final String TASK_TAGS = "taskTags";
    public static final String TASK_TAGS_DESCRIPTION = "Task Tags";
    
    public OperationsWSService(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    public OperationsWSService(String version, @Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders)
            throws VersionException {
        super(version, uriInfo, httpServletRequest, httpHeaders);
    }


    public static class VariantIndexParams extends RestBodyParams {
        public VariantIndexParams() {
        }

        public VariantIndexParams(String file,
                                  boolean resume, String outdir, boolean transform, boolean gvcf,
                                  boolean load, boolean loadSplitData, boolean skipPostLoadCheck,
                                  boolean excludeGenotype, String includeExtraFields, VariantStorageEngine.MergeMode merge,
                                  boolean calculateStats, Aggregation aggregated, String aggregationMappingFile, boolean annotate,
                                  VariantAnnotatorFactory.AnnotationEngine annotator, boolean overwriteAnnotations, boolean indexSearch) {
            this.file = file;
            this.resume = resume;
            this.outdir = outdir;
            this.transform = transform;
            this.gvcf = gvcf;
            this.load = load;
            this.loadSplitData = loadSplitData;
            this.skipPostLoadCheck = skipPostLoadCheck;
            this.excludeGenotype = excludeGenotype;
            this.includeExtraFields = includeExtraFields;
            this.merge = merge;
            this.calculateStats = calculateStats;
            this.aggregated = aggregated;
            this.aggregationMappingFile = aggregationMappingFile;
            this.annotate = annotate;
            this.annotator = annotator;
            this.overwriteAnnotations = overwriteAnnotations;
            this.indexSearch = indexSearch;
        }

        public String file;
        public boolean resume;
        public String outdir;

        public boolean transform;
        public boolean gvcf;

        public boolean load;
        public boolean loadSplitData;
        public boolean skipPostLoadCheck;
        public boolean excludeGenotype;
        public String includeExtraFields = VariantQueryUtils.ALL;
        public VariantStorageEngine.MergeMode merge = VariantStorageOptions.MERGE_MODE.defaultValue();

        public boolean calculateStats;
        public Aggregation aggregated = Aggregation.NONE;
        public String aggregationMappingFile;

        public boolean annotate;
        public VariantAnnotatorFactory.AnnotationEngine annotator;
        public boolean overwriteAnnotations;

        public boolean indexSearch;
    }

    @POST
    @Path("/variant/file/index")
    @ApiOperation(value = "Index variant files into the variant storage", response = Job.class)
    public Response variantFileIndex(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            VariantIndexParams params) {
        return submitTask("variant", "index", study, params, taskName, taskDescription, taskTags);
    }

    @DELETE
    @Path("/variant/file/delete")
    @ApiOperation(value = "Remove variant files from the variant storage", response = Job.class)
    public Response variantFileDelete(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            @ApiParam(value = "Files to remove") @QueryParam("file") String file,
            @ApiParam(value = "Resume a previously failed indexation") @QueryParam("resume") boolean resume) {
        HashMap<String, Object> paramsMap = new HashMap<>();
        paramsMap.put("study", study);
        paramsMap.put("file", file);
        if (resume) {
            paramsMap.put("resume", "");
        }
        return submitTask("variant", "delete", paramsMap, taskName, taskDescription, taskTags);
    }

    public static class VariantSecondaryIndexParams extends RestBodyParams {
        public VariantSecondaryIndexParams() {
        }
        public VariantSecondaryIndexParams(String project, String region, String sample, boolean overwrite) {
            this.project = project;
            this.region = region;
            this.sample = sample;
            this.overwrite = overwrite;
        }
        public String project;
        public String region;
        public String sample;
        public boolean overwrite;
    }

    @POST
    @Path("/variant/secondaryIndex")
    @ApiOperation(value = "Creates a secondary index using a search engine. "
            + "If samples are provided, sample data will be added to the secondary index.")
    public Response secondaryIndex(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            VariantSecondaryIndexParams params) {
        return submitTask("variant", "secondary-index", study, params, taskName, taskDescription, taskTags);
    }

    @DELETE
    @Path("/variant/secondaryIndex/delete")
    @ApiOperation(value = "Remove a secondary index from the search engine for a specific set of samples.")
    public Response secondaryIndex(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            @ApiParam(value = "Samples to remove. Needs to provide all the samples in the secondary index.") @QueryParam("samples") String samples) {
        HashMap<String, Object> params = new HashMap<>();
        params.put("study", study);
        params.put("samples", samples);
        return submitTask("variant", "secondary-index-delete", params, taskName, taskDescription, taskTags);
    }

    public static class VariantAnnotationParams extends RestBodyParams {
        public VariantAnnotationParams() {
        }

        public VariantAnnotationParams(String outdir, VariantAnnotatorFactory.AnnotationEngine annotator,
                                       boolean overwriteAnnotations, String region, boolean create, String load, String customName) {
            this.outdir = outdir;
            this.annotator = annotator;
            this.overwriteAnnotations = overwriteAnnotations;
            this.region = region;
            this.create = create;
            this.load = load;
            this.customName = customName;
        }
        public String outdir;
        public VariantAnnotatorFactory.AnnotationEngine annotator;
        public boolean overwriteAnnotations;
        public String region;
        public boolean create;
        public String load;
        public String customName;
    }

    @POST
    @Path("/variant/annotation/index")
    @ApiOperation(value = "Create and load variant annotations into the database", response = Job.class)
    public Response annotation(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            VariantAnnotationParams params) {
        return submitTask("variant", "annotation-index", params, taskName, taskDescription, taskTags);
    }

    @DELETE
    @Path("/variant/annotation/delete")
    @ApiOperation(value = "Deletes a saved copy of variant annotation")
    public Response annotationDelete(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = VariantCatalogQueryUtils.PROJECT_DESC) @QueryParam("project") String project,
            @ApiParam(value = "Annotation identifier") @QueryParam("annotationId") String annotationId
    ) {
        Map<String, Object> params = new HashMap<>();
        params.put("project", project);
        params.put("annotationId", annotationId);
        return submitTask("variant", "annotation-delete", params, taskName, taskDescription, taskTags);
    }

    public static class VariantAnnotationSaveParams extends RestBodyParams {
        public VariantAnnotationSaveParams() {
        }

        public VariantAnnotationSaveParams(String project, String annotationId) {
            this.project = project;
            this.annotationId = annotationId;
        }

        public String project;
        public String annotationId;
    }

    @POST
    @Path("/variant/annotation/save")
    @ApiOperation(value = "Save a copy of the current variant annotation at the database")
    public Response annotationSave(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            VariantAnnotationSaveParams params) {
        return submitTask("variant", "annotation-save", params, taskName, taskDescription, taskTags);
    }

    public static class VariantScoreIndexParams extends RestBodyParams {
        public VariantScoreIndexParams() {
        }
        public VariantScoreIndexParams(String cohort1, String cohort2, String input, String inputColumns,
                                       boolean resume) {
            this.cohort1 = cohort1;
            this.cohort2 = cohort2;
            this.input = input;
            this.inputColumns = inputColumns;
            this.resume = resume;
        }

        public String cohort1;
        public String cohort2;
        public String input;
        public String inputColumns;
        public boolean resume;
    }

    @POST
    @Path("/variant/score/index")
    @ApiOperation(value = "Index a variant score in the database.")
    public Response scoreIndex(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            VariantScoreIndexParams params) {
        return submitTask("variant", "score-index", study, params, taskName, taskDescription, taskTags);
    }

    @DELETE
    @Path("/variant/score/delete")
    @ApiOperation(value = "Remove a variant score in the database.")
    public Response scoreDelete(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            @ApiParam(value = "Unique name of the score within the study") @QueryParam("name") String name,
            @ApiParam(value = "Resume a previously failed remove") @QueryParam("resume") boolean resume,
            @ApiParam(value = "Force remove of partially indexed scores") @QueryParam("force") boolean force
    ) {
        Map<String, Object> params = new HashMap<>();
        params.put("study", study);
        params.put("name", name);
        if (resume) params.put("resume", "");
        if (force) params.put("force", "");
        return submitTask("variant", "score-delete", params, taskName, taskDescription, taskTags);
    }

    public static class VariantSampleIndexParams extends RestBodyParams {
        public VariantSampleIndexParams() {
        }
        public VariantSampleIndexParams(List<String> sample) {
            this.sample = sample;
        }
        public List<String> sample;
    }

    @POST
    @Path("/variant/sample/genotype/index")
    @ApiOperation(value = "Build and annotate the sample index.", response = Job.class)
    public Response sampleIndex(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            VariantSampleIndexParams params) {
        return submitTask("variant", "sample-index", study, params, taskName, taskDescription, taskTags);
    }

    public static class VariantFamilyIndexParams extends RestBodyParams {
        public VariantFamilyIndexParams() {
        }
        public VariantFamilyIndexParams(List<String> family, boolean overwrite) {
            this.family = family;
            this.overwrite = overwrite;
        }
        public List<String> family;
        public boolean overwrite;
    }

    @POST
    @Path("/variant/family/genotype/index")
    @ApiOperation(value = "Build the family index.", response = Job.class)
    public Response familyIndex(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            VariantFamilyIndexParams params) {
        return submitTask("variant", "family-index", study, params, taskName, taskDescription, taskTags);
    }

    public static class VariantAggregateFamilyParams extends RestBodyParams {
        public VariantAggregateFamilyParams() {
        }
        public VariantAggregateFamilyParams(boolean resume, List<String> samples) {
            this.resume = resume;
            this.samples = samples;
        }
        public boolean resume;
        public List<String> samples;
    }

    @POST
    @Path("/variant/family/aggregate")
    @ApiOperation(value = "Find variants where not all the samples are present, and fill the empty values.")
    public Response aggregateFamily(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            VariantAggregateFamilyParams params) {
        return submitTask("variant", "aggregate-family", study, params, taskName, taskDescription, taskTags);
    }

    public static class VariantAggregateParams extends RestBodyParams {
        public VariantAggregateParams() {
        }
        public VariantAggregateParams(String region, boolean overwrite, boolean resume) {
            this.region = region;
            this.overwrite = overwrite;
            this.resume = resume;
        }
        public String region;
        public boolean overwrite;
        public boolean resume;
    }

    @POST
    @Path("/variant/aggregate")
    @ApiOperation(value = "Find variants where not all the samples are present, and fill the empty values, excluding HOM-REF (0/0) values.")
    public Response aggregate(
            @ApiParam(value = TASK_NAME_DESCRIPTION) @QueryParam(TASK_NAME) String taskName,
            @ApiParam(value = TASK_DESCRIPTION_DESCRIPTION) @QueryParam(TASK_DESCRIPTION) String taskDescription,
            @ApiParam(value = TASK_TAGS_DESCRIPTION) @QueryParam(TASK_TAGS) String taskTags,
            @ApiParam(value = STUDY_PARAM_DESCRIPTION) @QueryParam(STUDY_PARAM) String study,
            VariantAggregateParams params) {
        return submitTask("variant", "aggregate", study, params, taskName, taskDescription, taskTags);
    }

    public Response submitTask(String command, String subcommand, RestBodyParams params,
                               String taskName, String taskDescription, String taskTags) {
        return submitTask(command, subcommand, null, params, taskName, taskDescription, taskTags);
    }

    public Response submitTask(String command, String subcommand, String study, RestBodyParams params,
                               String taskName, String taskDescription, String taskTags) {
        try {
            Map<String, Object> paramsMap = params.toParams();
            if (StringUtils.isNotEmpty(study)) {
                paramsMap.put("study", study);
            }
            return submitTask(command, subcommand, paramsMap, taskName, taskDescription, taskTags);
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    public Response submitTask(String command, String subcommand, Map<String, Object> paramsMap,
                               String taskName, String taskDescription, String taskTags) {

        Map<String, String> dynamicParamsMap = new HashMap<>();
        for (String key : this.params.keySet()) {
            String prefix = "dynamic_";
            if (key.startsWith(prefix)) {
                dynamicParamsMap.put(key.replace(prefix, ""), this.params.getString(key));
            }
        }
        if (dynamicParamsMap.size() > 0) {
            paramsMap.put("dynamicParams", dynamicParamsMap);
        }
        String study = (String) paramsMap.get("study");
        // FIXME
        //  This should actually submit a TASK, not a JOB
        if (StringUtils.isNotEmpty(study)) {
            return submitJob(study, command, subcommand, paramsMap, null, taskName, taskDescription, taskTags);
        } else {
            return createPendingResponse();
        }
    }
}