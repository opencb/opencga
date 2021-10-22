package org.opencb.opencga.catalog.managers;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.PipelineDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.audit.AuditRecord;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.job.Pipeline;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.ToolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

public class PipelineManager extends ResourceManager<Pipeline> {

    protected static Logger logger = LoggerFactory.getLogger(PipelineManager.class);
    private final UserManager userManager;
    private final StudyManager studyManager;

    public static final Pattern PIPELINE_VARIABLE_PATTERN = Pattern.compile("(\\$\\{PIPELINE.([^$]+)\\})");

//    private static Set<String> toolsCache;

    PipelineManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.PIPELINE;
    }

    @Override
    InternalGetDataResult<Pipeline> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                                String user, boolean ignoreException) throws CatalogException {
        if (CollectionUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing pipeline entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = new QueryOptions(ParamUtils.defaultObject(options, QueryOptions::new));

        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.put(PipelineDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        boolean versioned = queryCopy.getBoolean(Constants.ALL_VERSIONS)
                || queryCopy.containsKey(PipelineDBAdaptor.QueryParams.VERSION.key());
        if (versioned && uniqueList.size() > 1) {
            throw new CatalogException("Only one pipeline allowed when requesting multiple versions");
        }

        PipelineDBAdaptor.QueryParams idQueryParam = getFieldFilter(uniqueList);
        queryCopy.put(idQueryParam.key(), uniqueList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<Pipeline> pipelineOpenCGAResult = pipelineDBAdaptor.get(studyUid, queryCopy, queryOptions, user);

        Function<Pipeline, String> pipelineStringFunction = Pipeline::getId;
        if (idQueryParam.equals(PipelineDBAdaptor.QueryParams.UUID)) {
            pipelineStringFunction = Pipeline::getUuid;
        }

        if (ignoreException || pipelineOpenCGAResult.getNumResults() >= uniqueList.size()) {
            return keepOriginalOrder(uniqueList, pipelineStringFunction, pipelineOpenCGAResult, ignoreException, versioned);
        }
        // Query without adding the user check
        OpenCGAResult<Pipeline> resultsNoCheck = pipelineDBAdaptor.get(queryCopy, queryOptions);

        if (resultsNoCheck.getNumResults() == pipelineOpenCGAResult.getNumResults()) {
            throw CatalogException.notFound("pipelines",
                    getMissingFields(uniqueList, pipelineOpenCGAResult.getResults(), pipelineStringFunction));
        } else {
            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the pipelines.");
        }
    }

    PipelineDBAdaptor.QueryParams getFieldFilter(List<String> idList) throws CatalogException {
        PipelineDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : idList) {
            PipelineDBAdaptor.QueryParams param = PipelineDBAdaptor.QueryParams.ID;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                param = PipelineDBAdaptor.QueryParams.UUID;
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and ids in the same query. Please, choose one or do two different queries.");
            }
        }
        return idQueryParam;
    }

    @Override
    public OpenCGAResult<Pipeline> create(String studyStr, Pipeline pipeline, QueryOptions options, String token) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("pipeline", pipeline)
                .append("options", options)
                .append("token", token);
        try {
            // 1. We check everything can be done
            authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);

            // 2. Process dynamic variables
            try {
                String pipelineJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(pipeline);
                ObjectMap pipelineMap = JacksonUtils.getDefaultObjectMapper().readValue(pipelineJsonString, ObjectMap.class);
                ParamUtils.processDynamicVariables(pipelineMap, PIPELINE_VARIABLE_PATTERN);
                pipelineJsonString = JacksonUtils.getDefaultObjectMapper().writeValueAsString(pipelineMap);
                pipeline = JacksonUtils.getDefaultObjectMapper().readValue(pipelineJsonString, Pipeline.class);
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not process JSON properly", e);
            }

            validate(pipeline);
            validateForCreation(study, pipeline);

            // We create the pipeline
            pipelineDBAdaptor.insert(study.getUid(), pipeline, options);
            OpenCGAResult<Pipeline> queryResult = getPipeline(study.getUid(), pipeline.getUuid(), options);
            auditManager.auditCreate(userId, Enums.Resource.PIPELINE, pipeline.getId(), pipeline.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.PIPELINE, pipeline.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public static void validate(Pipeline pipeline) throws CatalogException {
        ParamUtils.checkObj(pipeline, "Pipeline");
        ParamUtils.checkParameter(pipeline.getId(), "id");
        if (pipeline.getJobs() == null) {
            throw new CatalogException("Pipeline does not have any jobs");
        }

        pipeline.setDescription(ParamUtils.defaultString(pipeline.getDescription(), ""));
        pipeline.setCreationDate(ParamUtils.checkDateOrGetCurrentDate(pipeline.getCreationDate(),
                PipelineDBAdaptor.QueryParams.CREATION_DATE.key()));
        pipeline.setModificationDate(ParamUtils.checkDateOrGetCurrentDate(pipeline.getModificationDate(),
                PipelineDBAdaptor.QueryParams.MODIFICATION_DATE.key()));
        pipeline.setParams(ParamUtils.defaultObject(pipeline.getParams(), Collections::emptyMap));
        pipeline.setConfig(ParamUtils.defaultObject(pipeline.getConfig(), Pipeline.PipelineConfig::init));

        Set<String> jobIds = new HashSet<>();
        for (Map.Entry<String, Pipeline.PipelineJob> entry : pipeline.getJobs().entrySet()) {
            String jobId = entry.getKey();
            Pipeline.PipelineJob job = entry.getValue();
            job.setToolId(ParamUtils.defaultString(job.getToolId(), ""));
            job.setName(ParamUtils.defaultString(job.getName(), ""));
            job.setDescription(ParamUtils.defaultString(job.getDescription(), ""));
            job.setParams(ParamUtils.defaultObject(job.getParams(), Collections::emptyMap));
            job.setTags(ParamUtils.defaultObject(job.getTags(), Collections::emptyList));
            job.setDependsOn(ParamUtils.defaultObject(job.getDependsOn(), Collections::emptyList));
            for (String tmpJobId : job.getDependsOn()) {
                if (!jobIds.contains(tmpJobId)) {
                    throw new CatalogException("Job '" + jobId + "' depends on job '" + tmpJobId + "'. '" + tmpJobId + "' is either"
                            + " undefined or not defined before job '" + jobId + "'.");
                }
            }

            if (StringUtils.isNotEmpty(job.getToolId())) {
                try {
                    new ToolFactory().getToolClass(job.getToolId());
                } catch (ToolException e) {
                    throw new CatalogException("Tool '" + job.getToolId() + "' from Pipeline does not exist.", e);
                }
            }
            jobIds.add(jobId);
        }
    }

//    private static void checkToolExists(String toolId) {
//        if (toolsCache == null) {
//            Reflections reflections = new Reflections(new ConfigurationBuilder()
//                    .setScanners(
//                            new SubTypesScanner(),
//                            new TypeAnnotationsScanner().filterResultsBy(s -> StringUtils.equals(s, Tool.class.getName()))
//                    )
//                    .addUrls(getUrls())
//                    .filterInputsBy(input -> input != null && input.endsWith(".class"))
//            );
//
//            reflections.getAllTypes()
//        }
//        return toolsCache.contains(toolId);
//    }
//
//    /**
//     * Code extracted from ToolFactory class
//     *
//     * @return urls to be checked.
//     */
//    private static Collection<URL> getUrls() {
//        // TODO: What if there are third party libraries that implement Tools?
//        //  Currently they must contain "opencga" in the jar name.
//        //  e.g.  acme-rockets-opencga-5.4.0.jar
//        Collection<URL> urls = new LinkedList<>();
//        for (URL url : ClasspathHelper.forClassLoader()) {
//            String name = url.getPath().substring(url.getPath().lastIndexOf('/') + 1);
//            if (name.isEmpty() || (name.contains("opencga") && !name.contains("opencga-storage-hadoop-deps"))) {
//                urls.add(url);
//            }
//        }
//        return urls;
//    }

    private void validateForCreation(Study study, Pipeline pipeline) throws CatalogException {
        ParamUtils.checkIdentifier(pipeline.getId(), "id");

        // Check the id is not in use
        Query query = new Query()
                .append(PipelineDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(PipelineDBAdaptor.QueryParams.ID.key(), pipeline.getId());
        if (pipelineDBAdaptor.count(query).getNumMatches() > 0) {
            throw new CatalogException("Pipeline '" + pipeline.getId() + "' already exists.");
        }

        pipeline.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.PIPELINE));
        pipeline.setVersion(1);
    }

    @Override
    public DBIterator<Pipeline> iterator(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult<Pipeline> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            fixQueryObject(study, query, userId);

            query.append(PipelineDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Pipeline> queryResult = pipelineDBAdaptor.get(study.getUid(), query, options, userId);

            auditManager.auditSearch(userId, Enums.Resource.PIPELINE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.PIPELINE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult<Pipeline> count(String studyId, Query query, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> ids, QueryOptions options, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, QueryOptions options, String token) throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String token)
            throws CatalogException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String token)
            throws CatalogException {
        return null;
    }

    void fixQueryObject(Study study, Query query, String userId) throws CatalogException {

    }

    private OpenCGAResult<Pipeline> getPipeline(long studyUid, String pipelineUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(PipelineDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(PipelineDBAdaptor.QueryParams.UUID.key(), pipelineUuid);
        return pipelineDBAdaptor.get(query, options);
    }
}
