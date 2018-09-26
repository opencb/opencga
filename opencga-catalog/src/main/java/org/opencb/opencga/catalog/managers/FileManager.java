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
package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.datastore.core.result.FacetedQueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.monitor.daemons.IndexDaemon;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.*;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.HookConfiguration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.utils.FileMetadataReader.VARIANT_FILE_STATS;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileManager extends AnnotationSetManager<File> {

    private static final QueryOptions INCLUDE_STUDY_URI;
    private static final QueryOptions INCLUDE_FILE_URI_PATH;
    private static final Comparator<File> ROOT_FIRST_COMPARATOR;
    private static final Comparator<File> ROOT_LAST_COMPARATOR;

    protected static Logger logger;
    private FileMetadataReader file;
    private UserManager userManager;

    public static final String SKIP_TRASH = "SKIP_TRASH";
    public static final String DELETE_EXTERNAL_FILES = "DELETE_EXTERNAL_FILES";
    public static final String FORCE_DELETE = "FORCE_DELETE";

    public static final String GET_NON_DELETED_FILES = Status.READY + "," + File.FileStatus.TRASHED + "," + File.FileStatus.STAGE + ","
            + File.FileStatus.MISSING;
    public static final String GET_NON_TRASHED_FILES = Status.READY + "," + File.FileStatus.STAGE + "," + File.FileStatus.MISSING;

    private final String defaultFacet = "creationYear>>creationMonth;format;bioformat;format>>bioformat;status";
    private final String defaultFacetRange = "size:0:214748364800:10737418240;numSamples:0:10:1";

    static {
        INCLUDE_STUDY_URI = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key());
        INCLUDE_FILE_URI_PATH = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.PATH.key(),
                        FileDBAdaptor.QueryParams.EXTERNAL.key()));
        ROOT_FIRST_COMPARATOR = (f1, f2) -> (f1.getPath() == null ? 0 : f1.getPath().length())
                - (f2.getPath() == null ? 0 : f2.getPath().length());
        ROOT_LAST_COMPARATOR = (f1, f2) -> (f2.getPath() == null ? 0 : f2.getPath().length())
                - (f1.getPath() == null ? 0 : f1.getPath().length());

        logger = LoggerFactory.getLogger(FileManager.class);
    }

    FileManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
        file = new FileMetadataReader(this.catalogManager);
        this.userManager = catalogManager.getUserManager();
    }

    public URI getUri(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            QueryResult<File> fileQueryResult = fileDBAdaptor.get(file.getUid(), INCLUDE_STUDY_URI);
            if (fileQueryResult.getNumResults() == 0) {
                throw new CatalogException("File " + file.getUid() + " not found");
            }
            return fileQueryResult.first().getUri();
        }
    }

    @Deprecated
    public URI getUri(long studyId, String filePath) throws CatalogException {
        ParamUtils.checkObj(filePath, "filePath");

        List<File> parents = getParents(false, INCLUDE_FILE_URI_PATH, filePath, studyId).getResult();

        for (File parent : parents) {
            if (parent.getUri() != null) {
                if (parent.isExternal()) {
                    throw new CatalogException("Cannot upload files to an external folder");
                }
                String relativePath = filePath.replaceFirst(parent.getPath(), "");
                return Paths.get(parent.getUri()).resolve(relativePath).toUri();
            }
        }
        URI studyUri = getStudyUri(studyId);
        return filePath.isEmpty()
                ? studyUri
                : catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, filePath);
    }

    public Study getStudy(File file, String sessionId) throws CatalogException {
        ParamUtils.checkObj(file, "file");
        ParamUtils.checkObj(sessionId, "session id");

        if (file.getStudyUid() <= 0) {
            throw new CatalogException("Missing study uid field in file");
        }

        String user = catalogManager.getUserManager().getUserId(sessionId);

        Query query = new Query(StudyDBAdaptor.QueryParams.UID.key(), file.getStudyUid());
        QueryResult<Study> studyQueryResult = studyDBAdaptor.get(query, QueryOptions.empty(), user);
        if (studyQueryResult.getNumResults() == 1) {
            return studyQueryResult.first();
        } else {
            authorizationManager.checkCanViewStudy(file.getStudyUid(), user);
            throw new CatalogException("Incorrect study uid");
        }
    }

    public void matchUpVariantFiles(String studyStr, List<File> transformedFiles, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        for (File transformedFile : transformedFiles) {
            authorizationManager.checkFilePermission(study.getUid(), transformedFile.getUid(), userId, FileAclEntry.FilePermissions.WRITE);
            String variantPathName = getOriginalFile(transformedFile.getPath());
            if (variantPathName == null) {
                // Skip the file.
                logger.warn("The file {} is not a variant transformed file", transformedFile.getName());
                continue;
            }

            // Search in the same path
            logger.info("Looking for vcf file in path {}", variantPathName);
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), variantPathName)
                    .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);

            List<File> fileList = fileDBAdaptor.get(query, new QueryOptions()).getResult();

            if (fileList.isEmpty()) {
                // Search by name in the whole study
                String variantFileName = getOriginalFile(transformedFile.getName());
                logger.info("Looking for vcf file by name {}", variantFileName);
                query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(FileDBAdaptor.QueryParams.NAME.key(), variantFileName)
                        .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);
                fileList = new ArrayList<>(fileDBAdaptor.get(query, new QueryOptions()).getResult());

                // In case of finding more than one file, try to find the proper one.
                if (fileList.size() > 1) {
                    // Discard files already with a transformed file.
                    fileList.removeIf(file -> file.getIndex() != null
                            && file.getIndex().getTransformedFile() != null
                            && file.getIndex().getTransformedFile().getId() != transformedFile.getUid());
                }
                if (fileList.size() > 1) {
                    // Discard files not transformed or indexed.
                    fileList.removeIf(file -> file.getIndex() == null
                            || file.getIndex().getStatus() == null
                            || file.getIndex().getStatus().getName() == null
                            || file.getIndex().getStatus().getName().equals(FileIndex.IndexStatus.NONE));
                }
            }


            if (fileList.size() != 1) {
                // VCF file not found
                logger.warn("The vcf file corresponding to the file " + transformedFile.getName() + " could not be found");
                continue;
            }
            File vcf = fileList.get(0);

            // Look for the json file. It should be in the same directory where the transformed file is.
            String jsonPathName = getMetaFile(transformedFile.getPath());
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), jsonPathName)
                    .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON);
            fileList = fileDBAdaptor.get(query, new QueryOptions()).getResult();
            if (fileList.size() != 1) {
                // Skip. This should not ever happen
                logger.warn("The json file corresponding to the file " + transformedFile.getName() + " could not be found");
                continue;
            }
            File json = fileList.get(0);

            /* Update relations */

            File.RelatedFile producedFromRelation = new File.RelatedFile(vcf.getUid(), File.RelatedFile.Relation.PRODUCED_FROM);

            // Update json file
            logger.debug("Updating json relation");
            List<File.RelatedFile> relatedFiles = ParamUtils.defaultObject(json.getRelatedFiles(), ArrayList::new);
            // Do not add twice the same relation
            if (!relatedFiles.contains(producedFromRelation)) {
                relatedFiles.add(producedFromRelation);
                ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFiles);
                fileDBAdaptor.update(json.getUid(), params, QueryOptions.empty());
            }

            // Update transformed file
            logger.debug("Updating transformed relation");
            relatedFiles = ParamUtils.defaultObject(transformedFile.getRelatedFiles(), ArrayList::new);
            // Do not add twice the same relation
            if (!relatedFiles.contains(producedFromRelation)) {
                relatedFiles.add(producedFromRelation);
                transformedFile.setRelatedFiles(relatedFiles);
                ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFiles);
                fileDBAdaptor.update(transformedFile.getUid(), params, QueryOptions.empty());
            }

            // Update vcf file
            logger.debug("Updating vcf relation");
            FileIndex index = vcf.getIndex();
            if (index.getTransformedFile() == null) {
                index.setTransformedFile(new FileIndex.TransformedFile(transformedFile.getUid(), json.getUid()));
            }
            String status = FileIndex.IndexStatus.NONE;
            if (vcf.getIndex() != null && vcf.getIndex().getStatus() != null && vcf.getIndex().getStatus().getName() != null) {
                status = vcf.getIndex().getStatus().getName();
            }
            if (FileIndex.IndexStatus.NONE.equals(status)) {
                // If TRANSFORMED, TRANSFORMING, etc, do not modify the index status
                index.setStatus(new FileIndex.IndexStatus(FileIndex.IndexStatus.TRANSFORMED, "Found transformed file"));
            }
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
            fileDBAdaptor.update(vcf.getUid(), params, QueryOptions.empty());

            // Update variant stats
            Path statsFile = Paths.get(json.getUri().getRawPath());
            try (InputStream is = FileUtils.newInputStream(statsFile)) {
                VariantFileMetadata fileMetadata = getDefaultObjectMapper().readValue(is, VariantFileMetadata.class);
                VariantSetStats stats = fileMetadata.getStats();
                params = new ObjectMap(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(VARIANT_FILE_STATS, stats));
                update(studyStr, vcf.getPath(), params, new QueryOptions(), sessionId);
            } catch (IOException e) {
                throw new CatalogException("Error reading file \"" + statsFile + "\"", e);
            }
        }
    }

    public void setStatus(String studyStr, String fileId, String status, String message, String sessionId) throws CatalogException {
        MyResource<File> resource = getUid(fileId, studyStr, sessionId);

        String userId = resource.getUser();
        long fileUid = resource.getResource().getUid();

        authorizationManager.checkFilePermission(resource.getStudy().getUid(), fileUid, userId, FileAclEntry.FilePermissions.WRITE);

        if (status != null && !File.FileStatus.isValid(status)) {
            throw new CatalogException("The status " + status + " is not valid file status.");
        }

        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(FileDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(FileDBAdaptor.QueryParams.STATUS_MSG.key(), message);

        fileDBAdaptor.update(fileUid, parameters, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.file, fileUid, userId, parameters, null, null);
    }

    public QueryResult<FileIndex> updateFileIndexStatus(File file, String newStatus, String message, String sessionId)
            throws CatalogException {
        return updateFileIndexStatus(file, newStatus, message, null, sessionId);
    }

    public QueryResult<FileIndex> updateFileIndexStatus(File file, String newStatus, String message, Integer release, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Long studyId = file.getStudyUid();
        authorizationManager.checkFilePermission(studyId, file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        FileIndex index = file.getIndex();
        if (index != null) {
            if (!FileIndex.IndexStatus.isValid(newStatus)) {
                throw new CatalogException("The status " + newStatus + " is not a valid status.");
            } else {
                index.setStatus(new FileIndex.IndexStatus(newStatus, message));
            }
        } else {
            index = new FileIndex(userId, TimeUtils.getTime(), new FileIndex.IndexStatus(newStatus), -1, new ObjectMap());
        }
        if (release != null) {
            if (newStatus.equals(FileIndex.IndexStatus.READY)) {
                index.setRelease(release);
            }
        }
        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
        fileDBAdaptor.update(file.getUid(), params, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.file, file.getUid(), userId, params, null, null);

        return new QueryResult<>("Update file index", 0, 1, 1, "", "", Arrays.asList(index));
    }

    @Deprecated
    public QueryResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.STUDY_UID.key())));

        if (fileQueryResult.getNumResults() == 0) {
            return fileQueryResult;
        }

        String userId = userManager.getUserId(sessionId);
        authorizationManager.checkFilePermission(fileQueryResult.first().getStudyUid(), fileId, userId, FileAclEntry.FilePermissions.VIEW);

        return getParents(true, options, fileQueryResult.first().getPath(), fileQueryResult.first().getStudyUid());
    }

    public QueryResult<File> createFolder(String studyStr, String path, File.FileStatus status, boolean parents, String description,
                                          QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkPath(path, "folderPath");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (!path.endsWith("/")) {
            path = path + "/";
        }

        QueryResult<File> fileQueryResult;
        switch (checkPathExists(path, study.getUid())) {
            case FREE_PATH:
                fileQueryResult = create(studyStr, File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, path, null,
                        description, status, 0, -1, null, -1, null, null, parents, null, options, sessionId);
                break;
            case DIRECTORY_EXISTS:
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(FileDBAdaptor.QueryParams.PATH.key(), path);
                fileQueryResult = fileDBAdaptor.get(query, options, userId);
                fileQueryResult.setWarningMsg("Folder was already created");
                break;
            case FILE_EXISTS:
            default:
                throw new CatalogException("A file with the same name of the folder already exists in Catalog");
        }

        fileQueryResult.setId("Create folder");
        return fileQueryResult;
    }

    public QueryResult<File> createFile(String studyStr, String path, String description, boolean parents, String content,
                                        String sessionId) throws CatalogException {
        ParamUtils.checkPath(path, "filePath");

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        switch (checkPathExists(path, study.getUid())) {
            case FREE_PATH:
                return create(studyStr, File.Type.FILE, File.Format.PLAIN, File.Bioformat.UNKNOWN, path, null, description,
                        new File.FileStatus(File.FileStatus.READY), 0, -1, null, -1, null, null, parents, content, new QueryOptions(),
                        sessionId);
            case FILE_EXISTS:
            case DIRECTORY_EXISTS:
            default:
                throw new CatalogException("A file or folder with the same name already exists in the path of Catalog");
        }
    }

    public QueryResult<File> create(String studyStr, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                    String creationDate, String description, File.FileStatus status, long size, long experimentId,
                                    List<Sample> samples, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                                    boolean parents, String content, QueryOptions options, String sessionId)
            throws CatalogException {
        File file = new File(type, format, bioformat, path, description, status, size, samples, jobId, null, stats, attributes);
        return create(studyStr, file, parents, content, options, sessionId);
    }

    @Override
    public QueryResult<File> create(String studyStr, File entry, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Call to create passing parents and content variables");
    }

    public QueryResult<File> create(String studyStr, File file, boolean parents, String content, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        return create(study, file, parents, content, options, sessionId);
    }

    public QueryResult<File> register(Study study, File file, boolean parents, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = study.getUid();

        /** Check and set all the params and create a File object **/
        ParamUtils.checkObj(file, "File");
        ParamUtils.checkPath(file.getPath(), "path");
        file.setType(ParamUtils.defaultObject(file.getType(), File.Type.FILE));
        file.setFormat(ParamUtils.defaultObject(file.getFormat(), File.Format.PLAIN));
        file.setBioformat(ParamUtils.defaultObject(file.getBioformat(), File.Bioformat.NONE));
        file.setDescription(ParamUtils.defaultString(file.getDescription(), ""));
        file.setRelatedFiles(ParamUtils.defaultObject(file.getRelatedFiles(), ArrayList::new));
        file.setCreationDate(TimeUtils.getTime());
        file.setModificationDate(file.getCreationDate());
        if (file.getType() == File.Type.FILE) {
            file.setStatus(ParamUtils.defaultObject(file.getStatus(), new File.FileStatus(File.FileStatus.STAGE)));
        } else {
            file.setStatus(ParamUtils.defaultObject(file.getStatus(), new File.FileStatus(File.FileStatus.READY)));
        }
        if (file.getSize() < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }
//        if (file.getExperiment().getId() > 0 && !jobDBAdaptor.experimentExists(file.getExperiment().getId())) {
//            throw new CatalogException("Experiment { id: " + file.getExperiment().getId() + "} does not exist.");
//        }

        file.setSamples(ParamUtils.defaultObject(file.getSamples(), ArrayList::new));
        for (Sample sample : file.getSamples()) {
            if (sample.getUid() <= 0 || !sampleDBAdaptor.exists(sample.getUid())) {
                throw new CatalogException("Sample { id: " + sample.getUid() + "} does not exist.");
            }
        }
        if (file.getJob() != null && file.getJob().getUid() > 0 && !jobDBAdaptor.exists(file.getJob().getUid())) {
            throw new CatalogException("Job { id: " + file.getJob().getUid() + "} does not exist.");
        }
        file.setStats(ParamUtils.defaultObject(file.getStats(), HashMap::new));
        file.setAttributes(ParamUtils.defaultObject(file.getAttributes(), HashMap::new));

        if (file.getType() == File.Type.DIRECTORY && !file.getPath().endsWith("/")) {
            file.setPath(file.getPath() + "/");
        }
        if (file.getType() == File.Type.FILE && file.getPath().endsWith("/")) {
            file.setPath(file.getPath().substring(0, file.getPath().length() - 1));
        }
        file.setName(Paths.get(file.getPath()).getFileName().toString());

        URI uri;
        try {
            if (file.getType() == File.Type.DIRECTORY) {
                uri = getFileUri(studyId, file.getPath(), true);
            } else {
                uri = getFileUri(studyId, file.getPath(), false);
            }
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }
        file.setUri(uri);

        // FIXME: Why am I doing this? Why am I not throwing an exception if it already exists?
        // Check if it already exists
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";" + File.FileStatus.DELETED
                        + ";" + File.FileStatus.DELETING + ";" + File.FileStatus.PENDING_DELETE + ";" + File.FileStatus.REMOVED);
        if (fileDBAdaptor.count(query).first() > 0) {
            logger.warn("The file {} already exists in catalog", file.getPath());
        }
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.URI.key(), uri)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";" + File.FileStatus.DELETED
                        + ";" + File.FileStatus.DELETING + ";" + File.FileStatus.PENDING_DELETE + ";" + File.FileStatus.REMOVED);
        if (fileDBAdaptor.count(query).first() > 0) {
            logger.warn("The uri {} of the file is already in catalog but on a different path", uri);
        }

        boolean external = isExternal(study, file.getPath(), uri);
        file.setExternal(external);
        file.setRelease(catalogManager.getStudyManager().getCurrentRelease(study, userId));

        //Find parent. If parents == true, create folders.
        String parentPath = getParentPath(file.getPath());

        long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
        boolean newParent = false;
        if (parentFileId < 0 && StringUtils.isNotEmpty(parentPath)) {
            if (parents) {
                newParent = true;
                File parentFile = new File(File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, parentPath, "",
                        new File.FileStatus(File.FileStatus.READY), 0, file.getSamples(), -1, null, Collections.emptyMap(),
                        Collections.emptyMap());
                parentFileId = register(study, parentFile, parents, options, sessionId).first().getUid();
            } else {
                throw new CatalogDBException("Directory not found " + parentPath);
            }
        }

        //Check permissions
        if (parentFileId < 0) {
            throw new CatalogException("Unable to create file without a parent file");
        } else {
            if (!newParent) {
                //If parent has been created, for sure we have permissions to create the new file.
                authorizationManager.checkFilePermission(studyId, parentFileId, userId, FileAclEntry.FilePermissions.WRITE);
            }
        }

        List<VariableSet> variableSetList = validateNewAnnotationSetsAndExtractVariableSets(study.getUid(), file.getAnnotationSets());

        file.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FILE));
        checkHooks(file, study.getFqn(), HookConfiguration.Stage.CREATE);
        QueryResult<File> queryResult = fileDBAdaptor.insert(studyId, file, variableSetList, options);
        // We obtain the permissions set in the parent folder and set them to the file or folder being created
        QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(studyId, parentFileId, userId, false);
        // Propagate ACLs
        if (allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getUid()), allFileAcls.getResult(), Entity.FILE);
        }

        auditManager.recordCreation(AuditRecord.Resource.file, queryResult.first().getUid(), userId, queryResult.first(), null, null);
        matchUpVariantFiles(study.getFqn(), queryResult.getResult(), sessionId);

        return queryResult;
    }

    private QueryResult<File> create(Study study, File file, boolean parents, String content, QueryOptions options, String sessionId)
            throws CatalogException {
        QueryResult<File> queryResult = register(study, file, parents, options, sessionId);

        if (file.getType() == File.Type.FILE && StringUtils.isNotEmpty(content)) {
            CatalogIOManager ioManager = catalogIOManagerFactory.getDefault();
            // We set parents to true because the file has been successfully registered, which means the directories are already registered
            // in catalog
            ioManager.createDirectory(Paths.get(file.getUri()).getParent().toUri(), true);
            InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
            ioManager.createFile(file.getUri(), inputStream);

            // Update file parameters
            ObjectMap params = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY)
                    .append(FileDBAdaptor.QueryParams.SIZE.key(), ioManager.getFileSize(file.getUri()));
            queryResult = fileDBAdaptor.update(file.getUid(), params, QueryOptions.empty());
        }

        return queryResult;
    }

    /**
     * Upload a file in Catalog.
     *
     * @param studyStr study where the file will be uploaded.
     * @param fileInputStream Input stream of the file to be uploaded.
     * @param file File object containing at least the basic metada necessary for a successful upload: path
     * @param overwrite Overwrite the current file if any.
     * @param parents boolean indicating whether unexisting parent folders should also be created automatically.
     * @param sessionId session id of the user performing the upload.
     * @return a QueryResult with the file uploaded.
     * @throws CatalogException if the user does not have permissions or any other unexpected issue happens.
     */
    public QueryResult<File> upload(String studyStr, InputStream fileInputStream, File file, boolean overwrite, boolean parents,
                                    String sessionId) throws CatalogException {
        // Check basic parameters
        ParamUtils.checkObj(file, "file");
        ParamUtils.checkParameter(file.getPath(), FileDBAdaptor.QueryParams.PATH.key());

        if (StringUtils.isEmpty(file.getName())) {
            file.setName(Paths.get(file.getPath()).toFile().getName());
        }

        ParamUtils.checkObj(fileInputStream, "file input stream");

        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyStr,
                new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(),
                        StudyDBAdaptor.QueryParams.ATTRIBUTES.key())), sessionId);
        if (studyQueryResult.getNumResults() == 0) {
            throw new CatalogException("Study " + studyStr + " not found");
        }
        Study study = studyQueryResult.first();

        QueryResult<File> parentFolders = getParents(false, QueryOptions.empty(), file.getPath(), study.getUid());
        if (parentFolders.getNumResults() == 0) {
            // There always must be at least the root folder
            throw new CatalogException("Unexpected error happened.");
        }

        String userId = userManager.getUserId(sessionId);

        // Check permissions over the most internal path
        authorizationManager.checkFilePermission(study.getUid(), parentFolders.first().getUid(), userId,
                FileAclEntry.FilePermissions.UPLOAD);
        authorizationManager.checkFilePermission(study.getUid(), parentFolders.first().getUid(), userId,
                FileAclEntry.FilePermissions.WRITE);

        // We obtain the basic studyPath where we will upload the file temporarily
        java.nio.file.Path studyPath = Paths.get(study.getUri());
        java.nio.file.Path tempFilePath = studyPath.resolve("tmp_" + file.getName()).resolve(file.getName());
        logger.info("Uploading file... Temporal file path: {}", tempFilePath.toString());

        CatalogIOManager ioManager = catalogManager.getCatalogIOManagerFactory().getDefault();

        // Create the temporal directory and upload the file
        try {
            if (!Files.exists(tempFilePath.getParent())) {
                logger.debug("Creating temporal folder: {}", tempFilePath.getParent());
                Files.createDirectory(tempFilePath.getParent());
            }

            // Start uploading the file to the temporal directory
            int read;
            byte[] bytes = new byte[1024];

            // Upload the file to a temporary folder
            try (OutputStream out = new FileOutputStream(new java.io.File(tempFilePath.toString()))) {
                while ((read = fileInputStream.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }
            }
        } catch (Exception e) {
            logger.error("Error uploading file {}", file.getName(), e);

            // Clean temporal directory
            ioManager.deleteDirectory(tempFilePath.getParent().toUri());

            throw new CatalogException("Error uploading file " + file.getName(), e);
        }

        // Register the file in catalog
        QueryResult<File> fileQueryResult;
        try {
            fileQueryResult = catalogManager.getFileManager().register(study, file, parents, QueryOptions.empty(), sessionId);

            // Create the directories where the file will be placed (if they weren't created before)
            ioManager.createDirectory(Paths.get(fileQueryResult.first().getUri()).getParent().toUri(), true);

            new org.opencb.opencga.catalog.managers.FileUtils(catalogManager).upload(tempFilePath.toUri(), fileQueryResult.first(),
                null, sessionId, false, overwrite, true, true, Long.MAX_VALUE);

            File fileMetadata = new FileMetadataReader(catalogManager)
                    .setMetadataInformation(fileQueryResult.first(), null, null, sessionId, false);
            fileQueryResult.setResult(Collections.singletonList(fileMetadata));
        } catch (Exception e) {
            logger.error("Error uploading file {}", file.getName(), e);

            // Clean temporal directory
            ioManager.deleteDirectory(tempFilePath.getParent().toUri());

            throw new CatalogException("Error uploading file " + file.getName(), e);
        }

        // Clean temporal directory
        ioManager.deleteDirectory(tempFilePath.getParent().toUri());

        return fileQueryResult;
    }

    /*
     * @deprecated This method if broken with multiple studies
     */
    @Deprecated
    public QueryResult<File> get(Long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(fileId), options, sessionId);
    }

    @Override
    public QueryResult<File> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));
        query.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        fixQueryObject(study, query, sessionId);

        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, options, userId);

        if (fileQueryResult.getNumResults() == 0 && query.containsKey(FileDBAdaptor.QueryParams.UID.key())) {
            List<Long> idList = query.getAsLongList(FileDBAdaptor.QueryParams.UID.key());
            for (Long myId : idList) {
                authorizationManager.checkFilePermission(study.getUid(), myId, userId, FileAclEntry.FilePermissions.VIEW);
            }
        }

        return fileQueryResult;
    }

    public QueryResult<FileTree> getTree(String fileIdStr, @Nullable String studyStr, Query query, QueryOptions queryOptions, int maxDepth,
                                         String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();

        queryOptions = ParamUtils.defaultObject(queryOptions, QueryOptions::new);
        query = ParamUtils.defaultObject(query, Query::new);

        if (queryOptions.containsKey(QueryOptions.INCLUDE)) {
            // Add type to the queryOptions
            List<String> asStringListOld = queryOptions.getAsStringList(QueryOptions.INCLUDE);
            List<String> newList = new ArrayList<>(asStringListOld.size());
            for (String include : asStringListOld) {
                newList.add(include);
            }
            newList.add(FileDBAdaptor.QueryParams.TYPE.key());
            queryOptions.put(QueryOptions.INCLUDE, newList);
        } else {
            // Avoid excluding type
            if (queryOptions.containsKey(QueryOptions.EXCLUDE)) {
                List<String> asStringListOld = queryOptions.getAsStringList(QueryOptions.EXCLUDE);
                if (asStringListOld.contains(FileDBAdaptor.QueryParams.TYPE.key())) {
                    // Remove type from exclude options
                    if (asStringListOld.size() > 1) {
                        List<String> toExclude = new ArrayList<>(asStringListOld.size() - 1);
                        for (String s : asStringListOld) {
                            if (!s.equalsIgnoreCase(FileDBAdaptor.QueryParams.TYPE.key())) {
                                toExclude.add(s);
                            }
                        }
                        queryOptions.put(QueryOptions.EXCLUDE, StringUtils.join(toExclude.toArray(), ","));
                    } else {
                        queryOptions.remove(QueryOptions.EXCLUDE);
                    }
                }
            }
        }

        MyResource<File> resource = getUid(fileIdStr, studyStr, sessionId);

        query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), resource.getStudy().getUid());

        // Check if we can obtain the file from the dbAdaptor properly.
        QueryOptions qOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.NAME.key(),
                        FileDBAdaptor.QueryParams.UID.key(), FileDBAdaptor.QueryParams.TYPE.key()));
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(resource.getResource().getUid(), qOptions);
        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
            throw new CatalogException("An error occurred with the database.");
        }

        // Check if the id does not correspond to a directory
        if (!fileQueryResult.first().getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogException("The file introduced is not a directory.");
        }

        // Call recursive method
        FileTree fileTree = getTree(fileQueryResult.first(), query, queryOptions, maxDepth, resource.getStudy().getUid(),
                resource.getUser());

        int dbTime = (int) (System.currentTimeMillis() - startTime);
        int numResults = countFilesInTree(fileTree);

        return new QueryResult<>("File tree", dbTime, numResults, numResults, "", "", Arrays.asList(fileTree));
    }

    public QueryResult<File> getFilesFromFolder(String folderStr, String studyStr, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(folderStr, "folder");
        MyResource<File> resource = getUid(folderStr, studyStr, sessionId);

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (!resource.getResource().getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogDBException("File {path:'" + resource.getResource().getPath() + "'} is not a folder.");
        }
        Query query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), resource.getResource().getPath());
        return get(studyStr, query, options, sessionId);
    }

    @Override
    public DBIterator<File> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(study, query, sessionId);
        query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return fileDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<File> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        fixQueryObject(study, query, sessionId);
        query.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        QueryResult<File> queryResult = fileDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    void fixQueryObject(Study study, Query query, String sessionId) throws CatalogException {
        // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResources<Sample> resource = catalogManager.getSampleManager().getUids(
                    query.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key()), study.getFqn(), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), resource.getResourceList().stream().map(Sample::getUid)
                    .collect(Collectors.toList()));
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }
    }

    @Override
    public QueryResult<File> count(String studyStr, Query query, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, query);
        // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        fixQueryObject(study, query, sessionId);

        query.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult<Long> queryResultAux = fileDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        params = ParamUtils.defaultObject(params, ObjectMap::new);

        WriteResult writeResult = new WriteResult("delete", -1, 0, 0, null, null, null);

        String userId;
        Study study;

        StopWatch watch = StopWatch.createStarted();

        // If the user is the owner or the admin, we won't check if he has permissions for every single entry
        boolean checkPermissions;

        // We try to get an iterator containing all the files to be deleted
        DBIterator<File> fileIterator;

        List<WriteResult.Fail> failedList = new ArrayList<>();

        try {
            userId = catalogManager.getUserManager().getUserId(sessionId);
            study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                    StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, sessionId);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            // If the user is the owner or the admin, we won't check if he has permissions for every single entry
            checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);

            fileIterator = fileDBAdaptor.iterator(finalQuery, QueryOptions.empty(), userId);

        } catch (CatalogException e) {
            logger.error("Delete file: {}", e.getMessage(), e);
            writeResult.setError(new Error(-1, null, e.getMessage()));
            writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
            return writeResult;
        }

        // We need to avoid processing subfolders or subfiles of an already processed folder independently
        Set<String> processedPaths = new HashSet<>();
        boolean physicalDelete = params.getBoolean(SKIP_TRASH, false) || params.getBoolean(DELETE_EXTERNAL_FILES, false);

        long numMatches = 0;

        while (fileIterator.hasNext()) {
            File file = fileIterator.next();

            if (subpathInPath(file.getPath(), processedPaths)) {
                // We skip this folder because it is a subfolder or subfile within an already processed folder
                continue;
            }

            try {
                if (checkPermissions) {
                    authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.DELETE);
                }

                // Check if the file can be deleted
                List<File> fileList = checkCanDeleteFile(studyStr, file, physicalDelete, userId);

                // Remove job references
                MyResources<File> resourceIds = new MyResources<>(userId, study, fileList);
                try {
                    removeJobReferences(resourceIds);
                } catch (CatalogException e) {
                    logger.error("Could not remove job references: {}", e.getMessage(), e);
                    throw new CatalogException("Could not remove job references: " + e.getMessage(), e);
                }


                // Remove the index references in case it is a transformed file or folder
                try {
                    updateIndexStatusAfterDeletionOfTransformedFile(study.getUid(), file);
                } catch (CatalogException e) {
                    logger.error("Could not remove relation references: {}", e.getMessage(), e);
                    throw new CatalogException("Could not remove relation references: " + e.getMessage(), e);
                }

                if (file.isExternal()) {
                    // unlink
                    WriteResult result = unlink(study.getUid(), file);
                    writeResult.setNumModified(writeResult.getNumModified() + result.getNumModified());
                    writeResult.setNumMatches(writeResult.getNumMatches() + result.getNumMatches());
                } else {
                    // local
                    if (physicalDelete) {
                        // physicalDelete
                        WriteResult result = physicalDelete(study.getUid(), file, params.getBoolean(FORCE_DELETE, false));
                        writeResult.setNumModified(writeResult.getNumModified() + result.getNumModified());
                        writeResult.setNumMatches(writeResult.getNumMatches() + result.getNumMatches());
                        failedList.addAll(result.getFailed());
                    } else {
                        // sendToTrash
                        WriteResult result = sendToTrash(study.getUid(), file);
                        writeResult.setNumModified(writeResult.getNumModified() + result.getNumModified());
                        writeResult.setNumMatches(writeResult.getNumMatches() + result.getNumMatches());
                    }
                }

                // We store the processed path as is
                if (file.getType() == File.Type.DIRECTORY) {
                    processedPaths.add(file.getPath());
                }
            } catch (Exception e) {
                numMatches += 1;

                failedList.add(new WriteResult.Fail(file.getId(), e.getMessage()));
                if (file.getType() == File.Type.FILE) {
                    logger.debug("Cannot delete file {}: {}", file.getId(), e.getMessage(), e);
                } else {
                    logger.debug("Cannot delete folder {}: {}", file.getId(), e.getMessage(), e);
                }
            }
        }

        writeResult.setDbTime((int) watch.getTime(TimeUnit.MILLISECONDS));
        writeResult.setFailed(failedList);
        writeResult.setNumMatches(writeResult.getNumMatches() + numMatches);

        if (!failedList.isEmpty()) {
            writeResult.setWarning(Collections.singletonList(new Error(-1, null, "There are files that could not be deleted")));
        }

        return writeResult;
    }

    public QueryResult<File> unlink(@Nullable String studyStr, String fileIdStr, String sessionId) throws CatalogException, IOException {
        ParamUtils.checkParameter(fileIdStr, "File");

        MyResource<File> resource = catalogManager.getFileManager().getUid(fileIdStr, studyStr, sessionId);
        String userId = resource.getUser();
        long fileId = resource.getResource().getUid();
        long studyUid = resource.getStudy().getUid();

        // Check 2. User has the proper permissions to delete the file.
        authorizationManager.checkFilePermission(studyUid, fileId, userId, FileAclEntry.FilePermissions.DELETE);

//        // Check if we can obtain the file from the dbAdaptor properly.
//        Query query = new Query()
//                .append(FileDBAdaptor.QueryParams.UID.key(), fileId)
//                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
//                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), GET_NON_TRASHED_FILES);
//        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, QueryOptions.empty());
//        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
//            throw new CatalogException("Cannot unlink file '" + fileIdStr + "'. The file was already deleted or unlinked.");
//        }
//
//        File file = fileQueryResult.first();
        File file = resource.getResource();

        // Check 3.
        if (!file.isExternal()) {
            throw new CatalogException("Only previously linked files can be unlinked. Please, use delete instead.");
        }

        // Check if the file can be deleted
        List<File> fileList = checkCanDeleteFile(studyStr, file, false, userId);

        // Remove job references
        MyResources<File> fileResource = new MyResources<>(userId, resource.getStudy(), fileList);
        try {
            removeJobReferences(fileResource);
        } catch (CatalogException e) {
            logger.error("Could not remove job references: {}", e.getMessage(), e);
            throw new CatalogException("Could not remove job references: " + e.getMessage(), e);
        }

        // Remove the index references in case it is a transformed file or folder
        try {
            updateIndexStatusAfterDeletionOfTransformedFile(studyUid, file);
        } catch (CatalogException e) {
            logger.error("Could not remove relation references: {}", e.getMessage(), e);
            throw new CatalogException("Could not remove relation references: " + e.getMessage(), e);
        }

        unlink(studyUid, file);

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), fileId)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        return fileDBAdaptor.get(query, new QueryOptions(), userId);
    }

    /**
     * This method sets the status of the file to PENDING_DELETE in all cases and does never remove a file from the file system (performed
     * by the daemon).
     * However, it applies the following changes:
     * - Status -> PENDING_DELETE
     * - Path -> Renamed to {path}__DELETED_{time}
     * - URI -> File or folder name from the file system renamed to {name}__DELETED_{time}
     * URI in the database updated accordingly
     *
     * @param studyId study id.
     * @param file    file or folder.
     * @return a WriteResult object.
     */
    private WriteResult physicalDelete(long studyId, File file, boolean forceDelete) throws CatalogException {
        StopWatch watch = StopWatch.createStarted();

        String currentStatus = file.getStatus().getName();
        if (File.FileStatus.DELETED.equals(currentStatus)) {
            throw new CatalogException("The file was already deleted");
        }
        if (File.FileStatus.PENDING_DELETE.equals(currentStatus) && !forceDelete) {
            throw new CatalogException("The file was already pending for deletion");
        }
        if (File.FileStatus.DELETING.equals(currentStatus)) {
            throw new CatalogException("The file is already being deleted");
        }

        URI fileUri = getUri(file);
        CatalogIOManager ioManager = catalogIOManagerFactory.get(fileUri);

        // Set the path suffix to DELETED
        String suffixName = INTERNAL_DELIMITER + File.FileStatus.DELETED + "_" + TimeUtils.getTime();

        long numMatched = 0;
        long numModified = 0;
        List<WriteResult.Fail> failedList = new ArrayList<>();

        if (file.getType() == File.Type.FILE) {
            logger.debug("Deleting physical file {}" + file.getPath());

            numMatched += 1;

            try {
                // 1. Set the file status to deleting
                ObjectMap update = new ObjectMap()
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING)
                        .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath() + suffixName);

                fileDBAdaptor.update(file.getUid(), update, QueryOptions.empty());

                // 2. Delete the file from disk
                try {
                    Files.delete(Paths.get(fileUri));
                } catch (IOException e) {
                    logger.error("{}", e.getMessage(), e);

                    // We rollback and leave the file/folder in PENDING_DELETE status
                    update = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE);
                    fileDBAdaptor.update(file.getUid(), update, QueryOptions.empty());

                    throw new CatalogException("Could not delete physical file/folder: " + e.getMessage(), e);
                }

                // 3. Update the file status in the database. Set to delete
                update = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);
                fileDBAdaptor.update(file.getUid(), update, QueryOptions.empty());

                numModified += 1;
            } catch (CatalogException e) {
                failedList.add(new WriteResult.Fail(file.getId(), e.getMessage()));
            }
        } else {
            logger.debug("Starting physical deletion of folder {}", file.getId());

            // Rename the directory in the filesystem.
            URI newURI;
            String basePath = Paths.get(file.getPath()).toString();
            String suffixedPath;

            if (!File.FileStatus.PENDING_DELETE.equals(currentStatus)) {
                try {
                    newURI = UriUtils.createDirectoryUri(Paths.get(fileUri).toString() + suffixName);
                } catch (URISyntaxException e) {
                    logger.error("URI exception: {}", e.getMessage(), e);
                    throw new CatalogException("URI exception: " + e.getMessage(), e);
                }

                logger.debug("Renaming {} to {}", fileUri.toString(), newURI.toString());
                ioManager.rename(fileUri, newURI);

                suffixedPath = basePath + suffixName;
            } else {
                // newURI is actually = to fileURI
                newURI = fileUri;

                // suffixedPath = basePath
                suffixedPath = basePath;
            }

            // Obtain all files and folders within the folder
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), GET_NON_DELETED_FILES);
            logger.debug("Looking for files and folders inside {} to mark as {}", file.getPath(), forceDelete
                    ? File.FileStatus.DELETED : File.FileStatus.PENDING_DELETE);

            QueryOptions options = new QueryOptions();
            if (forceDelete) {
                options.append(QueryOptions.SORT, FileDBAdaptor.QueryParams.PATH.key())
                        .append(QueryOptions.ORDER, QueryOptions.DESCENDING);
            }
            DBIterator<File> iterator = fileDBAdaptor.iterator(query, options);

            while (iterator.hasNext()) {
                File auxFile = iterator.next();
                numMatched += 1;

                String newPath;
                String newUri;

                if (!File.FileStatus.PENDING_DELETE.equals(currentStatus)) {
                    // Edit the PATH
                    newPath = auxFile.getPath().replaceFirst(basePath, suffixedPath);
                    newUri = auxFile.getUri().toString().replaceFirst(fileUri.toString(), newURI.toString());
                } else {
                    newPath = auxFile.getPath();
                    newUri = auxFile.getUri().toString();
                }

                try {
                    if (!forceDelete) {
                        // Deferred deletion
                        logger.debug("Replacing old uri {} for {}, old path {} for {}, and setting the status to {}",
                                auxFile.getUri().toString(), newUri, auxFile.getPath(), newPath, File.FileStatus.PENDING_DELETE);

                        ObjectMap updateParams = new ObjectMap()
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE)
                                .append(FileDBAdaptor.QueryParams.URI.key(), newUri)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), newPath);
                        fileDBAdaptor.update(auxFile.getUid(), updateParams, QueryOptions.empty());
                    } else {
                        // We delete the files and folders now

                        // 1. Set the file status to deleting
                        ObjectMap update = new ObjectMap()
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING)
                                .append(FileDBAdaptor.QueryParams.URI.key(), newUri)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), newPath);
                        fileDBAdaptor.update(auxFile.getUid(), update, QueryOptions.empty());

                        // 2. Delete the file from disk
                        try {
                            Files.delete(Paths.get(newUri.replaceFirst("file://", "")));
                        } catch (IOException e) {
                            logger.error("{}", e.getMessage(), e);

                            // We rollback and leave the file/folder in PENDING_DELETE status
                            update = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE);
                            fileDBAdaptor.update(auxFile.getUid(), update, QueryOptions.empty());

                            throw new CatalogException("Could not delete physical file/folder: " + e.getMessage(), e);
                        }

                        // 3. Update the file status in the database. Set to delete
                        update = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);
                        fileDBAdaptor.update(auxFile.getUid(), update, QueryOptions.empty());
                    }
                    numModified += 1;
                } catch (CatalogException e) {
                    failedList.add(new WriteResult.Fail(auxFile.getId(), e.getMessage()));
                }
            }
        }

        return new WriteResult("delete", (int) watch.getTime(TimeUnit.MILLISECONDS), numMatched, numModified, failedList, null, null);
    }

    private WriteResult sendToTrash(long studyId, File file) throws CatalogDBException {
        // It doesn't really matter if file is a file or a directory. I can directly set the status of the file or the directory +
        // subfiles and subdirectories doing a single query as I don't need to rename anything
        // Obtain all files within the folder
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), GET_NON_DELETED_FILES);
        ObjectMap params = new ObjectMap()
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);

        QueryResult<Long> update = fileDBAdaptor.update(query, params, QueryOptions.empty());

        return new WriteResult("trash", update.getDbTime(), update.first(), update.first(), null, null, null);
    }

    private WriteResult unlink(long studyId, File file) throws CatalogDBException {
        StopWatch watch = StopWatch.createStarted();

        String suffixName = INTERNAL_DELIMITER + File.FileStatus.REMOVED + "_" + TimeUtils.getTime();

        // Set the new path
        String basePath = Paths.get(file.getPath()).toString();
        String suffixedPath = basePath + suffixName;

        long numMatched = 0;
        long numModified = 0;

        if (file.getType() == File.Type.FILE) {
            numMatched += 1;

            ObjectMap params = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath().replaceFirst(basePath, suffixedPath))
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);

            logger.debug("Unlinking file {}", file.getPath());
            fileDBAdaptor.update(file.getUid(), params, QueryOptions.empty());

            numModified += 1;
        } else {
            // Obtain all files within the folder
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), GET_NON_DELETED_FILES);

            logger.debug("Looking for files and folders inside {} to unlink", file.getPath());
            DBIterator<File> iterator = fileDBAdaptor.iterator(query, new QueryOptions());

            while (iterator.hasNext()) {
                File auxFile = iterator.next();
                numMatched += 1;

                ObjectMap updateParams = new ObjectMap()
                        .append(FileDBAdaptor.QueryParams.PATH.key(), auxFile.getPath().replaceFirst(basePath, suffixedPath))
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);

                fileDBAdaptor.update(auxFile.getUid(), updateParams, QueryOptions.empty());

                numModified += 1;
            }
        }

        return new WriteResult("unlink", (int) watch.getTime(TimeUnit.MILLISECONDS), numMatched, numModified, null, null, null);
    }

    private boolean subpathInPath(String subpath, Set<String> pathSet) {
        String[] split = StringUtils.split(subpath, "/");
        String auxPath = "";
        for (String s : split) {
            auxPath += s + "/";
            if (pathSet.contains(auxPath)) {
                return true;
            }
        }
        return false;
    }

    public QueryResult<File> updateAnnotations(String studyStr, String fileStr, String annotationSetId,
                                               Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                               QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            return new QueryResult<>(fileStr, -1, -1, -1, "Nothing to do: The map of annotations is empty", "", Collections.emptyList());
        }
        ObjectMap params = new ObjectMap(AnnotationSetManager.ANNOTATIONS, new AnnotationSet(annotationSetId, "", annotations));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, fileStr, params, options, token);
    }

    public QueryResult<File> removeAnnotations(String studyStr, String fileStr, String annotationSetId,
                                               List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, fileStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public QueryResult<File> resetAnnotations(String studyStr, String fileStr, String annotationSetId, List<String> annotations,
                                              QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, fileStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    @Override
    public QueryResult<File> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResource<File> resource = getUid(entryStr, studyStr, sessionId);

        String userId = userManager.getUserId(sessionId);
        File file = resource.getResource();

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (parameters.containsKey(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            authorizationManager.checkFilePermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                    FileAclEntry.FilePermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkFilePermission(resource.getStudy().getUid(), file.getUid(), userId,
                    FileAclEntry.FilePermissions.WRITE);
        }

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> FileDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        // We obtain the numeric ids of the samples given
        if (StringUtils.isNotEmpty(parameters.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            List<String> sampleIdStr = parameters.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key());

            MyResources<Sample> sampleResource = catalogManager.getSampleManager().getUids(sampleIdStr, studyStr, sessionId);

            // Avoid sample duplicates
            Set<Long> sampleIdsSet = sampleResource.getResourceList().stream().map(Sample::getUid).collect(Collectors.toSet());

            List<Sample> sampleList = new ArrayList<>(sampleIdsSet.size());
            for (Long sampleId : sampleIdsSet) {
                Sample sample = new Sample();
                sample.setUid(sampleId);
                sampleList.add(sample);
            }
            parameters.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
        }

        //Name must be changed with "rename".
        if (parameters.containsKey(FileDBAdaptor.QueryParams.NAME.key())) {
            logger.info("Rename file using update method!");
            rename(studyStr, file.getPath(), parameters.getString(FileDBAdaptor.QueryParams.NAME.key()), sessionId);
        }

        return unsafeUpdate(resource.getStudy(), file, parameters, options, userId);
    }

    QueryResult<File> unsafeUpdate(Study study, File file, ObjectMap parameters, QueryOptions options, String userId)
            throws CatalogException {
        if (isRootFolder(file)) {
            throw new CatalogException("Cannot modify root folder");
        }

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> FileDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        MyResource<File> resource = new MyResource<>(userId, study, file);
        List<VariableSet> variableSetList = checkUpdateAnnotationsAndExtractVariableSets(resource, parameters, options, fileDBAdaptor);

        String ownerId = studyDBAdaptor.getOwnerId(study.getUid());
        fileDBAdaptor.update(file.getUid(), parameters, variableSetList, options);
        QueryResult<File> queryResult = fileDBAdaptor.get(file.getUid(), options);
        auditManager.recordUpdate(AuditRecord.Resource.file, file.getUid(), userId, parameters, null, null);
        userDBAdaptor.updateUserLastModified(ownerId);
        return queryResult;
    }

    private void removeJobReferences(MyResources<File> resource) throws CatalogException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                JobDBAdaptor.QueryParams.UID.key(), JobDBAdaptor.QueryParams.INPUT.key(), JobDBAdaptor.QueryParams.OUTPUT.key(),
                JobDBAdaptor.QueryParams.OUT_DIR.key(), JobDBAdaptor.QueryParams.ATTRIBUTES.key()));

        List<Long> resourceList = resource.getResourceList().stream().map(File::getUid).collect(Collectors.toList());

        // Find all the jobs containing references to any of the files to be deleted
        Query query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), resource.getStudy().getUid())
                .append(JobDBAdaptor.QueryParams.INPUT_UID.key(), resourceList)
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryResult<Job> jobInputFiles = jobDBAdaptor.get(query, options);

        query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), resource.getStudy().getUid())
                .append(JobDBAdaptor.QueryParams.OUTPUT_UID.key(), resourceList)
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryResult<Job> jobOutputFiles = jobDBAdaptor.get(query, options);

        query = new Query()
                .append(JobDBAdaptor.QueryParams.STUDY_UID.key(), resource.getStudy().getUid())
                .append(JobDBAdaptor.QueryParams.OUT_DIR_UID.key(), resourceList)
                .append(JobDBAdaptor.QueryParams.STATUS_NAME.key(), Constants.ALL_STATUS);
        QueryResult<Job> jobOutDirFolders = jobDBAdaptor.get(query, options);

        // We create a job map that will contain all the changes done so far to avoid performing more queries
        Map<Long, Job> jobMap = new HashMap<>();
        Set<Long> fileIdsReferencedInJobs = new HashSet<>();
        for (Job job : jobInputFiles.getResult()) {
            fileIdsReferencedInJobs.addAll(job.getInput().stream().map(File::getUid).collect(Collectors.toList()));
            jobMap.put(job.getUid(), job);
        }
        for (Job job : jobOutputFiles.getResult()) {
            fileIdsReferencedInJobs.addAll(job.getOutput().stream().map(File::getUid).collect(Collectors.toList()));
            jobMap.put(job.getUid(), job);
        }
        for (Job job : jobOutDirFolders.getResult()) {
            fileIdsReferencedInJobs.add(job.getOutDir().getUid());
            jobMap.put(job.getUid(), job);
        }

        if (fileIdsReferencedInJobs.isEmpty()) {
            logger.info("No associated jobs found for the files to be deleted.");
            return;
        }

        // We create a map with the files that are related to jobs that are going to be deleted
        Map<Long, File> relatedFileMap = new HashMap<>();
        for (Long fileId : resourceList) {
            if (fileIdsReferencedInJobs.contains(fileId)) {
                relatedFileMap.put(fileId, null);
            }
        }

        if (relatedFileMap.isEmpty()) {
            logger.error("Unexpected error: None of the matching jobs seem to be related to any of the files to be deleted");
            throw new CatalogException("Internal error. Please, report to the OpenCGA administrators.");
        }

        // We obtain the current information of those files
        query = new Query()
                .append(FileDBAdaptor.QueryParams.UID.key(), relatedFileMap.keySet())
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), resource.getStudy().getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), GET_NON_DELETED_FILES);
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, QueryOptions.empty());

        if (fileQueryResult.getNumResults() < relatedFileMap.size()) {
            logger.error("Unexpected error: The number of files fetched does not match the number of files looked for.");
            throw new CatalogException("Internal error. Please, report to the OpenCGA administrators.");
        }

        relatedFileMap = new HashMap<>();
        for (File file : fileQueryResult.getResult()) {
            relatedFileMap.put(file.getUid(), file);
        }

        // We update the input files from the jobs
        for (Job jobAux : jobInputFiles.getResult()) {
            Job job = jobMap.get(jobAux.getUid());

            List<File> inputFiles = new ArrayList<>(job.getInput().size());
            List<File> attributeFiles = new ArrayList<>(job.getInput().size());
            for (File file : job.getInput()) {
                if (relatedFileMap.containsKey(file.getUid())) {
                    attributeFiles.add(relatedFileMap.get(file.getUid()));
                } else {
                    inputFiles.add(file);
                }
            }

            if (attributeFiles.isEmpty()) {
                logger.error("Unexpected error: Deleted file was apparently not found in the map of job associated files");
                throw new CatalogException("Internal error. Please, report to the OpenCGA administrators.");
            }

            Map<String, Object> attributes = job.getAttributes();
            ObjectMap opencgaAttributes;
            if (!attributes.containsKey(Constants.PRIVATE_OPENCGA_ATTRIBUTES)) {
                opencgaAttributes = new ObjectMap();
                attributes.put(Constants.PRIVATE_OPENCGA_ATTRIBUTES, opencgaAttributes);
            } else {
                opencgaAttributes = (ObjectMap) attributes.get(Constants.PRIVATE_OPENCGA_ATTRIBUTES);
            }

            List<Object> fileList = opencgaAttributes.getAsList(Constants.JOB_DELETED_INPUT_FILES);
            if (fileList == null || fileList.isEmpty()) {
                fileList = new ArrayList<>(attributeFiles);
            } else {
                fileList = new ArrayList<>(fileList);
                fileList.addAll(attributeFiles);
            }
            opencgaAttributes.put(Constants.JOB_DELETED_INPUT_FILES, fileList);

            ObjectMap params = new ObjectMap()
                    .append(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes)
                    .append(JobDBAdaptor.QueryParams.INPUT.key(), inputFiles);
            jobDBAdaptor.update(job.getUid(), params, QueryOptions.empty());
        }

        // We update the output files from the jobs
        for (Job jobAux : jobOutputFiles.getResult()) {
            Job job = jobMap.get(jobAux.getUid());

            List<File> outputFiles = new ArrayList<>(job.getOutput().size());
            List<File> attributeFiles = new ArrayList<>(job.getOutput().size());
            for (File file : job.getOutput()) {
                if (relatedFileMap.containsKey(file.getUid())) {
                    attributeFiles.add(relatedFileMap.get(file.getUid()));
                } else {
                    outputFiles.add(file);
                }
            }

            if (attributeFiles.isEmpty()) {
                logger.error("Unexpected error: Deleted file was apparently not found in the map of job associated files");
                throw new CatalogException("Internal error. Please, report to the OpenCGA administrators.");
            }

            Map<String, Object> attributes = job.getAttributes();
            ObjectMap opencgaAttributes;
            if (!attributes.containsKey(Constants.PRIVATE_OPENCGA_ATTRIBUTES)) {
                opencgaAttributes = new ObjectMap();
                attributes.put(Constants.PRIVATE_OPENCGA_ATTRIBUTES, opencgaAttributes);
            } else {
                opencgaAttributes = (ObjectMap) attributes.get(Constants.PRIVATE_OPENCGA_ATTRIBUTES);
            }

            List<Object> fileList = opencgaAttributes.getAsList(Constants.JOB_DELETED_OUTPUT_FILES);
            if (fileList == null || fileList.isEmpty()) {
                fileList = new ArrayList<>(attributeFiles);
            } else {
                fileList = new ArrayList<>(fileList);
                fileList.addAll(attributeFiles);
            }
            opencgaAttributes.put(Constants.JOB_DELETED_OUTPUT_FILES, fileList);

            ObjectMap params = new ObjectMap()
                    .append(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes)
                    .append(JobDBAdaptor.QueryParams.OUTPUT.key(), outputFiles);
            jobDBAdaptor.update(job.getUid(), params, QueryOptions.empty());
        }

        // We update the outdir file from the jobs
        for (Job jobAux : jobOutDirFolders.getResult()) {
            Job job = jobMap.get(jobAux.getUid());

            File outDir = job.getOutDir();
            if (outDir == null || outDir.getUid() <= 0) {
                logger.error("Unexpected error: Output directory from job not found?");
                throw new CatalogException("Internal error. Please, report to the OpenCGA administrators.");
            }
            if (!relatedFileMap.containsKey(job.getOutDir().getUid())) {
                logger.error("Unexpected error: Deleted output directory was apparently not found in the map of job associated files");
                throw new CatalogException("Internal error. Please, report to the OpenCGA administrators.");
            }
            // We get the whole file entry
            outDir = relatedFileMap.get(outDir.getUid());

            Map<String, Object> attributes = job.getAttributes();
            ObjectMap opencgaAttributes;
            if (!attributes.containsKey(Constants.PRIVATE_OPENCGA_ATTRIBUTES)) {
                opencgaAttributes = new ObjectMap();
                attributes.put(Constants.PRIVATE_OPENCGA_ATTRIBUTES, opencgaAttributes);
            } else {
                opencgaAttributes = (ObjectMap) attributes.get(Constants.PRIVATE_OPENCGA_ATTRIBUTES);
            }
            opencgaAttributes.put(Constants.JOB_DELETED_OUTPUT_DIRECTORY, outDir);

            ObjectMap params = new ObjectMap()
                    .append(JobDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes)
                    .append(JobDBAdaptor.QueryParams.OUT_DIR.key(), -1L);
            jobDBAdaptor.update(job.getUid(), params, QueryOptions.empty());
        }
    }

    public QueryResult<File> link(String studyStr, URI uriOrigin, String pathDestiny, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
        // We make two attempts to link to ensure the behaviour remains even if it is being called at the same time link from different
        // threads
        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        try {
            return privateLink(study, uriOrigin, pathDestiny, params, sessionId);
        } catch (CatalogException | IOException e) {
            return privateLink(study, uriOrigin, pathDestiny, params, sessionId);
        }
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);

        fixQueryObject(study, query, sessionId);

        // Add study id to the query
        query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        // We do not need to check for permissions when we show the count of files
        QueryResult queryResult = fileDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    public QueryResult<File> rename(String studyStr, String fileStr, String newName, String sessionId) throws CatalogException {
        ParamUtils.checkFileName(newName, "name");
        MyResource<File> resource = getUid(fileStr, studyStr, sessionId);
        String userId = resource.getUser();
        Study study = resource.getStudy();
        String ownerId = StringUtils.split(study.getFqn(), "@")[0];

        File file = resource.getResource();

        authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        if (file.getName().equals(newName)) {
            return new QueryResult<>("rename", -1, 0, 0, "The file was already named " + newName, "", Collections.emptyList());
        }

        if (isRootFolder(file)) {
            throw new CatalogException("Can not rename root folder");
        }

        String oldPath = file.getPath();
        Path parent = Paths.get(oldPath).getParent();
        String newPath;
        if (parent == null) {
            newPath = newName;
        } else {
            newPath = parent.resolve(newName).toString();
        }

        userDBAdaptor.updateUserLastModified(ownerId);
        CatalogIOManager catalogIOManager;
        URI oldUri = file.getUri();
        URI newUri = Paths.get(oldUri).getParent().resolve(newName).toUri();
//        URI studyUri = file.getUri();
        boolean isExternal = file.isExternal(); //If the file URI is not null, the file is external located.
        QueryResult<File> result;
        switch (file.getType()) {
            case DIRECTORY:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(oldUri); // TODO? check if something in the subtree is not READY?
                    if (catalogIOManager.exists(oldUri)) {
                        catalogIOManager.rename(oldUri, newUri);   // io.move() 1
                    }
                }
                result = fileDBAdaptor.rename(file.getUid(), newPath, newUri.toString(), null);
                auditManager.recordUpdate(AuditRecord.Resource.file, file.getUid(), userId, new ObjectMap("path", newPath)
                        .append("name", newName), "rename", null);
                break;
            case FILE:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(oldUri);
                    catalogIOManager.rename(oldUri, newUri);
                }
                result = fileDBAdaptor.rename(file.getUid(), newPath, newUri.toString(), null);
                auditManager.recordUpdate(AuditRecord.Resource.file, file.getUid(), userId, new ObjectMap("path", newPath)
                        .append("name", newName), "rename", null);
                break;
            default:
                throw new CatalogException("Unknown file type " + file.getType());
        }

        return result;
    }

    public DataInputStream grep(String studyStr, String fileStr, String pattern, QueryOptions options, String sessionId)
            throws CatalogException {
        MyResource<File> resource = getUid(fileStr, studyStr, sessionId);
        authorizationManager.checkFilePermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                FileAclEntry.FilePermissions.VIEW);
        URI fileUri = getUri(resource.getResource());
        boolean ignoreCase = options.getBoolean("ignoreCase");
        boolean multi = options.getBoolean("multi");
        return catalogIOManagerFactory.get(fileUri).getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    public DataInputStream download(String studyStr, String fileStr, int start, int limit, QueryOptions options, String sessionId)
            throws CatalogException {
        MyResource<File> resource = getUid(fileStr, studyStr, sessionId);
        authorizationManager.checkFilePermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                FileAclEntry.FilePermissions.DOWNLOAD);
        URI fileUri = getUri(resource.getResource());
        return catalogIOManagerFactory.get(fileUri).getFileObject(fileUri, start, limit);
    }

    public QueryResult<Job> index(String studyStr, List<String> fileList, String type, Map<String, String> params, String sessionId)
            throws CatalogException {
        params = ParamUtils.defaultObject(params, HashMap::new);
        MyResources<File> resource = getUids(fileList, studyStr, sessionId);
        List<File> fileFolderIdList = resource.getResourceList();
        long studyId = resource.getStudy().getUid();
        String userId = resource.getUser();

        // Define the output directory where the indexes will be put
        String outDirPath = ParamUtils.defaultString(params.get("outdir"), "/");
        if (outDirPath != null && outDirPath.contains("/") && !outDirPath.endsWith("/")) {
            outDirPath = outDirPath + "/";
        }

        File outDir;
        try {
            outDir = smartResolutor(resource.getStudy().getUid(), outDirPath, resource.getUser());
        } catch (CatalogException e) {
            logger.warn("'{}' does not exist. Trying to create the output directory.", outDirPath);
            QueryResult<File> folder = createFolder(studyStr, outDirPath, new File.FileStatus(), true, "", new QueryOptions(), sessionId);
            outDir = folder.first();
        }

        authorizationManager.checkFilePermission(studyId, outDir.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        QueryResult<Job> jobQueryResult;
        List<File> fileIdList = new ArrayList<>();
        String indexDaemonType = null;
        String jobName = null;
        String description = null;

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                FileDBAdaptor.QueryParams.NAME.key(),
                FileDBAdaptor.QueryParams.PATH.key(),
                FileDBAdaptor.QueryParams.URI.key(),
                FileDBAdaptor.QueryParams.TYPE.key(),
                FileDBAdaptor.QueryParams.BIOFORMAT.key(),
                FileDBAdaptor.QueryParams.FORMAT.key(),
                FileDBAdaptor.QueryParams.INDEX.key())
        );

        if (type.equals("VCF")) {

            indexDaemonType = IndexDaemon.VARIANT_TYPE;
            Boolean transform = Boolean.valueOf(params.get("transform"));
            Boolean load = Boolean.valueOf(params.get("load"));
            if (transform && !load) {
                jobName = "variant_transform";
                description = "Transform variants from " + fileList;
            } else if (load && !transform) {
                description = "Load variants from " + fileList;
                jobName = "variant_load";
            } else {
                description = "Index variants from " + fileList;
                jobName = "variant_index";
            }

            for (File file : fileFolderIdList) {
                if (File.Type.DIRECTORY.equals(file.getType())) {
                    // Retrieve all the VCF files that can be found within the directory
                    String path = file.getPath().endsWith("/") ? file.getPath() : file.getPath() + "/";
                    Query query = new Query()
                            .append(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF, File.Format.GVCF))
                            .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + path + "*")
                            .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, queryOptions);

                    if (fileQueryResult.getNumResults() == 0) {
                        throw new CatalogException("No VCF files could be found in directory " + file.getPath());
                    }

                    for (File fileTmp : fileQueryResult.getResult()) {
                        authorizationManager.checkFilePermission(studyId, fileTmp.getUid(), userId, FileAclEntry.FilePermissions.VIEW);
                        authorizationManager.checkFilePermission(studyId, fileTmp.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

                        fileIdList.add(fileTmp);
                    }

                } else {
                    if (isTransformedFile(file.getName())) {
                        if (file.getRelatedFiles() == null || file.getRelatedFiles().isEmpty()) {
                            catalogManager.getFileManager().matchUpVariantFiles(studyStr, Collections.singletonList(file), sessionId);
                        }
                        if (file.getRelatedFiles() != null) {
                            for (File.RelatedFile relatedFile : file.getRelatedFiles()) {
                                if (File.RelatedFile.Relation.PRODUCED_FROM.equals(relatedFile.getRelation())) {
                                    Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), relatedFile.getFileId());
                                    file = get(studyStr, query, null, sessionId).first();
                                    break;
                                }
                            }
                        }
                    }
                    if (!File.Format.VCF.equals(file.getFormat()) && !File.Format.GVCF.equals(file.getFormat())) {
                        throw new CatalogException("The file " + file.getName() + " is not a VCF file.");
                    }

                    authorizationManager.checkFilePermission(studyId, file.getUid(), userId, FileAclEntry.FilePermissions.VIEW);
                    authorizationManager.checkFilePermission(studyId, file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

                    fileIdList.add(file);
                }
            }

            if (fileIdList.size() == 0) {
                throw new CatalogException("Cannot send to index. No files could be found to be indexed.");
            }

            params.put("outdir", outDir.getId());
            params.put("sid", sessionId);

        } else if (type.equals("BAM")) {

            indexDaemonType = IndexDaemon.ALIGNMENT_TYPE;
            jobName = "AlignmentIndex";

            for (File file : fileFolderIdList) {
                if (File.Type.DIRECTORY.equals(file.getType())) {
                    // Retrieve all the BAM files that can be found within the directory
                    String path = file.getPath().endsWith("/") ? file.getPath() : file.getPath() + "/";
                    Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.SAM, File.Format.BAM))
                            .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + path + "*")
                            .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, queryOptions);

                    if (fileQueryResult.getNumResults() == 0) {
                        throw new CatalogException("No SAM/BAM files could be found in directory " + file.getPath());
                    }

                    for (File fileTmp : fileQueryResult.getResult()) {
                        authorizationManager.checkFilePermission(studyId, fileTmp.getUid(), userId, FileAclEntry.FilePermissions.VIEW);
                        authorizationManager.checkFilePermission(studyId, fileTmp.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

                        fileIdList.add(fileTmp);
                    }

                } else {
                    if (!File.Format.BAM.equals(file.getFormat()) && !File.Format.SAM.equals(file.getFormat())) {
                        throw new CatalogException("The file " + file.getName() + " is not a SAM/BAM file.");
                    }

                    authorizationManager.checkFilePermission(studyId, file.getUid(), userId, FileAclEntry.FilePermissions.VIEW);
                    authorizationManager.checkFilePermission(studyId, file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

                    fileIdList.add(file);
                }
            }

        }

        if (fileIdList.size() == 0) {
            throw new CatalogException("Cannot send to index. No files could be found to be indexed.");
        }

        String fileIds = fileIdList.stream().map(File::getId).collect(Collectors.joining(","));
        params.put("file", fileIds);
        List<File> outputList = outDir.getUid() > 0 ? Arrays.asList(outDir) : Collections.emptyList();
        ObjectMap attributes = new ObjectMap();
        attributes.put(IndexDaemon.INDEX_TYPE, indexDaemonType);
        attributes.putIfNotNull(Job.OPENCGA_OUTPUT_DIR, outDirPath);
        attributes.putIfNotNull(Job.OPENCGA_STUDY, studyStr);

        logger.info("job description: " + description);
        jobQueryResult = catalogManager.getJobManager().queue(studyStr, jobName, description, "opencga-analysis.sh",
                Job.Type.INDEX, params, fileIdList, outputList, outDir, attributes, sessionId);
        jobQueryResult.first().setToolId(jobName);

        return jobQueryResult;
    }

    public void setFileIndex(String studyStr, String fileId, FileIndex index, String sessionId) throws CatalogException {
        MyResource<File> resource = getUid(fileId, studyStr, sessionId);
        authorizationManager.checkFilePermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
        fileDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, resource.getResource().getUid(), resource.getUser(), parameters, null, null);
    }

    public void setDiskUsage(String studyStr, String fileId, long size, String sessionId) throws CatalogException {
        MyResource<File> resource = getUid(fileId, studyStr, sessionId);
        authorizationManager.checkFilePermission(resource.getStudy().getUid(), resource.getResource().getUid(), resource.getUser(),
                FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.SIZE.key(), size);
        fileDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, resource.getResource().getUid(), resource.getUser(), parameters, null, null);
    }

    @Deprecated
    public void setModificationDate(String studyStr, String fileId, String date, String sessionId) throws CatalogException {
        MyResource<File> resource = getUid(fileId, studyStr, sessionId);
        String userId = resource.getUser();
        long studyId = resource.getStudy().getUid();
        authorizationManager.checkFilePermission(studyId, resource.getResource().getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.MODIFICATION_DATE.key(), date);
        fileDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, resource.getResource().getUid(), userId, parameters, null, null);
    }

    public void setUri(String studyStr, String fileId, String uri, String sessionId) throws CatalogException {
        MyResource<File> resource = getUid(fileId, studyStr, sessionId);
        String userId = resource.getUser();
        long studyId = resource.getStudy().getUid();
        authorizationManager.checkFilePermission(studyId, resource.getResource().getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.URI.key(), uri);
        fileDBAdaptor.update(resource.getResource().getUid(), parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, resource.getResource().getUid(), userId, parameters, null, null);
    }


    // **************************   ACLs  ******************************** //
    public List<QueryResult<FileAclEntry>> getAcls(String studyStr, List<String> fileList, String member, boolean silent, String sessionId)
            throws CatalogException {
        List<QueryResult<FileAclEntry>> fileAclList = new ArrayList<>(fileList.size());

        for (String file : fileList) {
            try {
                MyResource<File> resource = getUid(file, studyStr, sessionId);

                QueryResult<FileAclEntry> allFileAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allFileAcls = authorizationManager.getFileAcl(resource.getStudy().getUid(), resource.getResource().getUid(),
                            resource.getUser(), member);
                } else {
                    allFileAcls = authorizationManager.getAllFileAcls(resource.getStudy().getUid(), resource.getResource().getUid(),
                            resource.getUser(), true);
                }
                allFileAcls.setId(file);
                fileAclList.add(allFileAcls);
            } catch (CatalogException e) {
                if (silent) {
                    fileAclList.add(new QueryResult<>(file, 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return fileAclList;
    }

    public List<QueryResult<FileAclEntry>> updateAcl(String studyStr, List<String> fileList, String memberIds,
                                                     File.FileAclParams fileAclParams, String sessionId) throws CatalogException {
        int count = 0;
        count += fileList != null && !fileList.isEmpty() ? 1 : 0;
        count += StringUtils.isNotEmpty(fileAclParams.getSample()) ? 1 : 0;

        if (count > 1) {
            throw new CatalogException("Update ACL: Only one of these parameters are allowed: file or sample per query.");
        } else if (count == 0) {
            throw new CatalogException("Update ACL: At least one of these parameters should be provided: file or sample");
        }

        if (fileAclParams.getAction() == null) {
            throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
        }

        List<String> permissions = Collections.emptyList();
        if (StringUtils.isNotEmpty(fileAclParams.getPermissions())) {
            permissions = Arrays.asList(fileAclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
            checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);
        }

        if (StringUtils.isNotEmpty(fileAclParams.getSample())) {
            // Obtain the sample ids
            MyResources<Sample> resource = catalogManager.getSampleManager().getUids(
                    Arrays.asList(StringUtils.split(fileAclParams.getSample(), ",")), studyStr, sessionId);

            Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), resource.getResourceList().stream().map(Sample::getUid)
                    .collect(Collectors.toList()));
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UID.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(studyStr, query, options, sessionId);

            fileList = fileQueryResult.getResult().stream().map(File::getUid).map(String::valueOf).collect(Collectors.toList());
        }

        // Obtain the resource ids
        MyResources<File> resource = getUids(fileList, studyStr, sessionId);
        authorizationManager.checkCanAssignOrSeePermissions(resource.getStudy().getUid(), resource.getUser());

        // Increase the list with the files/folders within the list of ids that correspond with folders
        resource = getRecursiveFilesAndFolders(resource);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resource.getStudy().getUid(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        switch (fileAclParams.getAction()) {
            case SET:
                // Todo: Remove this in 1.4
                List<String> allFilePermissions = EnumSet.allOf(FileAclEntry.FilePermissions.class)
                        .stream()
                        .map(String::valueOf)
                        .collect(Collectors.toList());
                return authorizationManager.setAcls(resource.getStudy().getUid(), resource.getResourceList().stream().map(File::getUid)
                        .collect(Collectors.toList()), members, permissions, allFilePermissions, Entity.FILE);
            case ADD:
                return authorizationManager.addAcls(resource.getStudy().getUid(), resource.getResourceList().stream().map(File::getUid)
                        .collect(Collectors.toList()), members, permissions, Entity.FILE);
            case REMOVE:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(File::getUid).collect(Collectors.toList()),
                        members, permissions, Entity.FILE);
            case RESET:
                return authorizationManager.removeAcls(resource.getResourceList().stream().map(File::getUid).collect(Collectors.toList()),
                        members, null, Entity.FILE);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
    }


    // **************************   Private methods   ******************************** //
    private boolean isRootFolder(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        return file.getPath().isEmpty();
    }

    /**
     * Fetch all the recursive files and folders within the list of file ids given.
     *
     * @param resource ResourceId object containing the list of file ids, studyId and userId.
     * @return a new ResourceId object
     */
    private MyResources<File> getRecursiveFilesAndFolders(MyResources<File> resource) throws CatalogException {
        List<File> fileList = new LinkedList<>();
        fileList.addAll(resource.getResourceList());

        Set<Long> uidFileSet = new HashSet<>();
        uidFileSet.addAll(resource.getResourceList().stream().map(File::getUid).collect(Collectors.toSet()));

        List<String> pathList = new ArrayList<>();
        for (File file : resource.getResourceList()) {
            if (file.getType().equals(File.Type.DIRECTORY)) {
                pathList.add("~^" + file.getPath());
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UID.key());
        if (CollectionUtils.isNotEmpty(pathList)) {
            // Search for all the files within the list of paths
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), resource.getStudy().getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), pathList);
            QueryResult<File> fileQueryResult1 = fileDBAdaptor.get(query, options);
            for (File file1 : fileQueryResult1.getResult()) {
                if (!uidFileSet.contains(file1.getUid())) {
                    uidFileSet.add(file1.getUid());
                    fileList.add(file1);
                }
            }
        }

        return new MyResources<>(resource.getUser(), resource.getStudy(), fileList);
    }

    private List<String> getParentPaths(String filePath) {
        String path = "";
        String[] split = filePath.split("/");
        List<String> paths = new ArrayList<>(split.length + 1);
        paths.add("");  //Add study root folder
        //Add intermediate folders
        //Do not add the last split, could be a file or a folder..
        //Depending on this, it could end with '/' or not.
        for (int i = 0; i < split.length - 1; i++) {
            String f = split[i];
            path = path + f + "/";
            paths.add(path);
        }
        paths.add(filePath); //Add the file path
        return paths;
    }

    @Override
    File smartResolutor(long studyUid, String fileName, String user) throws CatalogException {
        if (UUIDUtils.isOpenCGAUUID(fileName)) {
            // We search as uuid
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(FileDBAdaptor.QueryParams.UUID.key(), fileName);
            QueryResult<File> pathQueryResult = fileDBAdaptor.get(query, QueryOptions.empty());
            if (pathQueryResult.getNumResults() > 1) {
                throw new CatalogException("Error: More than one file id found based on " + fileName);
            } else if (pathQueryResult.getNumResults() == 1) {
                return pathQueryResult.first();
            }
        }

        fileName = fileName.replace(":", "/");

        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }

        // We search as a path
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.PATH.key(), fileName);
        QueryResult<File> pathQueryResult = fileDBAdaptor.get(query, QueryOptions.empty());
        if (pathQueryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one file id found based on " + fileName);
        } else if (pathQueryResult.getNumResults() == 1) {
            return pathQueryResult.first();
        }

        if (!fileName.contains("/")) {
            // We search as a fileName as well
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(FileDBAdaptor.QueryParams.NAME.key(), fileName);
            QueryResult<File> nameQueryResult = fileDBAdaptor.get(query, QueryOptions.empty());
            if (nameQueryResult.getNumResults() > 1) {
                throw new CatalogException("Error: More than one file id found based on " + fileName);
            } else if (nameQueryResult.getNumResults() == 1) {
                return nameQueryResult.first();
            }
        }

        throw new CatalogException("File " + fileName + " not found");
    }

    //FIXME: This should use org.opencb.opencga.storage.core.variant.io.VariantReaderUtils
    private String getOriginalFile(String name) {
        if (name.endsWith(".variants.avro.gz")
                || name.endsWith(".variants.proto.gz")
                || name.endsWith(".variants.json.gz")) {
            int idx = name.lastIndexOf(".variants.");
            return name.substring(0, idx);
        } else {
            return null;
        }
    }

    private boolean isTransformedFile(String name) {
        return getOriginalFile(name) != null;
    }

    private String getMetaFile(String path) {
        String file = getOriginalFile(path);
        if (file != null) {
            return file + ".file.json.gz";
        } else {
            return null;
        }
    }

    private QueryResult<File> getParents(boolean rootFirst, QueryOptions options, String filePath, long studyId) throws CatalogException {
        List<String> paths = getParentPaths(filePath);

        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), paths);
        query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        QueryResult<File> result = fileDBAdaptor.get(query, options);
        result.getResult().sort(rootFirst ? ROOT_FIRST_COMPARATOR : ROOT_LAST_COMPARATOR);
        return result;
    }

    private String getParentPath(String path) {
        Path parent = Paths.get(path).getParent();
        String parentPath;
        if (parent == null) {   //If parent == null, the file is in the root of the study
            parentPath = "";
        } else {
            parentPath = parent.toString() + "/";
        }
        return parentPath;
    }

    /**
     * Get the URI where a file should be in Catalog, given a study and a path.
     *
     * @param studyId   Study identifier
     * @param path      Path to locate
     * @param directory Boolean indicating if the file is a directory
     * @return URI where the file should be placed
     * @throws CatalogException CatalogException
     */
    private URI getFileUri(long studyId, String path, boolean directory) throws CatalogException, URISyntaxException {
        // Get the closest existing parent. If parents == true, may happen that the parent is not registered in catalog yet.
        File existingParent = getParents(false, null, path, studyId).first();

        //Relative path to the existing parent
        String relativePath = Paths.get(existingParent.getPath()).relativize(Paths.get(path)).toString();
        if (path.endsWith("/") && !relativePath.endsWith("/")) {
            relativePath += "/";
        }

        String uriStr = Paths.get(existingParent.getUri().getPath()).resolve(relativePath).toString();

        if (directory) {
            return UriUtils.createDirectoryUri(uriStr);
        } else {
            return UriUtils.createUri(uriStr);
        }
    }

    private boolean isExternal(Study study, String catalogFilePath, URI fileUri) throws CatalogException {
        URI studyUri = study.getUri();

        String studyFilePath = studyUri.resolve(catalogFilePath).getPath();
        String originalFilePath = fileUri.getPath();

        logger.info("Study file path: {}", studyFilePath);
        logger.info("File path: {}", originalFilePath);
        return !studyFilePath.equals(originalFilePath);
    }

    private FileTree getTree(File folder, Query query, QueryOptions queryOptions, int maxDepth, long studyId, String userId)
            throws CatalogDBException {

        if (maxDepth == 0) {
            return null;
        }

        try {
            authorizationManager.checkFilePermission(studyId, folder.getUid(), userId, FileAclEntry.FilePermissions.VIEW);
        } catch (CatalogException e) {
            return null;
        }

        // Update the new path to be looked for
        query.put(FileDBAdaptor.QueryParams.DIRECTORY.key(), folder.getPath());

        FileTree fileTree = new FileTree(folder);
        List<FileTree> children = new ArrayList<>();

        // Obtain the files and directories inside the directory
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, queryOptions);

        for (File fileAux : fileQueryResult.getResult()) {
            if (fileAux.getType().equals(File.Type.DIRECTORY)) {
                FileTree subTree = getTree(fileAux, query, queryOptions, maxDepth - 1, studyId, userId);
                if (subTree != null) {
                    children.add(subTree);
                }
            } else {
                try {
                    authorizationManager.checkFilePermission(studyId, fileAux.getUid(), userId, FileAclEntry.FilePermissions.VIEW);
                    children.add(new FileTree(fileAux));
                } catch (CatalogException e) {
                    continue;
                }
            }
        }
        fileTree.setChildren(children);

        return fileTree;
    }

    private int countFilesInTree(FileTree fileTree) {
        int count = 1;
        for (FileTree tree : fileTree.getChildren()) {
            count += countFilesInTree(tree);
        }
        return count;
    }

    /**
     * Method to check if a files matching a query can be deleted. It will only be possible to delete files as long as they are not indexed.
     *
     * @param query          Query object.
     * @param physicalDelete boolean indicating whether the files matching the query should be completely deleted from the file system or
     *                       they should be sent to the trash bin.
     * @param studyId        Study where the query will be applied.
     * @param userId         user for which DELETE permissions will be checked.
     * @return the list of files scanned that can be deleted.
     * @throws CatalogException if any of the files cannot be deleted.
     */
    public List<File> checkCanDeleteFiles(Query query, boolean physicalDelete, long studyId, String userId) throws CatalogException {
        String statusQuery = physicalDelete ? GET_NON_DELETED_FILES : GET_NON_TRASHED_FILES;

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.UID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.TYPE.key(), FileDBAdaptor.QueryParams.RELATED_FILES.key(),
                FileDBAdaptor.QueryParams.SIZE.key(), FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.PATH.key(),
                FileDBAdaptor.QueryParams.INDEX.key(), FileDBAdaptor.QueryParams.STATUS.key(), FileDBAdaptor.QueryParams.EXTERNAL.key()));
        Query myQuery = new Query(query);
        myQuery.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId);
        myQuery.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), statusQuery);

        QueryResult<File> fileQueryResult = fileDBAdaptor.get(myQuery, options);

        return checkCanDeleteFiles(fileQueryResult.getResult().iterator(), physicalDelete, String.valueOf(studyId), userId);
    }

    public List<File> checkCanDeleteFiles(Iterator<File> fileIterator, boolean physicalDelete, String studyStr, String userId)
            throws CatalogException {
        List<File> filesChecked = new LinkedList<>();

        while (fileIterator.hasNext()) {
            filesChecked.addAll(checkCanDeleteFile(studyStr, fileIterator.next(), physicalDelete, userId));
        }

        return filesChecked;
    }

    public List<File> checkCanDeleteFile(String studyStr, File file, boolean physicalDelete, String userId) throws CatalogException {
        String statusQuery = physicalDelete ? GET_NON_DELETED_FILES : GET_NON_TRASHED_FILES;

        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.UID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.TYPE.key(), FileDBAdaptor.QueryParams.RELATED_FILES.key(),
                FileDBAdaptor.QueryParams.SIZE.key(), FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.PATH.key(),
                FileDBAdaptor.QueryParams.INDEX.key(), FileDBAdaptor.QueryParams.STATUS.key(), FileDBAdaptor.QueryParams.EXTERNAL.key()));

        List<File> filesToAnalyse = new LinkedList<>();

        if (file.getType() == File.Type.FILE) {
            filesToAnalyse.add(file);
        } else {
            // We cannot delete the root folder
            if (isRootFolder(file)) {
                throw new CatalogException("Root directories cannot be deleted");
            }

            // Get all recursive files and folders
            Query newQuery = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), statusQuery);
            QueryResult<File> recursiveFileQueryResult = fileDBAdaptor.get(newQuery, options);

            if (file.isExternal()) {
                // Check there aren't local files within the directory
                List<String> wrongFiles = new LinkedList<>();
                for (File nestedFile : recursiveFileQueryResult.getResult()) {
                    if (!nestedFile.isExternal()) {
                        wrongFiles.add(nestedFile.getPath());
                    }
                }
                if (!wrongFiles.isEmpty()) {
                    throw new CatalogException("Local files {" + StringUtils.join(wrongFiles, ", ") + "} detected within the external "
                            + "folder " + file.getPath() + ". Please, delete those folders or files manually first");
                }
            } else {
                // Check there aren't external files within the directory
                List<String> wrongFiles = new LinkedList<>();
                for (File nestedFile : recursiveFileQueryResult.getResult()) {
                    if (nestedFile.isExternal()) {
                        wrongFiles.add(nestedFile.getPath());
                    }
                }
                if (!wrongFiles.isEmpty()) {
                    throw new CatalogException("External files {" + StringUtils.join(wrongFiles, ", ") + "} detected within the local "
                            + "folder " + file.getPath() + ". Please, unlink those folders or files manually first");
                }
            }

            filesToAnalyse.addAll(recursiveFileQueryResult.getResult());
        }

        // If the user is the owner or the admin, we won't check if he has permissions for every single file
        boolean checkPermissions = !authorizationManager.checkIsOwnerOrAdmin(study.getUid(), userId);

        Set<Long> transformedFromFileIds = new HashSet<>();

        for (File fileAux : filesToAnalyse) {
            if (checkPermissions) {
                authorizationManager.checkFilePermission(study.getUid(), fileAux.getUid(), userId, FileAclEntry.FilePermissions.DELETE);
            }

            // Check the file status is not STAGE or MISSING
            if (fileAux.getStatus() == null) {
                throw new CatalogException("Cannot check file status for deletion");
            }
            if (File.FileStatus.STAGE.equals(fileAux.getStatus().getName())
                    || File.FileStatus.MISSING.equals(fileAux.getStatus().getName())) {
                throw new CatalogException("Cannot delete file: " + fileAux.getName() + ". The status is " + fileAux.getStatus().getName());
            }

            // Check the index status
            if (fileAux.getIndex() != null && fileAux.getIndex().getStatus() != null
                    && !FileIndex.IndexStatus.NONE.equals(fileAux.getIndex().getStatus().getName())
                    && !FileIndex.IndexStatus.TRANSFORMED.equals(fileAux.getIndex().getStatus().getName())) {
                throw new CatalogException("Cannot delete file: " + fileAux.getName() + ". The index status is "
                        + fileAux.getIndex().getStatus().getName());
            }

            // Check if the file is produced from other file being indexed and add them to the transformedFromFileIds set
            if (fileAux.getRelatedFiles() != null && !fileAux.getRelatedFiles().isEmpty()) {
                transformedFromFileIds.addAll(
                        fileAux.getRelatedFiles().stream()
                                .filter(myFile -> myFile.getRelation() == File.RelatedFile.Relation.PRODUCED_FROM)
                                .map(File.RelatedFile::getFileId)
                                .collect(Collectors.toSet())
                );
            }
        }

        // Check the original files are not being indexed at the moment
        if (!transformedFromFileIds.isEmpty()) {
            Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(transformedFromFileIds));
            try (DBIterator<File> iterator = fileDBAdaptor.iterator(query, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FileDBAdaptor.QueryParams.INDEX.key(), FileDBAdaptor.QueryParams.UID.key())))) {
                while (iterator.hasNext()) {
                    File next = iterator.next();
                    String status = next.getIndex().getStatus().getName();
                    switch (status) {
                        case FileIndex.IndexStatus.READY:
                            // If they are already ready, we only need to remove the reference to the transformed files as they will be
                            // removed
                            next.getIndex().setTransformedFile(null);
                            break;
                        case FileIndex.IndexStatus.TRANSFORMED:
                            // We need to remove the reference to the transformed files and change their status from TRANSFORMED to NONE
                            next.getIndex().setTransformedFile(null);
                            next.getIndex().getStatus().setName(FileIndex.IndexStatus.NONE);
                            break;
                        case FileIndex.IndexStatus.NONE:
                        case FileIndex.IndexStatus.DELETED:
                            break;
                        default:
                            throw new CatalogException("Cannot delete files that are in use in storage.");
                    }
                }
            }
        }

        return filesToAnalyse;
    }

    public FacetedQueryResult facet(String studyStr, Query query, QueryOptions queryOptions, boolean defaultStats, String sessionId)
            throws CatalogException, IOException {
        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(queryOptions, QueryOptions::new);

        if (defaultStats) {
            String facet = queryOptions.getString(QueryOptions.FACET);
            String facetRange = queryOptions.getString(QueryOptions.FACET_RANGE);
            queryOptions.put(QueryOptions.FACET, StringUtils.isNotEmpty(facet) ? defaultFacet + ";" + facet : defaultFacet);
            queryOptions.put(QueryOptions.FACET_RANGE, StringUtils.isNotEmpty(facetRange) ? defaultFacetRange + ";" + facetRange
                    : defaultFacetRange);
        }

        CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager);

        String userId = userManager.getUserId(sessionId);
        // We need to add variableSets and groups to avoid additional queries as it will be used in the catalogSolrManager
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key())));

        AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

        return catalogSolrManager.facetedQuery(study, CatalogSolrManager.FILE_SOLR_COLLECTION, query, queryOptions, userId);
    }

    private void updateIndexStatusAfterDeletionOfTransformedFile(long studyId, File file) throws CatalogDBException {
        if (file.getType() == File.Type.FILE && (file.getRelatedFiles() == null || file.getRelatedFiles().isEmpty())) {
            return;
        }

        // We check if any of the files to be removed are transformation files
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                .append(FileDBAdaptor.QueryParams.RELATED_FILES_RELATION.key(), File.RelatedFile.Relation.PRODUCED_FROM);
        QueryResult<File> fileQR = fileDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE,
                FileDBAdaptor.QueryParams.RELATED_FILES.key()));
        if (fileQR.getNumResults() > 0) {
            // Among the files to be deleted / unlinked, there are transformed files. We need to check that these files are not being used
            // anymore.
            Set<Long> fileIds = new HashSet<>();
            for (File transformedFile : fileQR.getResult()) {
                fileIds.addAll(
                        transformedFile.getRelatedFiles().stream()
                                .filter(myFile -> myFile.getRelation() == File.RelatedFile.Relation.PRODUCED_FROM)
                                .map(File.RelatedFile::getFileId)
                                .collect(Collectors.toSet())
                );
            }

            // Update the original files to remove the transformed file
            query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(fileIds));
            Map<Long, FileIndex> filesToUpdate;
            try (DBIterator<File> iterator = fileDBAdaptor.iterator(query, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FileDBAdaptor.QueryParams.INDEX.key(), FileDBAdaptor.QueryParams.UID.key())))) {
                filesToUpdate = new HashMap<>();
                while (iterator.hasNext()) {
                    File next = iterator.next();
                    String status = next.getIndex().getStatus().getName();
                    switch (status) {
                        case FileIndex.IndexStatus.READY:
                            // If they are already ready, we only need to remove the reference to the transformed files as they will be
                            // removed
                            next.getIndex().setTransformedFile(null);
                            filesToUpdate.put(next.getUid(), next.getIndex());
                            break;
                        case FileIndex.IndexStatus.TRANSFORMED:
                            // We need to remove the reference to the transformed files and change their status from TRANSFORMED to NONE
                            next.getIndex().setTransformedFile(null);
                            next.getIndex().getStatus().setName(FileIndex.IndexStatus.NONE);
                            filesToUpdate.put(next.getUid(), next.getIndex());
                            break;
                        default:
                            break;
                    }
                }
            }

            for (Map.Entry<Long, FileIndex> indexEntry : filesToUpdate.entrySet()) {
                fileDBAdaptor.update(indexEntry.getKey(), new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), indexEntry.getValue()),
                        QueryOptions.empty());
            }
        }
    }

    /**
     * Create the parent directories that are needed.
     *
     * @param study            study where they will be created.
     * @param userId           user that is creating the parents.
     * @param studyURI         Base URI where the created folders will be pointing to. (base physical location)
     * @param path             Path used in catalog as a virtual location. (whole bunch of directories inside the virtual
     *                         location in catalog)
     * @param checkPermissions Boolean indicating whether to check if the user has permissions to create a folder in the first directory
     *                         that is available in catalog.
     * @throws CatalogDBException
     */
    private void createParents(Study study, String userId, URI studyURI, Path path, boolean checkPermissions) throws CatalogException {
        if (path == null) {
            if (checkPermissions) {
                authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_FILES);
            }
            return;
        }

        String stringPath = path.toString();
        if (("/").equals(stringPath)) {
            return;
        }

        logger.info("Path: {}", stringPath);

        if (stringPath.startsWith("/")) {
            stringPath = stringPath.substring(1);
        }

        if (!stringPath.endsWith("/")) {
            stringPath = stringPath + "/";
        }

        // Check if the folder exists
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.PATH.key(), stringPath);

        if (fileDBAdaptor.count(query).first() == 0) {
            createParents(study, userId, studyURI, path.getParent(), checkPermissions);
        } else {
            if (checkPermissions) {
                long fileId = fileDBAdaptor.getId(study.getUid(), stringPath);
                authorizationManager.checkFilePermission(study.getUid(), fileId, userId, FileAclEntry.FilePermissions.WRITE);
            }
            return;
        }

        String parentPath = getParentPath(stringPath);
        long parentFileId = fileDBAdaptor.getId(study.getUid(), parentPath);
        // We obtain the permissions set in the parent folder and set them to the file or folder being created
        QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), parentFileId, userId, checkPermissions);

        URI completeURI = Paths.get(studyURI).resolve(path).toUri();

        // Create the folder in catalog
        File folder = new File(path.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, completeURI,
                stringPath, null, TimeUtils.getTime(), TimeUtils.getTime(), "", new File.FileStatus(File.FileStatus.READY), false, 0, null,
                new Experiment(), Collections.emptyList(), new Job(), Collections.emptyList(), null,
                catalogManager.getStudyManager().getCurrentRelease(study, userId), Collections.emptyList(), null, null);
        folder.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FILE));
        checkHooks(folder, study.getFqn(), HookConfiguration.Stage.CREATE);
        QueryResult<File> queryResult = fileDBAdaptor.insert(study.getUid(), folder, Collections.emptyList(), new QueryOptions());
        // Propagate ACLs
        if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(study.getUid(), Arrays.asList(queryResult.first().getUid()), allFileAcls.getResult(),
                    Entity.FILE);
        }
    }

    private QueryResult<File> privateLink(Study study, URI uriOrigin, String pathDestiny, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
        params = ParamUtils.defaultObject(params, ObjectMap::new);
        CatalogIOManager ioManager = catalogIOManagerFactory.get(uriOrigin);
        if (!ioManager.exists(uriOrigin)) {
            throw new CatalogIOException("File " + uriOrigin + " does not exist");
        }

        final URI normalizedUri;
        try {
            normalizedUri = UriUtils.createUri(uriOrigin.normalize().getPath());
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }

        String userId = userManager.getUserId(sessionId);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_FILES);

        pathDestiny = ParamUtils.defaultString(pathDestiny, "");
        if (pathDestiny.length() == 1 && (pathDestiny.equals(".") || pathDestiny.equals("/"))) {
            pathDestiny = "";
        } else {
            if (pathDestiny.startsWith("/")) {
                pathDestiny = pathDestiny.substring(1);
            }
            if (!pathDestiny.isEmpty() && !pathDestiny.endsWith("/")) {
                pathDestiny = pathDestiny + "/";
            }
        }
        String externalPathDestinyStr;
        if (Paths.get(normalizedUri).toFile().isDirectory()) {
            externalPathDestinyStr = Paths.get(pathDestiny).resolve(Paths.get(normalizedUri).getFileName()).toString() + "/";
        } else {
            externalPathDestinyStr = Paths.get(pathDestiny).resolve(Paths.get(normalizedUri).getFileName()).toString();
        }

        // Check if the path already exists and is not external
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + Status.DELETED + ";!="
                        + File.FileStatus.REMOVED)
                .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), false);
        if (fileDBAdaptor.count(query).first() > 0) {
            throw new CatalogException("Cannot link to " + externalPathDestinyStr + ". The path already existed and is not external.");
        }

        // Check if the uri was already linked to that same path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), normalizedUri)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + Status.DELETED + ";!="
                        + File.FileStatus.REMOVED)
                .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);


        if (fileDBAdaptor.count(query).first() > 0) {
            // Create a regular expression on URI to return everything linked from that URI
            query.put(FileDBAdaptor.QueryParams.URI.key(), "~^" + normalizedUri);
            query.remove(FileDBAdaptor.QueryParams.PATH.key());

            // Limit the number of results and only some fields
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.LIMIT, 100);

            return fileDBAdaptor.get(query, queryOptions);
        }

        // Check if the uri was linked to other path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), normalizedUri)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + Status.DELETED + ";!="
                        + File.FileStatus.REMOVED)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);
        if (fileDBAdaptor.count(query).first() > 0) {
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
            String path = fileDBAdaptor.get(query, queryOptions).first().getPath();
            throw new CatalogException(normalizedUri + " was already linked to other path: " + path);
        }

        boolean parents = params.getBoolean("parents", false);
        // FIXME: Implement resync
        boolean resync = params.getBoolean("resync", false);
        String description = params.getString("description", "");
        String checksum = params.getString(FileDBAdaptor.QueryParams.CHECKSUM.key(), "");
        // Because pathDestiny can be null, we will use catalogPath as the virtual destiny where the files will be located in catalog.
        Path catalogPath = Paths.get(pathDestiny);

        if (pathDestiny.isEmpty()) {
            // If no destiny is given, everything will be linked to the root folder of the study.
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_FILES);
        } else {
            // Check if the folder exists
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), pathDestiny);
            if (fileDBAdaptor.count(query).first() == 0) {
                if (parents) {
                    // Get the base URI where the files are located in the study
                    URI studyURI = study.getUri();
                    createParents(study, userId, studyURI, catalogPath, true);
                    // Create them in the disk
//                    URI directory = Paths.get(studyURI).resolve(catalogPath).toUri();
//                    catalogIOManagerFactory.get(directory).createDirectory(directory, true);
                } else {
                    throw new CatalogException("The path " + catalogPath + " does not exist in catalog.");
                }
            } else {
                // Check if the user has permissions to link files in the directory
                long fileId = fileDBAdaptor.getId(study.getUid(), pathDestiny);
                authorizationManager.checkFilePermission(study.getUid(), fileId, userId, FileAclEntry.FilePermissions.WRITE);
            }
        }

        Path pathOrigin = Paths.get(normalizedUri);
        Path externalPathDestiny = Paths.get(externalPathDestinyStr);
        if (Paths.get(normalizedUri).toFile().isFile()) {

            // Check if there is already a file in the same path
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr);

            // Create the file
            if (fileDBAdaptor.count(query).first() == 0) {
                long size = Files.size(Paths.get(normalizedUri));

                String parentPath = getParentPath(externalPathDestinyStr);
                long parentFileId = fileDBAdaptor.getId(study.getUid(), parentPath);
                // We obtain the permissions set in the parent folder and set them to the file or folder being created
                QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), parentFileId, userId, true);

                File subfile = new File(externalPathDestiny.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                        File.Bioformat.NONE, normalizedUri, externalPathDestinyStr, checksum, TimeUtils.getTime(), TimeUtils.getTime(),
                        description, new File.FileStatus(File.FileStatus.READY), true, size, null, new Experiment(),
                        Collections.emptyList(), new Job(), Collections.emptyList(), null,
                        catalogManager.getStudyManager().getCurrentRelease(study, userId), Collections.emptyList(), Collections.emptyMap(),
                        Collections.emptyMap());
                subfile.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FILE));
                checkHooks(subfile, study.getFqn(), HookConfiguration.Stage.CREATE);
                QueryResult<File> queryResult = fileDBAdaptor.insert(study.getUid(), subfile, Collections.emptyList(), new QueryOptions());

                // Propagate ACLs
                if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                    authorizationManager.replicateAcls(study.getUid(), Arrays.asList(queryResult.first().getUid()), allFileAcls.getResult(),
                            Entity.FILE);
                }

                File file = this.file.setMetadataInformation(queryResult.first(), queryResult.first().getUri(),
                        new QueryOptions(), sessionId, false);
                queryResult.setResult(Arrays.asList(file));

                // If it is a transformed file, we will try to link it with the correspondent original file
                try {
                    if (isTransformedFile(file.getName())) {
                        matchUpVariantFiles(study.getFqn(), Arrays.asList(file), sessionId);
                    }
                } catch (CatalogException e) {
                    logger.warn("Matching avro to variant file: {}", e.getMessage());
                }

                return queryResult;
            } else {
                throw new CatalogException("Cannot link " + externalPathDestiny.getFileName().toString() + ". A file with the same name "
                        + "was found in the same path.");
            }
        } else {
            // This list will contain the list of transformed files detected during the link
            List<File> transformedFiles = new ArrayList<>();

            // We remove the / at the end for replacement purposes in the walkFileTree
            String finalExternalPathDestinyStr = externalPathDestinyStr.substring(0, externalPathDestinyStr.length() - 1);

            // Link all the files and folders present in the uri
            Files.walkFileTree(pathOrigin, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

                    try {
                        String destinyPath = dir.toString().replace(Paths.get(normalizedUri).toString(), finalExternalPathDestinyStr);

                        if (!destinyPath.isEmpty() && !destinyPath.endsWith("/")) {
                            destinyPath += "/";
                        }

                        if (destinyPath.startsWith("/")) {
                            destinyPath = destinyPath.substring(1);
                        }

                        Query query = new Query()
                                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                                .append(FileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                        if (fileDBAdaptor.count(query).first() == 0) {
                            // If the folder does not exist, we create it

                            String parentPath = getParentPath(destinyPath);
                            long parentFileId = fileDBAdaptor.getId(study.getUid(), parentPath);
                            // We obtain the permissions set in the parent folder and set them to the file or folder being created
                            QueryResult<FileAclEntry> allFileAcls;
                            try {
                                allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), parentFileId, userId, true);
                            } catch (CatalogException e) {
                                throw new RuntimeException(e);
                            }

                            File folder = new File(dir.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN,
                                    File.Bioformat.NONE, dir.toUri(), destinyPath, null, TimeUtils.getTime(),
                                    TimeUtils.getTime(), description, new File.FileStatus(File.FileStatus.READY), true, 0, null,
                                    new Experiment(), Collections.emptyList(), new Job(), Collections.emptyList(),
                                    null, catalogManager.getStudyManager().getCurrentRelease(study, userId), Collections.emptyList(),
                                    Collections.emptyMap(), Collections.emptyMap());
                            folder.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FILE));
                            checkHooks(folder, study.getFqn(), HookConfiguration.Stage.CREATE);
                            QueryResult<File> queryResult = fileDBAdaptor.insert(study.getUid(), folder, Collections.emptyList(),
                                    new QueryOptions());

                            // Propagate ACLs
                            if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                                authorizationManager.replicateAcls(study.getUid(), Arrays.asList(queryResult.first().getUid()),
                                        allFileAcls.getResult(), Entity.FILE);
                            }
                        }

                    } catch (CatalogException e) {
                        logger.error("An error occurred when trying to create folder {}", dir.toString());
//                        e.printStackTrace();
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) throws IOException {
                    try {
                        String destinyPath = filePath.toString().replace(Paths.get(normalizedUri).toString(), finalExternalPathDestinyStr);

                        if (destinyPath.startsWith("/")) {
                            destinyPath = destinyPath.substring(1);
                        }

                        Query query = new Query()
                                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                                .append(FileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                        if (fileDBAdaptor.count(query).first() == 0) {
                            long size = Files.size(filePath);
                            // If the file does not exist, we create it
                            String parentPath = getParentPath(destinyPath);
                            long parentFileId = fileDBAdaptor.getId(study.getUid(), parentPath);
                            // We obtain the permissions set in the parent folder and set them to the file or folder being created
                            QueryResult<FileAclEntry> allFileAcls;
                            try {
                                allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), parentFileId, userId, true);
                            } catch (CatalogException e) {
                                throw new RuntimeException(e);
                            }

                            File subfile = new File(filePath.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                                    File.Bioformat.NONE, filePath.toUri(), destinyPath, null, TimeUtils.getTime(),
                                    TimeUtils.getTime(), description, new File.FileStatus(File.FileStatus.READY), true, size, null,
                                    new Experiment(), Collections.emptyList(), new Job(), Collections.emptyList(),
                                    null, catalogManager.getStudyManager().getCurrentRelease(study, userId), Collections.emptyList(),
                                    Collections.emptyMap(), Collections.emptyMap());
                            subfile.setUuid(UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.FILE));
                            checkHooks(subfile, study.getFqn(), HookConfiguration.Stage.CREATE);
                            QueryResult<File> queryResult = fileDBAdaptor.insert(study.getUid(), subfile, Collections.emptyList(),
                                    new QueryOptions());

                            // Propagate ACLs
                            if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                                authorizationManager.replicateAcls(study.getUid(), Arrays.asList(queryResult.first().getUid()),
                                        allFileAcls.getResult(), Entity.FILE);
                            }

                            File file = FileManager.this.file.setMetadataInformation(queryResult.first(), queryResult.first().getUri(),
                                    new QueryOptions(), sessionId, false);
                            if (isTransformedFile(file.getName())) {
                                logger.info("Detected transformed file {}", file.getPath());
                                transformedFiles.add(file);
                            }
                        } else {
                            throw new CatalogException("Cannot link the file " + filePath.getFileName().toString()
                                    + ". There is already a file in the path " + destinyPath + " with the same name.");
                        }

                    } catch (CatalogException e) {
                        logger.error(e.getMessage());
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.SKIP_SUBTREE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    return FileVisitResult.CONTINUE;
                }
            });

            // Try to link transformed files with their corresponding original files if any
            try {
                if (transformedFiles.size() > 0) {
                    matchUpVariantFiles(study.getFqn(), transformedFiles, sessionId);
                }
            } catch (CatalogException e) {
                logger.warn("Matching avro to variant file: {}", e.getMessage());
            }

            // Check if the uri was already linked to that same path
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.URI.key(), "~^" + normalizedUri)
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";!=" + Status.DELETED + ";!="
                            + File.FileStatus.REMOVED)
                    .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);

            // Limit the number of results and only some fields
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.LIMIT, 100);
            return fileDBAdaptor.get(query, queryOptions);
        }
    }

    private void checkHooks(File file, String fqn, HookConfiguration.Stage stage) throws CatalogException {

        Map<String, Map<String, List<HookConfiguration>>> hooks = this.configuration.getHooks();
        if (hooks != null && hooks.containsKey(fqn)) {
            Map<String, List<HookConfiguration>> entityHookMap = hooks.get(fqn);
            List<HookConfiguration> hookList = null;
            if (entityHookMap.containsKey(MongoDBAdaptorFactory.FILE_COLLECTION)) {
                hookList = entityHookMap.get(MongoDBAdaptorFactory.FILE_COLLECTION);
            } else if (entityHookMap.containsKey(MongoDBAdaptorFactory.FILE_COLLECTION.toUpperCase())) {
                hookList = entityHookMap.get(MongoDBAdaptorFactory.FILE_COLLECTION.toUpperCase());
            }

            // We check the hook list
            if (hookList != null) {
                for (HookConfiguration hookConfiguration : hookList) {
                    if (hookConfiguration.getStage() != stage) {
                        continue;
                    }

                    String field = hookConfiguration.getField();
                    if (StringUtils.isEmpty(field)) {
                        logger.warn("Missing 'field' field from hook configuration");
                        continue;
                    }
                    field = field.toLowerCase();

                    String filterValue = hookConfiguration.getValue();
                    if (StringUtils.isEmpty(filterValue)) {
                        logger.warn("Missing 'value' field from hook configuration");
                        continue;
                    }

                    String value = null;
                    switch (field) {
                        case "name":
                            value = file.getName();
                            break;
                        case "format":
                            value = file.getFormat().name();
                            break;
                        case "bioformat":
                            value = file.getFormat().name();
                            break;
                        case "path":
                            value = file.getPath();
                            break;
                        case "description":
                            value = file.getDescription();
                            break;
                        // TODO: At some point, we will also have to consider any field that is not a String
//                        case "size":
//                            value = file.getSize();
//                            break;
                        default:
                            break;
                    }
                    if (value == null) {
                        continue;
                    }

                    String filterNewValue = hookConfiguration.getWhat();
                    if (StringUtils.isEmpty(filterNewValue)) {
                        logger.warn("Missing 'what' field from hook configuration");
                        continue;
                    }

                    String filterWhere = hookConfiguration.getWhere();
                    if (StringUtils.isEmpty(filterWhere)) {
                        logger.warn("Missing 'where' field from hook configuration");
                        continue;
                    }
                    filterWhere = filterWhere.toLowerCase();


                    if (filterValue.startsWith("~")) {
                        // Regular expression
                        if (!value.matches(filterValue.substring(1))) {
                            // If it doesn't match, we will check the next hook of the loop
                            continue;
                        }
                    } else {
                        if (!value.equals(filterValue)) {
                            // If it doesn't match, we will check the next hook of the loop
                            continue;
                        }
                    }

                    // The value matched, so we will perform the action desired by the user
                    if (hookConfiguration.getAction() == HookConfiguration.Action.ABORT) {
                        throw new CatalogException("A hook to abort the insertion matched");
                    }

                    // We check the field the user wants to update
                    if (filterWhere.equals(FileDBAdaptor.QueryParams.DESCRIPTION.key())) {
                        switch (hookConfiguration.getAction()) {
                            case ADD:
                            case SET:
                                file.setDescription(hookConfiguration.getWhat());
                                break;
                            case REMOVE:
                                file.setDescription("");
                                break;
                            default:
                                break;
                        }
                    } else if (filterWhere.equals(FileDBAdaptor.QueryParams.TAGS.key())) {
                        switch (hookConfiguration.getAction()) {
                            case ADD:
                                List<String> values;
                                if (hookConfiguration.getWhat().contains(",")) {
                                    values = Arrays.asList(hookConfiguration.getWhat().split(","));
                                } else {
                                    values = Collections.singletonList(hookConfiguration.getWhat());
                                }
                                List<String> tagsCopy = new ArrayList<>();
                                if (file.getTags() != null) {
                                    tagsCopy.addAll(file.getTags());
                                }
                                tagsCopy.addAll(values);
                                file.setTags(tagsCopy);
                                break;
                            case SET:
                                if (hookConfiguration.getWhat().contains(",")) {
                                    values = Arrays.asList(hookConfiguration.getWhat().split(","));
                                } else {
                                    values = Collections.singletonList(hookConfiguration.getWhat());
                                }
                                file.setTags(values);
                                break;
                            case REMOVE:
                                file.setTags(Collections.emptyList());
                                break;
                            default:
                                break;
                        }
                    } else if (filterWhere.startsWith(FileDBAdaptor.QueryParams.STATS.key())) {
                        String[] split = StringUtils.split(filterWhere, ".", 2);
                        String statsField = null;
                        if (split.length == 2) {
                            statsField = split[1];
                        }

                        switch (hookConfiguration.getAction()) {
                            case ADD:
                                if (statsField == null) {
                                    logger.error("Cannot add a value to {} directly. Expected {}.<subfield>",
                                            FileDBAdaptor.QueryParams.STATS.key(), FileDBAdaptor.QueryParams.STATS.key());
                                    continue;
                                }

                                List<String> values;
                                if (hookConfiguration.getWhat().contains(",")) {
                                    values = Arrays.asList(hookConfiguration.getWhat().split(","));
                                } else {
                                    values = Collections.singletonList(hookConfiguration.getWhat());
                                }

                                Object currentStatsValue = file.getStats().get(statsField);
                                if (currentStatsValue == null) {
                                    file.getStats().put(statsField, values);
                                } else if (currentStatsValue instanceof Collection) {
                                    ((List) currentStatsValue).addAll(values);
                                } else {
                                    logger.error("Cannot add a value to {} if it is not an array", filterWhere);
                                    continue;
                                }

                                break;
                            case SET:
                                if (statsField == null) {
                                    logger.error("Cannot set a value to {} directly. Expected {}.<subfield>",
                                            FileDBAdaptor.QueryParams.STATS.key(), FileDBAdaptor.QueryParams.STATS.key());
                                    continue;
                                }

                                if (hookConfiguration.getWhat().contains(",")) {
                                    values = Arrays.asList(hookConfiguration.getWhat().split(","));
                                } else {
                                    values = Collections.singletonList(hookConfiguration.getWhat());
                                }
                                file.getStats().put(statsField, values);
                                break;
                            case REMOVE:
                                if (statsField == null) {
                                    file.setStats(Collections.emptyMap());
                                } else {
                                    file.getStats().remove(statsField);
                                }
                                break;
                            default:
                                break;
                        }
                    } else if (filterWhere.startsWith(FileDBAdaptor.QueryParams.ATTRIBUTES.key())) {
                        String[] split = StringUtils.split(filterWhere, ".", 2);
                        String attributesField = null;
                        if (split.length == 2) {
                            attributesField = split[1];
                        }

                        switch (hookConfiguration.getAction()) {
                            case ADD:
                                if (attributesField == null) {
                                    logger.error("Cannot add a value to {} directly. Expected {}.<subfield>",
                                            FileDBAdaptor.QueryParams.ATTRIBUTES.key(), FileDBAdaptor.QueryParams.ATTRIBUTES.key());
                                    continue;
                                }

                                List<String> values;
                                if (hookConfiguration.getWhat().contains(",")) {
                                    values = Arrays.asList(hookConfiguration.getWhat().split(","));
                                } else {
                                    values = Collections.singletonList(hookConfiguration.getWhat());
                                }

                                Object currentStatsValue = file.getAttributes().get(attributesField);
                                if (currentStatsValue == null) {
                                    file.getAttributes().put(attributesField, values);
                                } else if (currentStatsValue instanceof Collection) {
                                    ((List) currentStatsValue).addAll(values);
                                } else {
                                    logger.error("Cannot add a value to {} if it is not an array", filterWhere);
                                    continue;
                                }
                                break;
                            case SET:
                                if (attributesField == null) {
                                    logger.error("Cannot set a value to {} directly. Expected {}.<subfield>",
                                            FileDBAdaptor.QueryParams.ATTRIBUTES.key(), FileDBAdaptor.QueryParams.ATTRIBUTES.key());
                                    continue;
                                }

                                if (hookConfiguration.getWhat().contains(",")) {
                                    values = Arrays.asList(hookConfiguration.getWhat().split(","));
                                } else {
                                    values = Collections.singletonList(hookConfiguration.getWhat());
                                }
                                file.getAttributes().put(attributesField, values);
                                break;
                            case REMOVE:
                                if (attributesField == null) {
                                    file.setAttributes(Collections.emptyMap());
                                } else {
                                    file.getAttributes().remove(attributesField);
                                }
                                break;
                            default:
                                break;
                        }
                    } else {
                        logger.error("{} field cannot be updated. Please, check the hook configured.", hookConfiguration.getWhere());
                    }
                }
            }
        }
    }

    private URI getStudyUri(long studyId) throws CatalogException {
        return studyDBAdaptor.get(studyId, INCLUDE_STUDY_URI).first().getUri();
    }

    private enum CheckPath {
        FREE_PATH, FILE_EXISTS, DIRECTORY_EXISTS
    }

    private CheckPath checkPathExists(String path, long studyId) throws CatalogDBException {
        String myPath = path;
        if (myPath.endsWith("/")) {
            myPath = myPath.substring(0, myPath.length() - 1);
        }

        // We first look for any file called the same way the directory needs to be called
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), myPath);
        QueryResult<Long> fileQueryResult = fileDBAdaptor.count(query);
        if (fileQueryResult.first() > 0) {
            return CheckPath.FILE_EXISTS;
        }

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), myPath + "/");
        fileQueryResult = fileDBAdaptor.count(query);

        return fileQueryResult.first() > 0 ? CheckPath.DIRECTORY_EXISTS : CheckPath.FREE_PATH;
    }
}
