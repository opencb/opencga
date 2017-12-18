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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.stats.VariantSetStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.monitor.daemons.IndexDaemon;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.Entity;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.core.models.acls.permissions.StudyAclEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.utils.FileMetadataReader.VARIANT_FILE_STATS;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileManager extends ResourceManager<File> {

    private static final QueryOptions INCLUDE_STUDY_URI;
    private static final QueryOptions INCLUDE_FILE_URI_PATH;
    private static final Comparator<File> ROOT_FIRST_COMPARATOR;
    private static final Comparator<File> ROOT_LAST_COMPARATOR;

    protected static Logger logger;
    private FileMetadataReader fileMetadataReader;
    private UserManager userManager;

    public static final String SKIP_TRASH = "SKIP_TRASH";
    public static final String DELETE_EXTERNAL_FILES = "DELETE_EXTERNAL_FILES";
    public static final String FORCE_DELETE = "FORCE_DELETE";

    static {
        INCLUDE_STUDY_URI = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key());
        INCLUDE_FILE_URI_PATH = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.PATH.key()));
        ROOT_FIRST_COMPARATOR = (f1, f2) -> (f1.getPath() == null ? 0 : f1.getPath().length())
                - (f2.getPath() == null ? 0 : f2.getPath().length());
        ROOT_LAST_COMPARATOR = (f1, f2) -> (f2.getPath() == null ? 0 : f2.getPath().length())
                - (f1.getPath() == null ? 0 : f1.getPath().length());

        logger = LoggerFactory.getLogger(FileManager.class);
    }

    FileManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
        fileMetadataReader = new FileMetadataReader(this.catalogManager);
        this.userManager = catalogManager.getUserManager();
    }

    public URI getUri(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            QueryResult<File> fileQueryResult = fileDBAdaptor.get(file.getId(), INCLUDE_STUDY_URI);
            if (fileQueryResult.getNumResults() == 0) {
                throw new CatalogException("File " + file.getId() + " not found");
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
                String relativePath = filePath.replaceFirst(parent.getPath(), "");
                return Paths.get(parent.getUri()).resolve(relativePath).toUri();
            }
        }
        URI studyUri = getStudyUri(studyId);
        return filePath.isEmpty()
                ? studyUri
                : catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, filePath);
    }

    @Override
    public Long getStudyId(long fileId) throws CatalogException {
        return fileDBAdaptor.getStudyIdByFileId(fileId);
    }

    @Override
    public MyResourceId getId(String fileStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(fileStr)) {
            throw new CatalogException("Missing file parameter");
        }

        String userId;
        long studyId;
        long fileId;

        if (StringUtils.isNumeric(fileStr) && Long.parseLong(fileStr) > configuration.getCatalog().getOffset()) {
            fileId = Long.parseLong(fileStr);
            fileDBAdaptor.exists(fileId);
            studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
            userId = userManager.getUserId(sessionId);
        } else {
            if (fileStr.contains(",")) {
                throw new CatalogException("More than one file found");
            }

            userId = userManager.getUserId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            fileId = smartResolutor(fileStr, studyId);
        }

        return new MyResourceId(userId, studyId, fileId);
    }

    @Override
    public MyResourceIds getIds(List<String> fileList, @Nullable String studyStr, boolean silent, String sessionId)
            throws CatalogException {
        if (fileList == null || fileList.isEmpty()) {
            throw new CatalogException("Missing file parameter");
        }

        String userId;
        long studyId;
        List<Long> fileIds = new ArrayList<>();

        if (fileList.size() == 1 && StringUtils.isNumeric(fileList.get(0))
                && Long.parseLong(fileList.get(0)) > configuration.getCatalog().getOffset()) {
            fileIds.add(Long.parseLong(fileList.get(0)));
            fileDBAdaptor.exists(fileIds.get(0));
            studyId = fileDBAdaptor.getStudyIdByFileId(fileIds.get(0));
            userId = userManager.getUserId(sessionId);
        } else {
            userId = userManager.getUserId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            for (String fileStrAux : fileList) {
                try {
                    fileIds.add(smartResolutor(fileStrAux, studyId));
                } catch (CatalogException e) {
                    if (silent) {
                        fileIds.add(-1L);
                    } else {
                        throw e;
                    }
                }
            }
        }
        return new MyResourceIds(userId, studyId, fileIds);
    }

    public void matchUpVariantFiles(List<File> transformedFiles, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        for (File transformedFile : transformedFiles) {
            Long studyId = getStudyId(transformedFile.getId());
            authorizationManager.checkFilePermission(studyId, transformedFile.getId(), userId, FileAclEntry.FilePermissions.WRITE);
            String variantPathName = getOriginalFile(transformedFile.getPath());
            if (variantPathName == null) {
                // Skip the file.
                logger.warn("The file {} is not a variant transformed file", transformedFile.getName());
                continue;
            }

            logger.info("Looking for vcf file in path {}", variantPathName);
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), variantPathName)
                    .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);

            QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

            if (fileQueryResult.getNumResults() == 0) {
                // Search in the whole study
                String variantFileName = getOriginalFile(transformedFile.getName());
                logger.info("Looking for vcf file by name {}", variantFileName);
                query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.NAME.key(), variantFileName)
                        .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);
                fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());
            }

            if (fileQueryResult.getNumResults() > 0) {
                List<File> fileList = new ArrayList<>(fileQueryResult.getNumResults());
                List<String> acceptedStatus = Arrays.asList(FileIndex.IndexStatus.NONE, FileIndex.IndexStatus.TRANSFORMING,
                        FileIndex.IndexStatus.INDEXING, FileIndex.IndexStatus.READY);
                // Check index status
                for (File file : fileQueryResult.getResult()) {
                    if (file.getIndex() == null || file.getIndex().getStatus() == null || file.getIndex().getStatus().getName() == null
                            || acceptedStatus.contains(file.getIndex().getStatus().getName())) {
                        fileList.add(file);
                    }
                }
                fileQueryResult.setResult(fileList);
                fileQueryResult.setNumResults(fileList.size());
            }

            if (fileQueryResult.getNumResults() == 0 || fileQueryResult.getNumResults() > 1) {
                // VCF file not found
                logger.warn("The vcf file corresponding to the file " + transformedFile.getName() + " could not be found");
                continue;
            }
            File vcf = fileQueryResult.first();

            // Look for the json file. It should be in the same directory where the transformed file is.
            String jsonPathName = getMetaFile(transformedFile.getPath());
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), jsonPathName)
                    .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON);
            fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());
            if (fileQueryResult.getNumResults() != 1) {
                // Skip. This should not ever happen
                logger.warn("The json file corresponding to the file " + transformedFile.getName() + " could not be found");
                continue;
            }
            File json = fileQueryResult.first();

            /* Update relations */

            // Update json file
            logger.debug("Updating json relation");
            List<File.RelatedFile> relatedFiles = json.getRelatedFiles();
            if (relatedFiles == null) {
                relatedFiles = new ArrayList<>();
            }
            relatedFiles.add(new File.RelatedFile(vcf.getId(), File.RelatedFile.Relation.PRODUCED_FROM));
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFiles);
            fileDBAdaptor.update(json.getId(), params, QueryOptions.empty());

            // Update transformed file
            logger.debug("Updating transformed relation");
            relatedFiles = transformedFile.getRelatedFiles();
            if (relatedFiles == null) {
                relatedFiles = new ArrayList<>();
            }
            relatedFiles.add(new File.RelatedFile(vcf.getId(), File.RelatedFile.Relation.PRODUCED_FROM));
            params = new ObjectMap(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFiles);
            fileDBAdaptor.update(transformedFile.getId(), params, QueryOptions.empty());

            // Update vcf file
            logger.debug("Updating vcf relation");
            FileIndex index = vcf.getIndex();
            if (index.getTransformedFile() == null) {
                index.setTransformedFile(new FileIndex.TransformedFile(transformedFile.getId(), json.getId()));
            }
            String status = FileIndex.IndexStatus.NONE;
            if (vcf.getIndex() != null && vcf.getIndex().getStatus() != null && vcf.getIndex().getStatus().getName() != null) {
                status = vcf.getIndex().getStatus().getName();
            }
            if (FileIndex.IndexStatus.NONE.equals(status)) {
                // If TRANSFORMED, TRANSFORMING, etc, do not modify the index status
                index.setStatus(new FileIndex.IndexStatus(FileIndex.IndexStatus.TRANSFORMED));
            }
            params = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
            fileDBAdaptor.update(vcf.getId(), params, QueryOptions.empty());

            // Update variant stats
            Path statsFile = Paths.get(json.getUri().getRawPath());
            try (InputStream is = FileUtils.newInputStream(statsFile)) {
                VariantFileMetadata fileMetadata = new ObjectMapper().readValue(is, VariantFileMetadata.class);
                VariantSetStats stats = fileMetadata.getStats();
                params = new ObjectMap(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(VARIANT_FILE_STATS, stats));
                update(vcf.getId(), params, new QueryOptions(), sessionId);
            } catch (IOException e) {
                throw new CatalogException("Error reading file \"" + statsFile + "\"", e);
            }
        }
    }

    public void setStatus(String id, String status, String message, String sessionId) throws CatalogException {
        MyResourceId resource = getId(id, null, sessionId);
        String userId = resource.getUser();
        long fileId = resource.getResourceId();

        authorizationManager.checkFilePermission(resource.getStudyId(), fileId, userId, FileAclEntry.FilePermissions.WRITE);

        if (status != null && !File.FileStatus.isValid(status)) {
            throw new CatalogException("The status " + status + " is not valid file status.");
        }

        ObjectMap parameters = new ObjectMap();
        parameters.putIfNotNull(FileDBAdaptor.QueryParams.STATUS_NAME.key(), status);
        parameters.putIfNotNull(FileDBAdaptor.QueryParams.STATUS_MSG.key(), message);

        fileDBAdaptor.update(fileId, parameters, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    public QueryResult<FileIndex> updateFileIndexStatus(File file, String newStatus, String message, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Long studyId = getStudyId(file.getId());
        authorizationManager.checkFilePermission(studyId, file.getId(), userId, FileAclEntry.FilePermissions.WRITE);

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
        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
        fileDBAdaptor.update(file.getId(), params, QueryOptions.empty());
        auditManager.recordUpdate(AuditRecord.Resource.file, file.getId(), userId, params, null, null);

        return new QueryResult<>("Update file index", 0, 1, 1, "", "", Arrays.asList(index));
    }

    public QueryResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return getParents(true, options, get(fileId, new QueryOptions("include", "projects.studies.files.path"), sessionId).first()
                .getPath(), getStudyId(fileId));
    }

    public QueryResult<File> getParent(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        File file = get(fileId, null, sessionId).first();
        Path parent = Paths.get(file.getPath()).getParent();
        String parentPath;
        if (parent == null) {
            parentPath = "";
        } else {
            parentPath = parent.toString().endsWith("/") ? parent.toString() : parent.toString() + "/";
        }
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        String user = userManager.getUserId(sessionId);
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), parentPath);
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, options, user);
        if (fileQueryResult.getNumResults() == 0) {
            throw CatalogAuthorizationException.deny(user, "view", "file", fileId, "");
        }
        fileQueryResult.setId(Long.toString(fileId));
        return fileQueryResult;
    }

    public QueryResult<File> createFolder(String studyStr, String path, File.FileStatus status, boolean parents, String description,
                                          QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkPath(path, "folderPath");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (!path.endsWith("/")) {
            path = path + "/";
        }

        QueryResult<File> fileQueryResult;
        switch (checkPathExists(path, studyId)) {
            case FREE_PATH:
                fileQueryResult = create(Long.toString(studyId), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, path, null,
                        description, status, 0, -1, null, -1, null, null, parents, null, options, sessionId);
                break;
            case DIRECTORY_EXISTS:
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
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
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        switch (checkPathExists(path, studyId)) {
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
        File file = new File(type, format, bioformat, path, description, status, size, samples, jobId, stats, attributes);
        return create(studyStr, file, parents, content, options, sessionId);
    }

    @Override
    public QueryResult<File> create(String studyStr, File entry, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Call to create passing parents and content variables");
    }

    public QueryResult<File> create(String studyStr, File file, boolean parents, String content, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

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

        file.setSamples(ParamUtils.defaultObject(file.getSamples(), ArrayList<Sample>::new));
        for (Sample sample : file.getSamples()) {
            if (sample.getId() <= 0 || !sampleDBAdaptor.exists(sample.getId())) {
                throw new CatalogException("Sample { id: " + sample.getId() + "} does not exist.");
            }
        }
        if (file.getJob().getId() > 0 && !jobDBAdaptor.exists(file.getJob().getId())) {
            throw new CatalogException("Job { id: " + file.getJob().getId() + "} does not exist.");
        }
        file.setStats(ParamUtils.defaultObject(file.getStats(), HashMap<String, Object>::new));
        file.setAttributes(ParamUtils.defaultObject(file.getAttributes(), HashMap<String, Object>::new));

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
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath())
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";" + File.FileStatus.DELETED
                        + ";" + File.FileStatus.DELETING + ";" + File.FileStatus.PENDING_DELETE + ";" + File.FileStatus.REMOVED);
        if (fileDBAdaptor.count(query).first() > 0) {
            logger.warn("The file {} already exists in catalog", file.getPath());
        }
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.URI.key(), uri)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";" + File.FileStatus.DELETED
                        + ";" + File.FileStatus.DELETING + ";" + File.FileStatus.PENDING_DELETE + ";" + File.FileStatus.REMOVED);
        if (fileDBAdaptor.count(query).first() > 0) {
            logger.warn("The uri {} of the file is already in catalog but on a different path", uri);
        }

        boolean external = isExternal(studyId, file.getPath(), uri);
        file.setExternal(external);
        file.setRelease(catalogManager.getStudyManager().getCurrentRelease(studyId));

        //Find parent. If parents == true, create folders.
        String parentPath = getParentPath(file.getPath());

        long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
        boolean newParent = false;
        if (parentFileId < 0 && StringUtils.isNotEmpty(parentPath)) {
            if (parents) {
                newParent = true;
                File parentFile = new File(File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, parentPath, "",
                        new File.FileStatus(File.FileStatus.READY), 0, file.getSamples(), -1, Collections.emptyMap(),
                        Collections.emptyMap());
                parentFileId = create(Long.toString(studyId), parentFile, parents, null, options, sessionId).first().getId();
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

        if (Objects.equals(file.getStatus().getName(), File.FileStatus.READY)) {
            CatalogIOManager ioManager = catalogIOManagerFactory.get(uri);
            if (file.getType() == File.Type.DIRECTORY) {
                ioManager.createDirectory(uri, parents);
            } else {
                content = content != null ? content : "";
                InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
                ioManager.createFile(uri, inputStream);
            }
        }

        QueryResult<File> queryResult = fileDBAdaptor.insert(file, studyId, options);
        // We obtain the permissions set in the parent folder and set them to the file or folder being created
        QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(studyId, parentFileId, userId, false);
        // Propagate ACLs
        if (allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()), allFileAcls.getResult(), Entity.FILE);
        }

        auditManager.recordCreation(AuditRecord.Resource.file, queryResult.first().getId(), userId, queryResult.first(), null, null);

        matchUpVariantFiles(queryResult.getResult(), sessionId);

        return queryResult;
    }

    public QueryResult<File> get(Long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(fileId), options, sessionId);
    }

    @Override
    public QueryResult<File> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(sessionId);
        Long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        query.append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, options, userId);

        if (fileQueryResult.getNumResults() == 0 && query.containsKey("id")) {
            List<Long> idList = query.getAsLongList("id");
            for (Long myId : idList) {
                authorizationManager.checkFilePermission(studyId, myId, userId, FileAclEntry.FilePermissions.VIEW);
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

        MyResourceId resource = getId(fileIdStr, studyStr, sessionId);

        query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), resource.getStudyId());

        // Check if we can obtain the file from the dbAdaptor properly.
        QueryOptions qOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.NAME.key(),
                        FileDBAdaptor.QueryParams.ID.key(), FileDBAdaptor.QueryParams.TYPE.key()));
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(resource.getResourceId(), qOptions);
        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
            throw new CatalogException("An error occurred with the database.");
        }

        // Check if the id does not correspond to a directory
        if (!fileQueryResult.first().getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogException("The file introduced is not a directory.");
        }

        // Call recursive method
        FileTree fileTree = getTree(fileQueryResult.first(), query, queryOptions, maxDepth, resource.getStudyId(), resource.getUser());

        int dbTime = (int) (System.currentTimeMillis() - startTime);
        int numResults = countFilesInTree(fileTree);

        return new QueryResult<>("File tree", dbTime, numResults, numResults, "", "", Arrays.asList(fileTree));
    }

    public QueryResult<File> getFilesFromFolder(String folderStr, String studyStr, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(folderStr, "folder");
        MyResourceId resource = getId(folderStr, studyStr, sessionId);

        options = ParamUtils.defaultObject(options, QueryOptions::new);
        File folder = get(resource.getResourceId(), null, sessionId).first();
        if (!folder.getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogDBException("File {id:" + resource.getResourceId() + ", path:'" + folder.getPath() + "'} is not a folder.");
        }
        Query query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), folder.getPath());
        return get(resource.getStudyId(), query, options, sessionId);
    }

    public QueryResult<File> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getUserId(sessionId);

        if (studyId <= 0) {
            throw new CatalogDBException("Permission denied. Only the files of one study can be seen at a time.");
        } else {
            query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        }

        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        QueryResult<File> queryResult = fileDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    @Override
    public DBIterator<File> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (studyId <= 0) {
            throw new CatalogDBException("Permission denied. Only the files of one study can be seen at a time.");
        } else {
            query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        }

        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        return fileDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<File> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        query.append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<File> queryResult = fileDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    @Override
    public QueryResult<File> count(String studyStr, Query query, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        query.append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = fileDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public QueryResult<File> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        MyResourceId resource = getId(entryStr, studyStr, sessionId);

        String userId = userManager.getUserId(sessionId);
        File file = get(resource.getResourceId(), null, sessionId).first();

        if (isRootFolder(file)) {
            throw new CatalogException("Can not modify root folder");
        }

        authorizationManager.checkFilePermission(resource.getStudyId(), resource.getResourceId(), userId,
                FileAclEntry.FilePermissions.WRITE);
        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            FileDBAdaptor.QueryParams queryParam = FileDBAdaptor.QueryParams.getParam(param.getKey());
            switch (queryParam) {
                case NAME:
                case FORMAT:
                case BIOFORMAT:
                case DESCRIPTION:
                case ATTRIBUTES:
                case STATS:
                case JOB_ID:
                case SAMPLES:
                    break;
                default:
                    throw new CatalogException("Parameter '" + queryParam + "' cannot be changed.");
            }
        }

        // We obtain the numeric ids of the samples given
        if (StringUtils.isNotEmpty(parameters.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            List<String> sampleIdStr = parameters.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key());
//            parameters.remove(FileDBAdaptor.QueryParams.SAMPLES.key());

            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(sampleIdStr, Long.toString(resource.getStudyId()),
                    sessionId);

            // Avoid sample duplicates
            Set<Long> sampleIdsSet = new LinkedHashSet<>();
            sampleIdsSet.addAll(resourceIds.getResourceIds());

            List<Sample> sampleList = new ArrayList<>(sampleIdsSet.size());
            for (Long sampleId : sampleIdsSet) {
                sampleList.add(new Sample().setId(sampleId));
            }
//            fileDBAdaptor.addSamplesToFile(fileId, sampleList);
            parameters.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
        }

        //Name must be changed with "rename".
        if (parameters.containsKey("name")) {
            logger.info("Rename file using update method!");
            rename(resource.getResourceId(), parameters.getString("name"), sessionId);
        }

        String ownerId = studyDBAdaptor.getOwnerId(resource.getStudyId());
        fileDBAdaptor.update(resource.getResourceId(), parameters, QueryOptions.empty());
        QueryResult<File> queryResult = fileDBAdaptor.get(resource.getResourceId(), options);
        auditManager.recordUpdate(AuditRecord.Resource.file, resource.getResourceId(), userId, parameters, null, null);
        userDBAdaptor.updateUserLastModified(ownerId);
        return queryResult;
    }

    @Deprecated
    public QueryResult<File> update(Long fileId, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        return update(null, String.valueOf(fileId), parameters, options, sessionId);
    }

    @Override
    public List<QueryResult<File>> delete(@Nullable String studyStr, String fileIdStr, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
        /*
         * This method checks:
         * 1. fileIdStr converts easily to a valid fileId.
         * 2. The user belonging to the sessionId has permissions to delete files.
         * 3. If file is external and DELETE_EXTERNAL_FILE is false, we will call unlink method.
         * 4. Check if the status of the file/folder and the children is a valid one.
         * 5. Root folders cannot be deleted.
         * 6. No external files or folders are found within the path.
         */
        QueryResult<File> deletedFileResult = new QueryResult<>("Delete file", -1, 0, 0, "", "No changes made", Collections.emptyList());

        params = ParamUtils.defaultObject(params, ObjectMap::new);

        AbstractManager.MyResourceIds resource = catalogManager.getFileManager().getIds(
                Arrays.asList(StringUtils.split(fileIdStr, ",")), studyStr, sessionId);
        String userId = resource.getUser();
        long studyId = resource.getStudyId();

        // Check 1. No comma-separated values are valid, only one single File or Directory can be deleted.
        List<Long> fileIds = resource.getResourceIds();
        List<QueryResult<File>> queryResultList = new ArrayList<>(fileIds.size());
        // TODO: All the throws should be catched and put in the error field of queryResult
        for (Long fileId : fileIds) {
            // Check 2. User has the proper permissions to delete the file.
            authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.DELETE);

            // Check if we can obtain the file from the dbAdaptor properly.
            QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, QueryOptions.empty());
            if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
                throw new CatalogException("Cannot delete file '" + fileIdStr + "'. There was an error with the database.");
            }
            File file = fileQueryResult.first();

            // Check 3.
            // If file is not externally linked or if it is external but with DELETE_EXTERNAL_FILES set to true then can be deleted.
            // This prevents external linked files to be accidentally deleted.
            // If file is linked externally and DELETE_EXTERNAL_FILES is false then we just unlink the file.
            if (file.isExternal() && !params.getBoolean(DELETE_EXTERNAL_FILES, false)) {
                queryResultList.add(unlink(StringUtils.join(resource.getResourceIds(), ","), Long.toString(resource.getStudyId()),
                        sessionId));
                continue;
            }

            // Check 4.
            // We cannot delete the root folder
            if (file.getType().equals(File.Type.DIRECTORY) && isRootFolder(file)) {
                throw new CatalogException("Root directories cannot be deleted");
            }

            // Check 5.
            // Only READY, TRASHED and PENDING_DELETE files can be deleted
            String fileStatus = file.getStatus().getName();
            // TODO change this to accept only valid statuses
            if (fileStatus.equalsIgnoreCase(File.FileStatus.STAGE) || fileStatus.equalsIgnoreCase(File.FileStatus.MISSING)
                    || fileStatus.equalsIgnoreCase(File.FileStatus.DELETING) || fileStatus.equalsIgnoreCase(File.FileStatus.DELETED)
                    || fileStatus.equalsIgnoreCase(File.FileStatus.REMOVED)) {
                throw new CatalogException("File cannot be deleted, status is: " + fileStatus);
            }

            // Check 6.
            if (file.getType().equals(File.Type.DIRECTORY)) {
                // We cannot delete a folder containing files or folders with status missing or staged
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(),
                                Arrays.asList(File.FileStatus.MISSING, File.FileStatus.STAGE));

                long count = fileDBAdaptor.count(query).first();
                if (count > 0) {
                    throw new CatalogException("Cannot delete folder. " + count + " files have been found with status missing or staged.");
                }
            }

            // Check 7.
            // We cannot delete a folder containing any linked file/folder, these must be unlinked first
            if (file.getType().equals(File.Type.DIRECTORY) && !params.getBoolean(DELETE_EXTERNAL_FILES, false)) {
                if (studyId == -1) {
                    studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
                }

                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY)
                        .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);

                long count = fileDBAdaptor.count(query).first();
                if (count > 0) {
                    throw new CatalogException("Cannot delete folder. " + count + " linked files have been found within the path. Please, "
                            + "unlink them first.");
                }
            }

            // Check 8
            // We cannot unlink a file or folder containing files that are indexed or being processed in storage
            checkUsedInStorage(studyId, file);

            if (params.getBoolean(SKIP_TRASH, false) || params.getBoolean(DELETE_EXTERNAL_FILES, false)) {
                deletedFileResult = deleteFromDisk(file, studyId, userId, params);
            } else {
                if (fileStatus.equalsIgnoreCase(File.FileStatus.READY)) {
                    ObjectMap updateParams = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
                    if (file.getType().equals(File.Type.FILE)) {
                        checkCanDelete(Arrays.asList(fileId));
                        fileDBAdaptor.update(fileId, updateParams, QueryOptions.empty());
                        Query query = new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                        jobDBAdaptor.extractFilesFromJobs(query, Arrays.asList(fileId));
                    } else {
                        if (studyId == -1) {
                            studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
                        }

                        // Send to trash all the files and subfolders
                        Query query = new Query()
                                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
                        checkCanDelete(query);
                        fileDBAdaptor.update(query, updateParams, QueryOptions.empty());

                        // Remove any reference to the file ids recently sent to the trash bin
                        query.put(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
                        QueryResult<File> queryResult = fileDBAdaptor.get(query, new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor
                                .QueryParams.ID.key()));
                        List<Long> fileIdsTmp = queryResult.getResult().stream().map(File::getId).collect(Collectors.toList());
                        jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId), fileIdsTmp);

                    }

                    Query query = new Query()
                            .append(FileDBAdaptor.QueryParams.ID.key(), fileId)
                            .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
                    deletedFileResult = fileDBAdaptor.get(query, QueryOptions.empty());
                }
            }
            queryResultList.add(deletedFileResult);
        }

        return queryResultList;
    }


    public QueryResult<File> link(URI uriOrigin, String pathDestiny, long studyId, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
        // We make two attempts to link to ensure the behaviour remains even if it is being called at the same time link from different
        // threads
        try {
            return privateLink(uriOrigin, pathDestiny, studyId, params, sessionId);
        } catch (CatalogException | IOException e) {
            return privateLink(uriOrigin, pathDestiny, studyId, params, sessionId);
        }
    }

    public QueryResult<File> unlink(String fileIdStr, @Nullable String studyStr, String sessionId) throws CatalogException, IOException {
        ParamUtils.checkParameter(fileIdStr, "File");

        AbstractManager.MyResourceId resource = catalogManager.getFileManager().getId(fileIdStr, studyStr, sessionId);
        String userId = resource.getUser();
        long fileId = resource.getResourceId();
        long studyId = resource.getStudyId();

        // Check 2. User has the proper permissions to delete the file.
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.DELETE);

        // Check if we can obtain the file from the dbAdaptor properly.
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, QueryOptions.empty());
        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
            throw new CatalogException("Cannot delete file '" + fileIdStr + "'. There was an error with the database.");
        }

        File file = fileQueryResult.first();

        // Check 3.
        if (!file.isExternal()) {
            throw new CatalogException("Only previously linked files can be unlinked. Please, use delete instead.");
        }

        // Check 8
        // We cannot unlink a file or folder containing files that are indexed or being processed in storage
        checkUsedInStorage(studyId, file);

        String suffixName = ".REMOVED_" + TimeUtils.getTime();
        String basePath = Paths.get(file.getPath()).toString();
        String suffixedPath = basePath + suffixName;
        if (file.getType().equals(File.Type.FILE)) {
            if (fileQueryResult.first().getStatus().getName().equals(File.FileStatus.REMOVED)) {
                return fileQueryResult;
            }
            if (!fileQueryResult.first().getStatus().getName().equals(File.FileStatus.READY)) {
                throw new CatalogException("Cannot unlink. Unexpected file status: " + fileQueryResult.first().getStatus().getName());
            }
            logger.debug("Unlinking file {}", file.getUri().toString());

            ObjectMap update = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), suffixedPath);

            QueryResult<File> retFile = fileDBAdaptor.update(file.getId(), update, QueryOptions.empty());

            // Remove any reference to the file ids recently sent to the trash bin
            jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId), Arrays.asList(file.getId()));

            return retFile;
        } else {
            logger.debug("Unlinking folder {}", file.getUri().toString());

            Files.walkFileTree(Paths.get(file.getUri()), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    try {
                        // Look for the file in catalog
                        Query query = new Query()
                                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(FileDBAdaptor.QueryParams.URI.key(), path.toUri().toString())
                                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true)
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);

                        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                        if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                            logger.debug("Cannot unlink " + path.toString() + ". The file could not be found in catalog.");
                            return FileVisitResult.CONTINUE;
                        }

                        if (fileQueryResult.getNumResults() > 1) {
                            logger.error("Internal error: More than one file was found in catalog for uri " + path.toString());
                            return FileVisitResult.CONTINUE;
                        }

                        File file = fileQueryResult.first();
                        if (!file.getStatus().getName().equals(File.FileStatus.READY)) {
                            logger.warn("Not unlinking file {}, file status: {}", file.getPath(), file.getStatus().getName());
                            return FileVisitResult.CONTINUE;
                        }

                        ObjectMap update = new ObjectMap()
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath().replaceFirst(basePath, suffixedPath));

                        fileDBAdaptor.update(file.getId(), update, QueryOptions.empty());

                        logger.debug("{} unlinked", file.toString());

                        // Remove any reference to the file ids recently sent to the trash bin
                        jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId),
                                Arrays.asList(file.getId()));

                    } catch (CatalogDBException e) {
                        e.printStackTrace();
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.SKIP_SUBTREE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (exc == null) {
                        try {
                            // Only empty folders can be deleted for safety reasons
                            Query query = new Query()
                                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                    .append(FileDBAdaptor.QueryParams.URI.key(), "~^" + dir.toUri().toString() + "/*")
                                    .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true)
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);

                            QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                            if (fileQueryResult == null || fileQueryResult.getNumResults() > 1) {
                                // The only result should be the current directory
                                logger.debug("Cannot unlink " + dir.toString()
                                        + ". There are files/folders inside the folder that have not been unlinked.");
                                return FileVisitResult.CONTINUE;
                            }

                            // Look for the folder in catalog
                            query = new Query()
                                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                    .append(FileDBAdaptor.QueryParams.URI.key(), dir.toUri().toString())
                                    .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true)
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);

                            fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                            if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                                logger.debug("Cannot unlink " + dir.toString() + ". The directory could not be found in catalog.");
                                return FileVisitResult.CONTINUE;
                            }

                            if (fileQueryResult.getNumResults() > 1) {
                                logger.error("Internal error: More than one file was found in catalog for uri " + dir.toString());
                                return FileVisitResult.CONTINUE;
                            }

                            File file = fileQueryResult.first();
                            if (!file.getStatus().getName().equals(File.FileStatus.READY)) {
                                logger.warn("Not unlinking folder {}, folder status: {}", file.getPath(), file.getStatus().getName());
                                return FileVisitResult.CONTINUE;
                            }

                            ObjectMap update = new ObjectMap()
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                                    .append(FileDBAdaptor.QueryParams.PATH.key(),
                                            file.getPath().replaceFirst(basePath, suffixedPath));

                            fileDBAdaptor.update(file.getId(), update, QueryOptions.empty());

                            logger.debug("{} unlinked", dir.toString());

                            // Remove any reference to the file ids recently sent to the trash bin
                            jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId),
                                    Arrays.asList(file.getId()));
                        } catch (CatalogDBException e) {
                            e.printStackTrace();
                        }

                        return FileVisitResult.CONTINUE;
                    } else {
                        // directory iteration failed
                        throw exc;
                    }
                }
            });

            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.ID.key(), file.getId())
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED);
            return fileDBAdaptor.get(query, new QueryOptions());
        }
    }

    @Override
    public QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
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
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        // Add study id to the query
        query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        // We do not need to check for permissions when we show the count of files
        QueryResult queryResult = fileDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    //    @Deprecated
//    private QueryResult<File> checkCanDeleteFile(File file, String userId) throws CatalogException {
//        authorizationManager.checkFilePermission(studyId, file.getId(), userId, FileAclEntry.FilePermissions.DELETE);
//
//        switch (file.getStatus().getName()) {
//            case File.FileStatus.TRASHED:
//                //Send warning message
//                String warningMsg = "File already deleted. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}";
//                logger.warn(warningMsg);
//                return new QueryResult<File>("Delete file", 0, 0, 0,
//                        warningMsg,
//                        null, Collections.emptyList());
//            case File.FileStatus.READY:
//                break;
//            case File.FileStatus.STAGE:
//            case File.FileStatus.MISSING:
//            default:
//                throw new CatalogException("File is not ready. {"
//                        + "id: " + file.getId() + ", "
//                        + "path:\"" + file.getPath() + "\","
//                        + "status: '" + file.getStatus().getName() + "'}");
//        }
//        return null;
//    }

    public QueryResult<File> rename(long fileId, String newName, String sessionId) throws CatalogException {
        ParamUtils.checkFileName(newName, "name");
        String userId = userManager.getUserId(sessionId);
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        long projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = projectDBAdaptor.getOwnerId(projectId);

        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);
        QueryResult<File> fileResult = fileDBAdaptor.get(fileId, null);
        File file = fileResult.first();

        if (file.getName().equals(newName)) {
            fileResult.setId("rename");
            fileResult.setWarningMsg("File name '" + newName + "' is the original name. Do nothing.");
            return fileResult;
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
                    catalogIOManager.rename(oldUri, newUri);   // io.move() 1
                }
                result = fileDBAdaptor.rename(fileId, newPath, newUri.toString(), null);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, new ObjectMap("path", newPath)
                        .append("name", newName), "rename", null);
                break;
            case FILE:
                if (!isExternal) {  //Only rename non external files
                    catalogIOManager = catalogIOManagerFactory.get(oldUri);
                    catalogIOManager.rename(oldUri, newUri);
                }
                result = fileDBAdaptor.rename(fileId, newPath, newUri.toString(), null);
                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, new ObjectMap("path", newPath)
                        .append("name", newName), "rename", null);
                break;
            default:
                throw new CatalogException("Unknown file type " + file.getType());
        }

        return result;
    }

    public DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.VIEW);

        URI fileUri = getUri(get(fileId, null, sessionId).first());
        boolean ignoreCase = options.getBoolean("ignoreCase");
        boolean multi = options.getBoolean("multi");
        return catalogIOManagerFactory.get(fileUri).getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    public DataInputStream download(long fileId, int start, int limit, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.DOWNLOAD);

        URI fileUri = getUri(get(fileId, null, sessionId).first());

        return catalogIOManagerFactory.get(fileUri).getFileObject(fileUri, start, limit);
    }

    public QueryResult index(List<String> fileList, String studyStr, String type, Map<String, String> params, String sessionId)
            throws CatalogException {
        MyResourceIds resourceIds = getIds(fileList, studyStr, sessionId);
        List<Long> fileFolderIdList = resourceIds.getResourceIds();
        long studyId = resourceIds.getStudyId();
        String userId = resourceIds.getUser();

        // Check they all belong to the same study
        for (Long fileId : fileFolderIdList) {
            if (fileId == -1) {
                throw new CatalogException("Could not find file or folder " + fileList);
            }

            long studyIdByFileId = fileDBAdaptor.getStudyIdByFileId(fileId);

            if (studyId == -1) {
                studyId = studyIdByFileId;
            } else if (studyId != studyIdByFileId) {
                throw new CatalogException("Cannot index files coming from different studies.");
            }
        }

        // Define the output directory where the indexes will be put
        String outDirPath = ParamUtils.defaultString(params.get("outdir"), "/");
        if (outDirPath != null && !StringUtils.isNumeric(outDirPath) && outDirPath.contains("/") && !outDirPath.endsWith("/")) {
            outDirPath = outDirPath + "/";
        }

        File outDir;
        try {
            outDir = new File().setId(getId(outDirPath, Long.toString(studyId), sessionId).getResourceId());
        } catch (CatalogException e) {
            logger.warn("'{}' does not exist. Trying to create the output directory.", outDirPath);
            QueryResult<File> folder = createFolder(Long.toString(studyId), outDirPath, new File.FileStatus(), true, "",
                    new QueryOptions(), sessionId);
            outDir = folder.first();
        }

        if (outDir.getId() > 0) {
            authorizationManager.checkFilePermission(studyId, outDir.getId(), userId, FileAclEntry.FilePermissions.WRITE);
            if (fileDBAdaptor.getStudyIdByFileId(outDir.getId()) != studyId) {
                throw new CatalogException("The output directory does not correspond to the same study of the files");
            }

        } else {
            ObjectMap parsedSampleStr = parseFeatureId(userId, outDirPath);
            String path = (String) parsedSampleStr.get("featureName");
            logger.info("Outdir {}", path);
            if (path.contains("/")) {
                if (!path.endsWith("/")) {
                    path = path + "/";
                }
                // It is a path, so we will try to create the folder
                createFolder(Long.toString(studyId), path, new File.FileStatus(), true, "", new QueryOptions(), sessionId);
                outDir = new File().setId(getId(path, Long.toString(studyId), sessionId).getResourceId());
                logger.info("Outdir {} -> {}", outDir, path);
            }
        }

        QueryResult<Job> jobQueryResult;
        List<File> fileIdList = new ArrayList<>();
        String indexDaemonType = null;
        String jobName = null;
        String description = null;

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

            for (Long fileId : fileFolderIdList) {
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        FileDBAdaptor.QueryParams.NAME.key(),
                        FileDBAdaptor.QueryParams.PATH.key(),
                        FileDBAdaptor.QueryParams.URI.key(),
                        FileDBAdaptor.QueryParams.TYPE.key(),
                        FileDBAdaptor.QueryParams.BIOFORMAT.key(),
                        FileDBAdaptor.QueryParams.FORMAT.key(),
                        FileDBAdaptor.QueryParams.INDEX.key())
                );
                QueryResult<File> file = fileDBAdaptor.get(fileId, queryOptions);

                if (file.getNumResults() != 1) {
                    throw new CatalogException("Could not find file or folder " + fileList);
                }

                if (File.Type.DIRECTORY.equals(file.first().getType())) {
                    // Retrieve all the VCF files that can be found within the directory
                    String path = file.first().getPath().endsWith("/") ? file.first().getPath() : file.first().getPath() + "/";
                    Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF, File.Format.GVCF))
                            .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + path + "*")
                            .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, queryOptions);

                    if (fileQueryResult.getNumResults() == 0) {
                        throw new CatalogException("No VCF files could be found in directory " + file.first().getPath());
                    }

                    for (File fileTmp : fileQueryResult.getResult()) {
                        authorizationManager.checkFilePermission(studyId, fileTmp.getId(), userId, FileAclEntry.FilePermissions.VIEW);
                        authorizationManager.checkFilePermission(studyId, fileTmp.getId(), userId, FileAclEntry.FilePermissions.WRITE);

                        fileIdList.add(fileTmp);
                    }

                } else {
                    if (!File.Format.VCF.equals(file.first().getFormat()) && !File.Format.GVCF.equals(file.first().getFormat())) {
                        throw new CatalogException("The file " + file.first().getName() + " is not a VCF file.");
                    }

                    authorizationManager.checkFilePermission(studyId, file.first().getId(), userId, FileAclEntry.FilePermissions.VIEW);
                    authorizationManager.checkFilePermission(studyId, file.first().getId(), userId, FileAclEntry.FilePermissions.WRITE);

                    fileIdList.add(file.first());
                }
            }

            if (fileIdList.size() == 0) {
                throw new CatalogException("Cannot send to index. No files could be found to be indexed.");
            }

            params.put("outdir", Long.toString(outDir.getId()));
            params.put("sid", sessionId);

        } else if (type.equals("BAM")) {

            indexDaemonType = IndexDaemon.ALIGNMENT_TYPE;
            jobName = "AlignmentIndex";

            for (Long fileId : fileFolderIdList) {
                QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        FileDBAdaptor.QueryParams.PATH.key(),
                        FileDBAdaptor.QueryParams.URI.key(),
                        FileDBAdaptor.QueryParams.TYPE.key(),
                        FileDBAdaptor.QueryParams.FORMAT.key(),
                        FileDBAdaptor.QueryParams.INDEX.key())
                );
                QueryResult<File> file = fileDBAdaptor.get(fileId, queryOptions);

                if (file.getNumResults() != 1) {
                    throw new CatalogException("Could not find file or folder " + fileList);
                }

                if (File.Type.DIRECTORY.equals(file.first().getType())) {
                    // Retrieve all the BAM files that can be found within the directory
                    String path = file.first().getPath().endsWith("/") ? file.first().getPath() : file.first().getPath() + "/";
                    Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.SAM, File.Format.BAM))
                            .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + path + "*")
                            .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, queryOptions);

                    if (fileQueryResult.getNumResults() == 0) {
                        throw new CatalogException("No SAM/BAM files could be found in directory " + file.first().getPath());
                    }

                    for (File fileTmp : fileQueryResult.getResult()) {
                        authorizationManager.checkFilePermission(studyId, fileTmp.getId(), userId, FileAclEntry.FilePermissions.VIEW);
                        authorizationManager.checkFilePermission(studyId, fileTmp.getId(), userId, FileAclEntry.FilePermissions.WRITE);

                        fileIdList.add(fileTmp);
                    }

                } else {
                    if (!File.Format.BAM.equals(file.first().getFormat()) && !File.Format.SAM.equals(file.first().getFormat())) {
                        throw new CatalogException("The file " + file.first().getName() + " is not a SAM/BAM file.");
                    }

                    authorizationManager.checkFilePermission(studyId, file.first().getId(), userId, FileAclEntry.FilePermissions.VIEW);
                    authorizationManager.checkFilePermission(studyId, file.first().getId(), userId, FileAclEntry.FilePermissions.WRITE);

                    fileIdList.add(file.first());
                }
            }

        }

        if (fileIdList.size() == 0) {
            throw new CatalogException("Cannot send to index. No files could be found to be indexed.");
        }

        String fileIds = fileIdList.stream().map(File::getId).map(l -> Long.toString(l)).collect(Collectors.joining(","));
        params.put("file", fileIds);
        List<File> outputList = outDir.getId() > 0 ? Arrays.asList(outDir) : Collections.emptyList();
        ObjectMap attributes = new ObjectMap();
        attributes.put(IndexDaemon.INDEX_TYPE, indexDaemonType);
        attributes.putIfNotNull(Job.OPENCGA_OUTPUT_DIR, outDirPath);
        attributes.putIfNotNull(Job.OPENCGA_STUDY, studyStr);

        logger.info("job description: " + description);
        jobQueryResult = catalogManager.getJobManager().queue(studyId, jobName, description, "opencga-analysis.sh",
                Job.Type.INDEX, params, fileIdList, outputList, outDir, userId, attributes);
        jobQueryResult.first().setToolId(jobName);

        return jobQueryResult;
    }

    public void setFileIndex(long fileId, FileIndex index, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
        fileDBAdaptor.update(fileId, parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    public void setDiskUsage(long fileId, long size, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.SIZE.key(), size);
        fileDBAdaptor.update(fileId, parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    public void setModificationDate(long fileId, String date, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.MODIFICATION_DATE.key(), date);
        fileDBAdaptor.update(fileId, parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    public void setUri(long fileId, String uri, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.URI.key(), uri);
        fileDBAdaptor.update(fileId, parameters, QueryOptions.empty());

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }


    // **************************   ACLs  ******************************** //
    public List<QueryResult<FileAclEntry>> getAcls(String studyStr, List<String> fileList, String member, boolean silent, String sessionId)
            throws CatalogException {
        MyResourceIds resource = getIds(fileList, studyStr, silent, sessionId);
        List<QueryResult<FileAclEntry>> fileAclList = new ArrayList<>(resource.getResourceIds().size());

        List<Long> resourceIds = resource.getResourceIds();
        for (int i = 0; i < resourceIds.size(); i++) {
            Long fileId = resourceIds.get(i);
            try {
                QueryResult<FileAclEntry> allFileAcls;
                if (StringUtils.isNotEmpty(member)) {
                    allFileAcls = authorizationManager.getFileAcl(resource.getStudyId(), fileId, resource.getUser(), member);
                } else {
                    allFileAcls = authorizationManager.getAllFileAcls(resource.getStudyId(), fileId, resource.getUser(), true);
                }
                allFileAcls.setId(String.valueOf(fileId));
                fileAclList.add(allFileAcls);
            } catch (CatalogException e) {
                if (silent) {
                    fileAclList.add(new QueryResult<>(fileList.get(i), 0, 0, 0, "", e.toString(),
                            new ArrayList<>(0)));
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
            MyResourceIds ids = catalogManager.getSampleManager().getIds(Arrays.asList(StringUtils.split(fileAclParams.getSample(), ",")),
                    studyStr, sessionId);

            Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(ids.getStudyId(), query, options, sessionId);

            fileList = fileQueryResult.getResult().stream().map(File::getId).map(String::valueOf).collect(Collectors.toList());

            studyStr = Long.toString(ids.getStudyId());
        }

        // Obtain the resource ids
        MyResourceIds resourceIds = getIds(fileList, studyStr, sessionId);
        authorizationManager.checkCanAssignOrSeePermissions(resourceIds.getStudyId(), resourceIds.getUser());

        // Increase the list with the files/folders within the list of ids that correspond with folders
        resourceIds = getRecursiveFilesAndFolders(resourceIds);

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
        checkMembers(resourceIds.getStudyId(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        switch (fileAclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.FILE);
            case ADD:
                return authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        Entity.FILE);
            case REMOVE:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, Entity.FILE);
            case RESET:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, Entity.FILE);
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
     * @param resourceIds ResourceId object containing the list of file ids, studyId and userId.
     * @return a new ResourceId object
     */
    private MyResourceIds getRecursiveFilesAndFolders(MyResourceIds resourceIds) throws CatalogException {
        Set<Long> fileIdSet = new HashSet<>();
        fileIdSet.addAll(resourceIds.getResourceIds());

        // Get info of the files to see if they are files or folders
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                .append(FileDBAdaptor.QueryParams.ID.key(), resourceIds.getResourceIds());
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.TYPE.key()));

        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, options);
        if (fileQueryResult.getNumResults() != resourceIds.getResourceIds().size()) {
            logger.error("Some files were not found for query {}", query.safeToString());
            throw new CatalogException("Internal error. Some files were not found.");
        }

        List<String> pathList = new ArrayList<>();
        for (File file : fileQueryResult.getResult()) {
            if (file.getType().equals(File.Type.DIRECTORY)) {
                pathList.add("~^" + file.getPath());
            }
        }

        options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key());
        if (CollectionUtils.isNotEmpty(pathList)) {
            // Search for all the files within the list of paths
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), resourceIds.getStudyId())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), pathList);
            QueryResult<File> fileQueryResult1 = fileDBAdaptor.get(query, options);
            fileIdSet.addAll(fileQueryResult1.getResult().stream().map(File::getId).collect(Collectors.toSet()));
        }

        List<Long> fileIdList = new ArrayList<>(fileIdSet.size());
        fileIdList.addAll(fileIdSet);
        return new MyResourceIds(resourceIds.getUser(), resourceIds.getStudyId(), fileIdList);
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

    private Long smartResolutor(String fileName, long studyId) throws CatalogException {
        if (StringUtils.isNumeric(fileName) && Long.parseLong(fileName) > configuration.getCatalog().getOffset()) {
            long fileId = Long.parseLong(fileName);
            fileDBAdaptor.checkId(fileId);
            return fileId;
        }

        // This line is only just in case the files are being passed from the webservice. Paths cannot contain "/" when passed via the URLs
        // so they should be passed as : instead. This is just to support that behaviour.
        fileName = fileName.replace(":", "/");

        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }

        // We search as a path
        Query query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), fileName);
//                .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=EMPTY");
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.files.id");
        QueryResult<File> pathQueryResult = fileDBAdaptor.get(query, qOptions);
        if (pathQueryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one file id found based on " + fileName);
        }

        if (!fileName.contains("/")) {
            // We search as a fileName as well
            query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.NAME.key(), fileName);
//                    .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=EMPTY");
            QueryResult<File> nameQueryResult = fileDBAdaptor.get(query, qOptions);
            if (nameQueryResult.getNumResults() > 1) {
                throw new CatalogException("Error: More than one file id found based on " + fileName);
            }

            if (pathQueryResult.getNumResults() == 1 && nameQueryResult.getNumResults() == 0) {
                return pathQueryResult.first().getId();
            } else if (pathQueryResult.getNumResults() == 0 && nameQueryResult.getNumResults() == 1) {
                return nameQueryResult.first().getId();
            } else if (pathQueryResult.getNumResults() == 1 && nameQueryResult.getNumResults() == 1) {
                if (pathQueryResult.first().getId() == nameQueryResult.first().getId()) {
                    // The file was in the root folder, so it could be found based on the path and the name
                    return pathQueryResult.first().getId();
                } else {
                    throw new CatalogException("Error: More than one file id found based on " + fileName);
                }
            } else {
                // No results
                throw new CatalogException("File " + fileName + " not found in study " + studyId);
            }
        }

        if (pathQueryResult.getNumResults() == 1) {
            return pathQueryResult.first().getId();
        } else if (pathQueryResult.getNumResults() == 0) {
            throw new CatalogException("File " + fileName + " not found in study " + studyId);
        } else {
            throw new CatalogException("Multiple files found under " + fileName + " in study " + studyId);
        }
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

    /**
     * Return all parent folders from a file.
     *
     * @param file
     * @param options
     * @return
     * @throws CatalogException
     */
    private QueryResult<File> getParents(File file, boolean rootFirst, QueryOptions options) throws CatalogException {
        String filePath = file.getPath();
        return getParents(rootFirst, options, filePath, getStudyId(file.getId()));
    }

    private QueryResult<File> getParents(boolean rootFirst, QueryOptions options, String filePath, long studyId) throws CatalogException {
        List<String> paths = getParentPaths(filePath);

        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), paths);
        query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
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

    private boolean isExternal(long studyId, String catalogFilePath, URI fileUri) throws CatalogException {
        URI studyUri = getStudyUri(studyId);

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
            authorizationManager.checkFilePermission(studyId, folder.getId(), userId, FileAclEntry.FilePermissions.VIEW);
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
                    authorizationManager.checkFilePermission(studyId, fileAux.getId(), userId, FileAclEntry.FilePermissions.VIEW);
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

    private void checkCanDelete(Query query) throws CatalogException {
        QueryResult<File> queryResult = fileDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key()));
        List<Long> fileIds = queryResult.getResult().stream().map(File::getId).collect(Collectors.toList());
        if (fileIds.isEmpty()) {
            logger.debug("Could not obtain any id given the query: {}", query.safeToString());
            throw new CatalogDBException("Could not obtain any id given the query");
        }

        checkCanDelete(fileIds);
    }

    private void checkCanDelete(List<Long> fileIds) throws CatalogException {
        if (fileIds == null || fileIds.isEmpty()) {
            throw new CatalogException("Nothing to delete");
        }

        Query jobQuery = new Query(JobDBAdaptor.QueryParams.INPUT.key(), fileIds);
        long jobCount = jobDBAdaptor.count(jobQuery).first();
        if (jobCount > 0) {
            throw new CatalogException("The file(s) cannot be deleted because there is at least one being used as input of " + jobCount
                    + " jobs.");
        }

        Query datasetQuery = new Query(DatasetDBAdaptor.QueryParams.FILES.key(), fileIds);
        long datasetCount = datasetDBAdaptor.count(datasetQuery).first();
        if (datasetCount > 0) {
            throw new CatalogException("The file(s) cannot be deleted because there is at least one being part of " + datasetCount
                    + "datasets");
        }
    }

    private QueryResult<File> deleteFromDisk(File fileOrDirectory, long studyId, String userId, ObjectMap params)
            throws CatalogException, IOException {
        QueryResult<File> removedFileResult;

        // Check permissions for the current file
        authorizationManager.checkFilePermission(studyId, fileOrDirectory.getId(), userId, FileAclEntry.FilePermissions.DELETE);

        // Not external file
        URI fileUri = getUri(fileOrDirectory);
        Path filesystemPath = Paths.get(fileUri);
        // FileUtils.checkFile(filesystemPath);
        CatalogIOManager ioManager = catalogIOManagerFactory.get(fileUri);

        String suffixName = ".DELETED_" + TimeUtils.getTime();

        // If file is not a directory then we can just delete it from disk and update Catalog.
        if (fileOrDirectory.getType().equals(File.Type.FILE)) {
            if (fileOrDirectory.getStatus().getName().equals(File.FileStatus.READY)) {
                checkCanDelete(Arrays.asList(fileOrDirectory.getId()));
            }

            // 1. Set the file status to deleting
            ObjectMap update = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING);
            fileDBAdaptor.update(fileOrDirectory.getId(), update, QueryOptions.empty());

            // 2. Delete the file from disk
            ioManager.deleteFile(fileUri);

            // 3. Update the file status in the database. Set to delete
            update = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), fileOrDirectory.getPath() + suffixName);
            removedFileResult = fileDBAdaptor.update(fileOrDirectory.getId(), update, QueryOptions.empty());

            if (fileOrDirectory.getStatus().getName().equals(File.FileStatus.READY)) {
                // Remove any reference to the file ids recently sent to the trash bin
                jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId),
                        Arrays.asList(fileOrDirectory.getId()));
            }
        } else {
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + fileOrDirectory.getPath() + "*");

            if (fileOrDirectory.getStatus().getName().equals(File.FileStatus.READY)) {
                checkCanDelete(query);
            }

            String basePath = Paths.get(fileOrDirectory.getPath()).toString();
            String suffixedPath = basePath + suffixName;

            // Directories can be marked to be deferred removed by setting FORCE_DELETE to false, then File daemon will remove it.
            // In this mode directory is just renamed and URIs and Paths updated in Catalog. By default removal is deferred.
            if (!params.getBoolean(FORCE_DELETE, false)
                    && !fileOrDirectory.getStatus().getName().equals(File.FileStatus.PENDING_DELETE)) {
                // Rename the directory in the filesystem.
                URI newURI;
                try {
                    newURI = UriUtils.createDirectoryUri(Paths.get(fileUri).toString() + suffixName);
                } catch (URISyntaxException e) {
                    throw new CatalogException(e);
                }
//                URI newURI = Paths.get(Paths.get(fileUri).toString() + suffixName).toUri();

                // Get all the files that starts with path
                logger.debug("Looking for files and folders inside {}", fileOrDirectory.getPath());
                QueryResult<File> queryResult = fileDBAdaptor.get(query, new QueryOptions());

                if (queryResult != null && queryResult.getNumResults() > 0) {
                    logger.debug("Renaming {} to {}", fileUri.toString(), newURI.toString());
                    ioManager.rename(fileUri, newURI);

                    logger.debug("Changing the URI in catalog to {} and setting the status to {}", newURI.toString(),
                            File.FileStatus.PENDING_DELETE);

                    // We update the uri and status of all the files and folders so it can be later deleted by the daemon
                    for (File file : queryResult.getResult()) {

                        String newUri = file.getUri().toString().replaceFirst(fileUri.toString(), newURI.toString());
                        String newPath = file.getPath().replaceFirst(basePath, suffixedPath);

                        System.out.println("newPath = " + newPath);

                        logger.debug("Replacing old uri {} per {} and setting the status to {}", file.getUri().toString(),
                                newUri, File.FileStatus.PENDING_DELETE);

                        ObjectMap update = new ObjectMap()
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE)
                                .append(FileDBAdaptor.QueryParams.URI.key(), newUri)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), newPath);
                        fileDBAdaptor.update(file.getId(), update, QueryOptions.empty());
                    }

                    if (fileOrDirectory.getStatus().getName().equals(File.FileStatus.READY)) {
                        // Remove any reference to the file ids recently sent to the trash bin
                        List<Long> fileIdsTmp = queryResult.getResult().stream().map(File::getId).collect(Collectors.toList());
                        jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId), fileIdsTmp);
                    }
                } else {
                    // The uri in the disk has been changed but not in the database !!
                    throw new CatalogException("ERROR: Could not retrieve all the files and folders hanging from " + fileUri.toString());
                }

            } else if (params.getBoolean(FORCE_DELETE, false)) {
                // Physically delete all the files hanging from the folder
                Files.walkFileTree(filesystemPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                        try {
                            // Look for the file in catalog
                            Query query = new Query()
                                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                    .append(FileDBAdaptor.QueryParams.URI.key(), path.toUri().toString())
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);

                            QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

                            if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                                logger.debug("Cannot delete " + path.toString() + ". The file could not be found in catalog.");
                                return FileVisitResult.CONTINUE;
                            }

                            if (fileQueryResult.getNumResults() > 1) {
                                logger.error("Internal error: More than one file was found in catalog for uri " + path.toString());
                                return FileVisitResult.CONTINUE;
                            }

                            File file = fileQueryResult.first();
                            String newPath = file.getPath().replaceFirst(basePath, suffixedPath);

                            // 1. Set the file status to deleting
                            ObjectMap update = new ObjectMap(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING);
                            fileDBAdaptor.update(file.getId(), update, QueryOptions.empty());

                            logger.debug("Deleting file '" + path.toString() + "' from filesystem and Catalog");

                            // 2. Delete the file from disk
                            ioManager.deleteFile(path.toUri());

                            // 3. Update the file status and path in the database. Set to delete
                            update = new ObjectMap()
                                    .append(FileDBAdaptor.QueryParams.PATH.key(), newPath)
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);

                            fileDBAdaptor.update(file.getId(), update, QueryOptions.empty());
                            logger.debug("DELETE: {} successfully removed from the filesystem and catalog", path.toString());

                            if (fileOrDirectory.getStatus().getName().equals(File.FileStatus.READY)) {
                                // Remove any reference to the file ids recently sent to the trash bin
                                jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId),
                                        Arrays.asList(file.getId()));
                            }
                        } catch (CatalogDBException | CatalogIOException e) {
                            logger.error(e.getMessage());
                            e.printStackTrace();
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException io) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        if (exc == null) {
                            // Only empty folders can be deleted for safety reasons
                            if (dir.toFile().list().length == 0) {
                                try {
                                    String folderUri = dir.toUri().toString();
//                                    if (folderUri.endsWith("/")) {
//                                        folderUri = folderUri.substring(0, folderUri.length() - 1);
//                                    }
                                    Query query = new Query()
                                            .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                            .append(FileDBAdaptor.QueryParams.URI.key(), folderUri)
                                            .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.DELETED);

                                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, QueryOptions.empty());

                                    if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
                                        logger.debug("Cannot remove " + dir.toString() + ". The directory could not be found in catalog.");
                                        return FileVisitResult.CONTINUE;
                                    }

                                    if (fileQueryResult.getNumResults() > 1) {
                                        logger.error("Internal error: More than one file was found in catalog for uri " + dir.toString());
                                        return FileVisitResult.CONTINUE;
                                    }

                                    File file = fileQueryResult.first();

                                    logger.debug("Removing empty directory '" + dir.toString() + "' from filesystem and catalog");

                                    ioManager.deleteDirectory(dir.toUri());

                                    String newPath = file.getPath().replaceFirst(basePath, suffixedPath);
                                    ObjectMap update = new ObjectMap()
                                            .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED)
                                            .append(FileDBAdaptor.QueryParams.PATH.key(), newPath);

                                    fileDBAdaptor.update(file.getId(), update, QueryOptions.empty());
                                    logger.debug("REMOVE: {} successfully removed from the filesystem and catalog", dir.toString());

                                    if (fileOrDirectory.getStatus().getName().equals(File.FileStatus.READY)) {
                                        // Remove any reference to the file ids recently sent to the trash bin
                                        jobDBAdaptor.extractFilesFromJobs(new Query(JobDBAdaptor.QueryParams.STUDY_ID.key(), studyId),
                                                Arrays.asList(file.getId()));
                                    }
                                } catch (CatalogDBException e) {
                                    logger.error(e.getMessage());
                                    e.printStackTrace();
                                } catch (CatalogIOException e) {
                                    logger.error(e.getMessage());
                                    e.printStackTrace();
                                }
                            } else {
                                logger.warn("REMOVE: {} Could not remove the directory as it is not empty.", dir.toString());
                            }
                            return FileVisitResult.CONTINUE;
                        } else {
                            // directory iteration failed
                            throw exc;
                        }
                    }
                });
            }
            removedFileResult = fileDBAdaptor.get(fileOrDirectory.getId(), QueryOptions.empty());
        }
        return removedFileResult;
    }

    /**
     * Create the parent directories that are needed.
     *
     * @param studyId          study id where they will be created.
     * @param userId           user that is creating the parents.
     * @param studyURI         Base URI where the created folders will be pointing to. (base physical location)
     * @param path             Path used in catalog as a virtual location. (whole bunch of directories inside the virtual
     *                         location in catalog)
     * @param checkPermissions Boolean indicating whether to check if the user has permissions to create a folder in the first directory
     *                         that is available in catalog.
     * @throws CatalogDBException
     */
    private void createParents(long studyId, String userId, URI studyURI, Path path, boolean checkPermissions) throws CatalogException {
        if (path == null) {
            if (checkPermissions) {
                authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_FILES);
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
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), stringPath);

        if (fileDBAdaptor.count(query).first() == 0) {
            createParents(studyId, userId, studyURI, path.getParent(), checkPermissions);
        } else {
            if (checkPermissions) {
                long fileId = fileDBAdaptor.getId(studyId, stringPath);
                authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);
            }
            return;
        }

        String parentPath = getParentPath(stringPath);
        long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
        // We obtain the permissions set in the parent folder and set them to the file or folder being created
        QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(studyId, parentFileId, userId, checkPermissions);

        URI completeURI = Paths.get(studyURI).resolve(path).toUri();

        // Create the folder in catalog
        File folder = new File(-1, path.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, completeURI,
                stringPath, TimeUtils.getTime(), TimeUtils.getTime(), "", new File.FileStatus(File.FileStatus.READY),
                false, 0, new Experiment(), Collections.emptyList(), new Job(), Collections.emptyList(), null, null,
                catalogManager.getStudyManager().getCurrentRelease(studyId), null);
        QueryResult<File> queryResult = fileDBAdaptor.insert(folder, studyId, new QueryOptions());
        // Propagate ACLs
        if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()), allFileAcls.getResult(), Entity.FILE);
        }
    }

    private QueryResult<File> privateLink(URI uriOrigin, String pathDestiny, long studyId, ObjectMap params, String sessionId)
            throws CatalogException, IOException {
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

        studyDBAdaptor.checkId(studyId);
        String userId = userManager.getUserId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_FILES);

        pathDestiny = ParamUtils.defaultString(pathDestiny, "");
        if (pathDestiny.length() == 1 && (pathDestiny.equals(".") || pathDestiny.equals("/"))) {
            pathDestiny = "";
        } else {
            if (pathDestiny.startsWith("/")) {
                pathDestiny.substring(1);
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
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!="
                        + File.FileStatus.REMOVED)
                .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), false);
        if (fileDBAdaptor.count(query).first() > 0) {
            throw new CatalogException("Cannot link to " + externalPathDestinyStr + ". The path already existed and is not external.");
        }

        // Check if the uri was already linked to that same path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), normalizedUri)
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!="
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
//                    .append(QueryOptions.INCLUDE, Arrays.asList(
//                            FileDBAdaptor.QueryParams.ID.key(),
//                            FileDBAdaptor.QueryParams.NAME.key(),
//                            FileDBAdaptor.QueryParams.TYPE.key(),
//                            FileDBAdaptor.QueryParams.PATH.key(),
//                            FileDBAdaptor.QueryParams.URI.key(),
//                            FileDBAdaptor.QueryParams.FORMAT.key(),
//                            FileDBAdaptor.QueryParams.BIOFORMAT.key()
//                    ));

            return fileDBAdaptor.get(query, queryOptions);
        }

        // Check if the uri was linked to other path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), normalizedUri)
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!="
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

        // Because pathDestiny can be null, we will use catalogPath as the virtual destiny where the files will be located in catalog.
        Path catalogPath = Paths.get(pathDestiny);

        if (pathDestiny.isEmpty()) {
            // If no destiny is given, everything will be linked to the root folder of the study.
            authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_FILES);
        } else {
            // Check if the folder exists
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), pathDestiny);
            if (fileDBAdaptor.count(query).first() == 0) {
                if (parents) {
                    // Get the base URI where the files are located in the study
                    URI studyURI = getStudyUri(studyId);
                    createParents(studyId, userId, studyURI, catalogPath, true);
                    // Create them in the disk
                    URI directory = Paths.get(studyURI).resolve(catalogPath).toUri();
                    catalogIOManagerFactory.get(directory).createDirectory(directory, true);
                } else {
                    throw new CatalogException("The path " + catalogPath + " does not exist in catalog.");
                }
            } else {
                // Check if the user has permissions to link files in the directory
                long fileId = fileDBAdaptor.getId(studyId, pathDestiny);
                authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);
            }
        }

        Path pathOrigin = Paths.get(normalizedUri);
        Path externalPathDestiny = Paths.get(externalPathDestinyStr);
        if (Paths.get(normalizedUri).toFile().isFile()) {

            // Check if there is already a file in the same path
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr);

            // Create the file
            if (fileDBAdaptor.count(query).first() == 0) {
                long size = Files.size(Paths.get(normalizedUri));

                String parentPath = getParentPath(externalPathDestinyStr);
                long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
                // We obtain the permissions set in the parent folder and set them to the file or folder being created
                QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(studyId, parentFileId, userId, true);

                File subfile = new File(-1, externalPathDestiny.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                        File.Bioformat.NONE, normalizedUri, externalPathDestinyStr, TimeUtils.getTime(), TimeUtils.getTime(), description,
                        new File.FileStatus(File.FileStatus.READY), true, size, new Experiment(), Collections.emptyList(), new Job(),
                        Collections.emptyList(), null, Collections.emptyMap(),
                        catalogManager.getStudyManager().getCurrentRelease(studyId), Collections.emptyMap());
                QueryResult<File> queryResult = fileDBAdaptor.insert(subfile, studyId, new QueryOptions());
                // Propagate ACLs
                if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                    authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()), allFileAcls.getResult(),
                            Entity.FILE);
                }

                File file = fileMetadataReader.setMetadataInformation(queryResult.first(), queryResult.first().getUri(),
                        new QueryOptions(), sessionId, false);
                queryResult.setResult(Arrays.asList(file));

                // If it is a transformed file, we will try to link it with the correspondent original file
                try {
                    if (isTransformedFile(file.getName())) {
                        matchUpVariantFiles(Arrays.asList(file), sessionId);
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
                                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                        if (fileDBAdaptor.count(query).first() == 0) {
                            // If the folder does not exist, we create it

                            String parentPath = getParentPath(destinyPath);
                            long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
                            // We obtain the permissions set in the parent folder and set them to the file or folder being created
                            QueryResult<FileAclEntry> allFileAcls;
                            try {
                                allFileAcls = authorizationManager.getAllFileAcls(studyId, parentFileId, userId, true);
                            } catch (CatalogException e) {
                                throw new RuntimeException(e);
                            }

                            File folder = new File(-1, dir.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN,
                                    File.Bioformat.NONE, dir.toUri(), destinyPath, TimeUtils.getTime(), TimeUtils.getTime(),
                                    description, new File.FileStatus(File.FileStatus.READY), true, 0, new Experiment(),
                                    Collections.emptyList(), new Job(), Collections.emptyList(), null,
                                    Collections.emptyMap(), catalogManager.getStudyManager().getCurrentRelease(studyId),
                                    Collections.emptyMap());
                            QueryResult<File> queryResult = fileDBAdaptor.insert(folder, studyId, new QueryOptions());

                            // Propagate ACLs
                            if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                                authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()),
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
                                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                        if (fileDBAdaptor.count(query).first() == 0) {
                            long size = Files.size(filePath);
                            // If the file does not exist, we create it

                            String parentPath = getParentPath(destinyPath);
                            long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
                            // We obtain the permissions set in the parent folder and set them to the file or folder being created
                            QueryResult<FileAclEntry> allFileAcls;
                            try {
                                allFileAcls = authorizationManager.getAllFileAcls(studyId, parentFileId, userId, true);
                            } catch (CatalogException e) {
                                throw new RuntimeException(e);
                            }

                            File subfile = new File(-1, filePath.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                                    File.Bioformat.NONE, filePath.toUri(), destinyPath, TimeUtils.getTime(), TimeUtils.getTime(),
                                    description, new File.FileStatus(File.FileStatus.READY), true, size, new Experiment(),
                                    Collections.emptyList(), new Job(), Collections.emptyList(), null,
                                    Collections.emptyMap(), catalogManager.getStudyManager().getCurrentRelease(studyId),
                                    Collections.emptyMap());
                            QueryResult<File> queryResult = fileDBAdaptor.insert(subfile, studyId, new QueryOptions());

                            // Propagate ACLs
                            if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                                authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()),
                                        allFileAcls.getResult(), Entity.FILE);
                            }

                            File file = fileMetadataReader.setMetadataInformation(queryResult.first(), queryResult.first().getUri(),
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
                    matchUpVariantFiles(transformedFiles, sessionId);
                }
            } catch (CatalogException e) {
                logger.warn("Matching avro to variant file: {}", e.getMessage());
            }

            // Check if the uri was already linked to that same path
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.URI.key(), "~^" + normalizedUri)
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!="
                            + File.FileStatus.REMOVED)
                    .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);

            // Limit the number of results and only some fields
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.LIMIT, 100);
            return fileDBAdaptor.get(query, queryOptions);
        }
    }

    /**
     * Check if the file or files inside the folder can be deleted / unlinked if they are being used in storage.
     *
     * @param studyId study id.
     * @param file    File or folder to be deleted / unlinked.
     */
    private void checkUsedInStorage(long studyId, File file) throws CatalogException {
        // We cannot delete/unlink a file or folder containing files that are indexed or being processed in storage
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*")
                .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(),
                        Arrays.asList(FileIndex.IndexStatus.TRANSFORMING, FileIndex.IndexStatus.LOADING,
                                FileIndex.IndexStatus.INDEXING, FileIndex.IndexStatus.READY));
        long count = fileDBAdaptor.count(query).first();
        if (count > 0) {
            throw new CatalogException("Cannot delete. " + count + " files have been or are being used in storage.");
        }

        // We check if any of the files to be removed are transformation files
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
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

            // Check the original files are not being indexed at the moment
            query = new Query(FileDBAdaptor.QueryParams.ID.key(), new ArrayList<>(fileIds));
            Map<Long, FileIndex> filesToUpdate;
            try (DBIterator<File> iterator = fileDBAdaptor.iterator(query, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FileDBAdaptor.QueryParams.INDEX.key(), FileDBAdaptor.QueryParams.ID.key())))) {
                filesToUpdate = new HashMap<>();
                while (iterator.hasNext()) {
                    File next = iterator.next();
                    String status = next.getIndex().getStatus().getName();
                    switch (status) {
                        case FileIndex.IndexStatus.READY:
                            // If they are already ready, we only need to remove the reference to the transformed files as they will be
                            // removed
                            next.getIndex().setTransformedFile(null);
                            filesToUpdate.put(next.getId(), next.getIndex());
                            break;
                        case FileIndex.IndexStatus.TRANSFORMED:
                            // We need to remove the reference to the transformed files and change their status from TRANSFORMED to NONE
                            next.getIndex().setTransformedFile(null);
                            next.getIndex().getStatus().setName(FileIndex.IndexStatus.NONE);
                            filesToUpdate.put(next.getId(), next.getIndex());
                            break;
                        case FileIndex.IndexStatus.NONE:
                        case FileIndex.IndexStatus.DELETED:
                        case FileIndex.IndexStatus.TRASHED:
                            break;
                        default:
                            throw new CatalogException("Cannot delete files that are in use in storage.");
                    }
                }
            }

            for (Map.Entry<Long, FileIndex> indexEntry : filesToUpdate.entrySet()) {
                fileDBAdaptor.update(indexEntry.getKey(), new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), indexEntry.getValue()),
                        QueryOptions.empty());
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

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), myPath);
        QueryResult<Long> fileQueryResult = fileDBAdaptor.count(query);

        if (fileQueryResult.first() > 0) {
            return CheckPath.FILE_EXISTS;
        }

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), myPath + "/");
        fileQueryResult = fileDBAdaptor.count(query);

        return fileQueryResult.first() > 0 ? CheckPath.DIRECTORY_EXISTS : CheckPath.FREE_PATH;
    }
}
