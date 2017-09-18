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

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.stats.VariantGlobalStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.FileUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.managers.api.IUserManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.monitor.daemons.IndexDaemon;
import org.opencb.opencga.catalog.utils.CatalogMemberValidator;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
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
import static org.opencb.opencga.catalog.utils.FileMetadataReader.VARIANT_STATS;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileManager extends AbstractManager implements IFileManager {

    private static final QueryOptions INCLUDE_STUDY_URI;
    private static final QueryOptions INCLUDE_FILE_URI_PATH;
    private static final Comparator<File> ROOT_FIRST_COMPARATOR;
    private static final Comparator<File> ROOT_LAST_COMPARATOR;

    protected static Logger logger;
    private FileMetadataReader fileMetadataReader;
    private IUserManager userManager;

    public static final String SKIP_TRASH = "SKIP_TRASH";
    public static final String DELETE_EXTERNAL_FILES = "DELETE_EXTERNAL_FILES";
    public static final String FORCE_DELETE = "FORCE_DELETE";

    static {
        INCLUDE_STUDY_URI = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key());
        INCLUDE_FILE_URI_PATH = new QueryOptions("include", Arrays.asList("projects.studies.files.uri", "projects.studies.files.path"));
        ROOT_FIRST_COMPARATOR = (f1, f2) -> (f1.getPath() == null ? 0 : f1.getPath().length())
                - (f2.getPath() == null ? 0 : f2.getPath().length());
        ROOT_LAST_COMPARATOR = (f1, f2) -> (f2.getPath() == null ? 0 : f2.getPath().length())
                - (f1.getPath() == null ? 0 : f1.getPath().length());

        logger = LoggerFactory.getLogger(FileManager.class);
    }

    @Deprecated
    public FileManager(AuthorizationManager authorizationManager, AuditManager auditManager,
                       DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       Properties catalogProperties) {
        super(authorizationManager, auditManager, catalogDBAdaptorFactory, ioManagerFactory, catalogProperties);
    }

    public FileManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                       DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory,
                       Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                configuration);
        fileMetadataReader = new FileMetadataReader(this.catalogManager);
        this.userManager = catalogManager.getUserManager();
    }

    public static List<String> getParentPaths(String filePath) {
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
    public URI getStudyUri(long studyId) throws CatalogException {
        return studyDBAdaptor.get(studyId, INCLUDE_STUDY_URI).first().getUri();
    }

    @Override
    public URI getUri(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            // This should never be executed, since version 0.8-rc1 the URI is stored always.
            return getUri(studyDBAdaptor.get(getStudyId(file.getId()), INCLUDE_STUDY_URI).first(), file);
        }
    }

    @Override
    public URI getUri(Study study, File file) throws CatalogException {
        ParamUtils.checkObj(study, "Study");
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            QueryResult<File> parents = getParents(file, false, INCLUDE_FILE_URI_PATH);
            for (File parent : parents.getResult()) {
                if (parent.getUri() != null) {
                    String relativePath = file.getPath().replaceFirst(parent.getPath(), "");
                    return parent.getUri().resolve(relativePath);
                }
            }
            URI studyUri = study.getUri() == null ? getStudyUri(study.getId()) : study.getUri();
            return file.getPath().isEmpty()
                    ? studyUri
                    : catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, file.getPath());
        }
    }

    @Deprecated
    @Override
    public URI getUri(URI studyUri, String relativeFilePath) throws CatalogException {
        ParamUtils.checkObj(studyUri, "studyUri");
        ParamUtils.checkObj(relativeFilePath, "relativeFilePath");

        return relativeFilePath.isEmpty()
                ? studyUri
                : catalogIOManagerFactory.get(studyUri).getFileUri(studyUri, relativeFilePath);
    }

    public URI getUri(long studyId, String filePath) throws CatalogException {
        ParamUtils.checkObj(filePath, "filePath");

        List<File> parents = getParents(false, new QueryOptions("include", "projects.studies.files.path,projects.studies.files.uri"),
                filePath, studyId).getResult();

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
            userId = userManager.getId(sessionId);
        } else {
            if (fileStr.contains(",")) {
                throw new CatalogException("More than one file found");
            }

            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);
            fileId = smartResolutor(fileStr, studyId);
        }

        return new MyResourceId(userId, studyId, fileId);
    }

    @Override
    public MyResourceIds getIds(String fileStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        if (StringUtils.isEmpty(fileStr)) {
            throw new CatalogException("Missing file parameter");
        }

        String userId;
        long studyId;
        List<Long> fileIds;

        if (StringUtils.isNumeric(fileStr) && Long.parseLong(fileStr) > configuration.getCatalog().getOffset()) {
            fileIds = Arrays.asList(Long.parseLong(fileStr));
            fileDBAdaptor.exists(fileIds.get(0));
            studyId = fileDBAdaptor.getStudyIdByFileId(fileIds.get(0));
            userId = userManager.getId(sessionId);
        } else {
            userId = userManager.getId(sessionId);
            studyId = catalogManager.getStudyManager().getId(userId, studyStr);

            String[] fileSplit = fileStr.split(",");
            fileIds = new ArrayList<>(fileSplit.length);
            for (String fileStrAux : fileSplit) {
                fileIds.add(smartResolutor(fileStrAux, studyId));
            }
        }

        return new MyResourceIds(userId, studyId, fileIds);
    }

    private Long smartResolutor(String fileName, long studyId) throws CatalogException {
        if (StringUtils.isNumeric(fileName) && Long.parseLong(fileName) > configuration.getCatalog().getOffset()) {
            long fileId = Long.parseLong(fileName);
            fileDBAdaptor.exists(fileId);
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

    @Deprecated
    @Override
    public Long getId(String userId, String fileStr) throws CatalogException {
        logger.info("Looking for file {}", fileStr);
        if (StringUtils.isNumeric(fileStr)) {
            return Long.parseLong(fileStr);
        }

        // Resolve the studyIds and filter the fileStr
        ObjectMap parsedSampleStr = parseFeatureId(userId, fileStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        if (studyIds.size() > 1) {
            throw new CatalogException("More than one study found.");
        }
        String fileName = parsedSampleStr.getString("featureName");

        return smartResolutor(fileName, studyIds.get(0));
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

    @Override
    public void matchUpVariantFiles(List<File> transformedFiles, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
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
            fileDBAdaptor.update(json.getId(), params);

            // Update transformed file
            logger.debug("Updating transformed relation");
            relatedFiles = transformedFile.getRelatedFiles();
            if (relatedFiles == null) {
                relatedFiles = new ArrayList<>();
            }
            relatedFiles.add(new File.RelatedFile(vcf.getId(), File.RelatedFile.Relation.PRODUCED_FROM));
            params = new ObjectMap(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFiles);
            fileDBAdaptor.update(transformedFile.getId(), params);

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
            fileDBAdaptor.update(vcf.getId(), params);

            // Update variant stats
            Path statsFile = Paths.get(json.getUri().getRawPath());
            try (InputStream is = FileUtils.newInputStream(statsFile)) {
                VariantSource variantSource = new ObjectMapper().readValue(is, VariantSource.class);
                VariantGlobalStats stats = variantSource.getStats();
                params = new ObjectMap(FileDBAdaptor.QueryParams.STATS.key(), new ObjectMap(VARIANT_STATS, stats));
                update(vcf.getId(), params, new QueryOptions(), sessionId);
            } catch (IOException e) {
                throw new CatalogException("Error reading file \"" + statsFile + "\"", e);
            }
        }
    }

    @Override
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

        fileDBAdaptor.update(fileId, parameters);
        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    @Deprecated
    @Override
    public Long getId(String id) throws CatalogException {
        if (StringUtils.isNumeric(id)) {
            return Long.parseLong(id);
        }

        String[] split = id.split("@", 2);
        if (split.length != 2) {
            return -1L;
        }
        String[] projectStudyPath = split[1].replace(':', '/').split("/", 3);
        if (projectStudyPath.length <= 2) {
            return -2L;
        }
        long projectId = projectDBAdaptor.getId(split[0], projectStudyPath[0]);
        long studyId = studyDBAdaptor.getId(projectId, projectStudyPath[1]);
        return fileDBAdaptor.getId(studyId, projectStudyPath[2]);
    }

    /**
     * Returns if a file is externally located.
     * <p>
     * A file externally located is the one with a URI or a parent folder with an external URI.
     *
     * @throws CatalogException
     */
    @Override
    public boolean isExternal(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
//        return file.getUri() != null;
        return file.isExternal();
    }

    @Override
    public QueryResult<FileIndex> updateFileIndexStatus(File file, String newStatus, String message, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
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
        fileDBAdaptor.update(file.getId(), params);
        auditManager.recordUpdate(AuditRecord.Resource.file, file.getId(), userId, params, null, null);

        return new QueryResult<>("Update file index", 0, 1, 1, "", "", Arrays.asList(index));
    }

    public boolean isRootFolder(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        return file.getPath().isEmpty();
    }

    @Override
    public QueryResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return getParents(true, options, get(fileId, new QueryOptions("include", "projects.studies.files.path"), sessionId).first()
                .getPath(), getStudyId(fileId));
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

    @Override
    public QueryResult<File> createFolder(String studyStr, String path, File.FileStatus status, boolean parents, String description,
                                          QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkPath(path, "folderPath");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = catalogManager.getUserManager().getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (!path.endsWith("/")) {
            path = path + "/";
        }

        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), path);
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, options);

        if (fileQueryResult.getNumResults() == 0) {
            fileQueryResult = create(Long.toString(studyId), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, path, null,
                    description, status, 0, -1, null, -1, null, null, parents, null, options, sessionId);
        } else {
            // The folder already exists
            authorizationManager.checkFilePermission(studyId, fileQueryResult.first().getId(), userId, FileAclEntry.FilePermissions.VIEW);
            fileQueryResult.setWarningMsg("Folder was already created");
        }

        fileQueryResult.setId("Create folder");
        return fileQueryResult;
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

    @Override
    public QueryResult<File> create(String studyStr, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                    String creationDate, String description, File.FileStatus status, long size, long experimentId,
                                    List<Sample> samples, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                                    boolean parents, String content, QueryOptions options, String sessionId)
            throws CatalogException {
        /** Check and set all the params and create a File object **/
        ParamUtils.checkPath(path, "filePath");
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        type = ParamUtils.defaultObject(type, File.Type.FILE);
        format = ParamUtils.defaultObject(format, File.Format.PLAIN);
        bioformat = ParamUtils.defaultObject(bioformat, File.Bioformat.NONE);
        description = ParamUtils.defaultString(description, "");
        if (type == File.Type.FILE) {
            status = (status == null) ? new File.FileStatus(File.FileStatus.STAGE) : status;
        } else {
            status = (status == null) ? new File.FileStatus(File.FileStatus.READY) : status;
        }
        if (size < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }
        if (experimentId > 0 && !jobDBAdaptor.experimentExists(experimentId)) {
            throw new CatalogException("Experiment { id: " + experimentId + "} does not exist.");
        }

        samples = ParamUtils.defaultObject(samples, ArrayList<Sample>::new);
        for (Sample sample : samples) {
            if (sample.getId() <= 0 || !sampleDBAdaptor.exists(sample.getId())) {
                throw new CatalogException("Sample { id: " + sample.getId() + "} does not exist.");
            }
        }

        if (jobId > 0 && !jobDBAdaptor.exists(jobId)) {
            throw new CatalogException("Job { id: " + jobId + "} does not exist.");
        }

        stats = ParamUtils.defaultObject(stats, HashMap<String, Object>::new);
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

//        if (!Objects.equals(status.getName(), File.FileStatus.STAGE) && type == File.Type.FILE) {
//            throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with status != STAGE and INDEXING");
//        }

        if (type == File.Type.DIRECTORY && !path.endsWith("/")) {
            path += "/";
        }
        if (type == File.Type.FILE && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        URI uri;
        try {
            if (type == File.Type.DIRECTORY) {
                uri = getFileUri(studyId, path, true);
            } else {
                uri = getFileUri(studyId, path, false);
            }
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }

        // FIXME: Why am I doing this? Why am I not throwing an exception if it already exists?
        // Check if it already exists
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), path)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";" + File.FileStatus.DELETED
                        + ";" + File.FileStatus.DELETING + ";" + File.FileStatus.PENDING_DELETE + ";" + File.FileStatus.REMOVED);
        if (fileDBAdaptor.count(query).first() > 0) {
            logger.warn("The file {} already exists in catalog", path);
        }
        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.URI.key(), uri)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + File.FileStatus.TRASHED + ";" + File.FileStatus.DELETED
                        + ";" + File.FileStatus.DELETING + ";" + File.FileStatus.PENDING_DELETE + ";" + File.FileStatus.REMOVED);
        if (fileDBAdaptor.count(query).first() > 0) {
            logger.warn("The uri {} of the file is already in catalog but on a different path", uri);
        }

        boolean external = isExternal(studyId, path, uri);
        File file = new File(-1, Paths.get(path).getFileName().toString(), type, format, bioformat, uri, path, TimeUtils.getTime(),
                TimeUtils.getTime(), description, status, external, size, new Experiment().setId(experimentId), samples,
                new Job().setId(jobId), Collections.emptyList(), Collections.emptyList(), null, stats,
                        catalogManager.getStudyManager().getCurrentRelease(studyId), attributes);

        //Find parent. If parents == true, create folders.
        String parentPath = getParentPath(file.getPath());

        long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
        boolean newParent = false;
        if (parentFileId < 0 && StringUtils.isNotEmpty(parentPath)) {
            if (parents) {
                newParent = true;
                parentFileId = create(Long.toString(studyId), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, parentPath,
                        file.getCreationDate(), "", new File.FileStatus(File.FileStatus.READY), 0, -1, samples, -1,
                        Collections.emptyMap(), Collections.emptyMap(), true, null, options, sessionId).first().getId();
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
        QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(userId, parentFileId, false);
        // Propagate ACLs
        if (allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()), allFileAcls.getResult(),
                    MongoDBAdaptorFactory.FILE_COLLECTION);
        }

        auditManager.recordAction(AuditRecord.Resource.file, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);

        matchUpVariantFiles(queryResult.getResult(), sessionId);

        return queryResult;
    }

    /**
     * Get the URI where a file should be in Catalog, given a study and a path.
     * @param studyId       Study identifier
     * @param path          Path to locate
     * @param directory     Boolean indicating if the file is a directory
     * @return              URI where the file should be placed
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

    @Override
    public QueryResult<File> get(Long id, QueryOptions options, String sessionId) throws CatalogException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getId(sessionId);
        Long studyId = getStudyId(id);
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.ID.key(), id)
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, options, userId);
        if (fileQueryResult.getNumResults() <= 0) {
            throw CatalogAuthorizationException.deny(userId, "view", "file", id, "");
        }
        fileQueryResult.setId(Long.toString(id));
        return fileQueryResult;
    }

    @Override
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
        String user = userManager.getId(sessionId);
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

    @Override
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

    @Override
    public QueryResult<File> get(Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        return get(query.getInt("studyId", -1), query, options, sessionId);
    }

    @Override
    public QueryResult<File> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getId(sessionId);

        if (studyId <= 0) {
            throw new CatalogDBException("Permission denied. Only the files of one study can be seen at a time.");
        } else {
            query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        }

        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        QueryResult<File> queryResult = fileDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    @Override
    public DBIterator<File> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getId(sessionId);

        if (studyId <= 0) {
            throw new CatalogDBException("Permission denied. Only the files of one study can be seen at a time.");
        } else {
            query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        }

        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        return fileDBAdaptor.iterator(query, options, userId);
    }

    @Override
    public QueryResult<File> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        query.append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<File> queryResult = fileDBAdaptor.get(query, options, userId);

        return queryResult;
    }

    @Override
    public QueryResult<File> count(String studyStr, Query query, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);

        // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        query.append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult<Long> queryResultAux = fileDBAdaptor.count(query, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);
        return new QueryResult<>("count", queryResultAux.getDbTime(), 0, queryResultAux.first(), queryResultAux.getWarningMsg(),
                queryResultAux.getErrorMsg(), Collections.emptyList());
    }

    @Override
    public QueryResult<Long> count(Query query, String sessionId) throws CatalogException {
        // Check the user exists in catalog
        String userId = catalogManager.getUserManager().getId(sessionId);
        if (query == null) {
            return new QueryResult<>("count");
        }

        return fileDBAdaptor.count(query);
    }

    @Override
    public QueryResult<File> get(String path, boolean recursive, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        String userId = userManager.getId(sessionId);

        // Prepare the path directory
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        if (!path.endsWith("/")) {
            path = path + "/";
        }

        if (recursive) {
            query.put(FileDBAdaptor.QueryParams.PATH.key(), "~^" + path + "*");
        } else {
            query.put(FileDBAdaptor.QueryParams.DIRECTORY.key(), path);
        }

        List<Long> studyIds;
        if (query.containsKey(FileDBAdaptor.QueryParams.STUDY_ID.key())) {
            studyIds = Arrays.asList(query.getLong(FileDBAdaptor.QueryParams.STUDY_ID.key()));
        } else {
            studyIds = studyDBAdaptor.getStudiesFromUser(userId,
                    new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.ID.key()))
                    .getResult()
                    .stream()
                    .map(Study::getId)
                    .collect(Collectors.toList());
        }

        QueryResult<File> fileQueryResult = new QueryResult<>("Search files by directory");
        for (Long studyId : studyIds) {
            query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
            QueryResult<File> tmpQueryResult = fileDBAdaptor.get(query, options, userId);
            if (tmpQueryResult.getResult().size() > 0) {
                fileQueryResult.getResult().addAll(tmpQueryResult.getResult());
                fileQueryResult.setDbTime(fileQueryResult.getDbTime() + tmpQueryResult.getDbTime());
            }
        }
        fileQueryResult.setNumResults(fileQueryResult.getResult().size());

        return fileQueryResult;
    }

    @Override
    public QueryResult<File> update(Long fileId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        if (fileId <= 0) {
            throw new CatalogException("File not found.");
        }
        String userId = userManager.getId(sessionId);
        File file = get(fileId, null, sessionId).first();
        Long studyId = getStudyId(fileId);

        if (isRootFolder(file)) {
            throw new CatalogException("Can not modify root folder");
        }

        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);
        for (Map.Entry<String, Object> param : parameters.entrySet()) {
            FileDBAdaptor.QueryParams queryParam = FileDBAdaptor.QueryParams.getParam(param.getKey());
            switch(queryParam) {
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
            String sampleIdStr = parameters.getString(FileDBAdaptor.QueryParams.SAMPLES.key());
//            parameters.remove(FileDBAdaptor.QueryParams.SAMPLES.key());

            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(sampleIdStr, Long.toString(studyId), sessionId);

            List<Sample> sampleList = new ArrayList<>(resourceIds.getResourceIds().size());
            for (Long sampleId : resourceIds.getResourceIds()) {
                sampleList.add(new Sample().setId(sampleId));
            }
//            fileDBAdaptor.addSamplesToFile(fileId, sampleList);
            parameters.put(FileDBAdaptor.QueryParams.SAMPLES.key(), sampleList);
        }

        //Name must be changed with "rename".
        if (parameters.containsKey("name")) {
            logger.info("Rename file using update method!");
            rename(fileId, parameters.getString("name"), sessionId);
        }

        String ownerId = studyDBAdaptor.getOwnerId(fileDBAdaptor.getStudyIdByFileId(fileId));
        fileDBAdaptor.update(fileId, parameters);
        QueryResult<File> queryResult = fileDBAdaptor.get(fileId, options);
        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
        userDBAdaptor.updateUserLastModified(ownerId);
        return queryResult;
    }

    @Override
    public List<QueryResult<File>> delete(String fileIdStr, @Nullable String studyStr, ObjectMap params, String sessionId)
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

        AbstractManager.MyResourceIds resource = catalogManager.getFileManager().getIds(fileIdStr, studyStr, sessionId);
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
                        fileDBAdaptor.update(fileId, updateParams);
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
                        fileDBAdaptor.update(query, updateParams);

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


    private void checkCanDelete(Query query) throws CatalogException {
        QueryResult<File> queryResult = fileDBAdaptor.get(query,
                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key()));
        List<Long> fileIds = queryResult.getResult().stream().map(File::getId).collect(Collectors.toList());
        if (fileIds.size() == 0) {
            logger.debug("Could not obtain any id given the query: {}", query.safeToString());
            throw new CatalogDBException("Could not obtain any id given the query");
        }

        checkCanDelete(fileIds);
    }

    private void checkCanDelete(List<Long> fileIds) throws CatalogException {
        if (fileIds == null || fileIds.size() == 0) {
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

    @Override
    public List<QueryResult<File>> delete(Query query, QueryOptions options, String sessionId) throws CatalogException, IOException {
        return null;
    }

    @Override
    public List<QueryResult<File>> restore(String ids, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public List<QueryResult<File>> restore(Query query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
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
            fileDBAdaptor.update(fileOrDirectory.getId(), update);

            // 2. Delete the file from disk
            ioManager.deleteFile(fileUri);

            // 3. Update the file status in the database. Set to delete
            update = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), fileOrDirectory.getPath() + suffixName);
            removedFileResult = fileDBAdaptor.update(fileOrDirectory.getId(), update);

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
                        fileDBAdaptor.update(file.getId(), update);
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
                            fileDBAdaptor.update(file.getId(), update);

                            logger.debug("Deleting file '" + path.toString() + "' from filesystem and Catalog");

                            // 2. Delete the file from disk
                            ioManager.deleteFile(path.toUri());

                            // 3. Update the file status and path in the database. Set to delete
                            update = new ObjectMap()
                                    .append(FileDBAdaptor.QueryParams.PATH.key(), newPath)
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);

                            fileDBAdaptor.update(file.getId(), update);
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

                                    fileDBAdaptor.update(file.getId(), update);
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
     * @param studyId study id where they will be created.
     * @param userId user that is creating the parents.
     * @param studyURI Base URI where the created folders will be pointing to. (base physical location)
     * @param path Path used in catalog as a virtual location. (whole bunch of directories inside the virtual location in catalog)
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
        if (stringPath.equals("/")) {
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
        QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(userId, parentFileId, checkPermissions);

        URI completeURI = Paths.get(studyURI).resolve(path).toUri();

        // Create the folder in catalog
        File folder = new File(-1, path.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, completeURI,
                stringPath, TimeUtils.getTime(), TimeUtils.getTime(), "", new File.FileStatus(File.FileStatus.READY),
                false, 0, new Experiment(), Collections.emptyList(), new Job(), Collections.emptyList(), allFileAcls.getResult(),
                null, null, catalogManager.getStudyManager().getCurrentRelease(studyId), null);
        QueryResult<File> queryResult = fileDBAdaptor.insert(folder, studyId, new QueryOptions());
        // Propagate ACLs
        if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()), allFileAcls.getResult(),
                    MongoDBAdaptorFactory.FILE_COLLECTION);
        }
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
        String userId = userManager.getId(sessionId);
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
        boolean resync  = params.getBoolean("resync", false);
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
                QueryResult<FileAclEntry> allFileAcls = authorizationManager.getAllFileAcls(userId, parentFileId, true);

                File subfile = new File(-1, externalPathDestiny.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                        File.Bioformat.NONE, normalizedUri, externalPathDestinyStr, TimeUtils.getTime(), TimeUtils.getTime(), description,
                        new File.FileStatus(File.FileStatus.READY), true, size, new Experiment(), Collections.emptyList(), new Job(),
                        Collections.emptyList(), allFileAcls.getResult(), null, Collections.emptyMap(),
                        catalogManager.getStudyManager().getCurrentRelease(studyId), Collections.emptyMap());
                QueryResult<File> queryResult = fileDBAdaptor.insert(subfile, studyId, new QueryOptions());
                // Propagate ACLs
                if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                    authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()), allFileAcls.getResult(),
                            MongoDBAdaptorFactory.FILE_COLLECTION);
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
                                allFileAcls = authorizationManager.getAllFileAcls(userId, parentFileId, true);
                            } catch (CatalogException e) {
                                throw new RuntimeException(e);
                            }

                            File folder = new File(-1, dir.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN,
                                    File.Bioformat.NONE, dir.toUri(), destinyPath, TimeUtils.getTime(), TimeUtils.getTime(),
                                    description, new File.FileStatus(File.FileStatus.READY), true, 0, new Experiment(),
                                    Collections.emptyList(), new Job(), Collections.emptyList(), allFileAcls.getResult(), null,
                                    Collections.emptyMap(), catalogManager.getStudyManager().getCurrentRelease(studyId),
                                    Collections.emptyMap());
                            QueryResult<File> queryResult = fileDBAdaptor.insert(folder, studyId, new QueryOptions());
                            // Propagate ACLs
                            if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                                authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()),
                                        allFileAcls.getResult(), MongoDBAdaptorFactory.FILE_COLLECTION);
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
                                allFileAcls = authorizationManager.getAllFileAcls(userId, parentFileId, true);
                            } catch (CatalogException e) {
                                throw new RuntimeException(e);
                            }

                            File subfile = new File(-1, filePath.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                                    File.Bioformat.NONE, filePath.toUri(), destinyPath, TimeUtils.getTime(), TimeUtils.getTime(),
                                    description, new File.FileStatus(File.FileStatus.READY), true, size, new Experiment(),
                                    Collections.emptyList(), new Job(), Collections.emptyList(), allFileAcls.getResult(), null,
                                    Collections.emptyMap(), catalogManager.getStudyManager().getCurrentRelease(studyId),
                                    Collections.emptyMap());
                            QueryResult<File> queryResult = fileDBAdaptor.insert(subfile, studyId, new QueryOptions());
                            // Propagate ACLs
                            if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                                authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getId()),
                                        allFileAcls.getResult(), MongoDBAdaptorFactory.FILE_COLLECTION);
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

            QueryResult<File> retFile = fileDBAdaptor.update(file.getId(), update);

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

                        fileDBAdaptor.update(file.getId(), update);

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

                            fileDBAdaptor.update(file.getId(), update);

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

    /**
     * Check if the file or files inside the folder can be deleted / unlinked if they are being used in storage.
     *
     * @param studyId study id.
     * @param file File or folder to be deleted / unlinked.
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
                fileDBAdaptor.update(indexEntry.getKey(), new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), indexEntry.getValue()));
            }
        }
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
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

        String userId = userManager.getId(sessionId);
        long studyId = catalogManager.getStudyManager().getId(userId, studyStr);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
            MyResourceIds resourceIds = catalogManager.getSampleManager().getIds(
                    query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()), Long.toString(studyId), sessionId);
            query.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), resourceIds.getResourceIds());
            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
        }

        // Add study id to the query
        query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.groupBy(query, fields, options);
        }

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

    @Override
    public QueryResult<File> rename(long fileId, String newName, String sessionId) throws CatalogException {
        ParamUtils.checkFileName(newName, "name");
        String userId = userManager.getId(sessionId);
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
        boolean isExternal = isExternal(file); //If the file URI is not null, the file is external located.
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

    @Deprecated
    @Override
    public QueryResult move(long fileId, String newPath, QueryOptions options, String sessionId) throws CatalogException {
        throw new UnsupportedOperationException();
        //TODO https://github.com/opencb/opencga/issues/136
    }

    @Override
    public QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(files, "files");
        String userId = userManager.getId(sessionId);

        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.WRITE_DATASETS);

        for (Long fileId : files) {
            if (fileDBAdaptor.getStudyIdByFileId(fileId) != studyId) {
                throw new CatalogException("Can't create a dataset with files from different studies.");
            }
            authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.VIEW);
        }

        Dataset dataset = new Dataset(-1, name, TimeUtils.getTime(), description, files, new Status(), attributes);
        QueryResult<Dataset> queryResult = datasetDBAdaptor.insert(dataset, studyId, options);
//        auditManager.recordCreation(AuditRecord.Resource.dataset, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.dataset, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public Long getStudyIdByDataset(long datasetId) throws CatalogException {
        return datasetDBAdaptor.getStudyIdByDatasetId(datasetId);
    }

    @Override
    public Long getDatasetId(String userId, String datasetStr) throws CatalogException {
        if (StringUtils.isNumeric(datasetStr)) {
            return Long.parseLong(datasetStr);
        }

        // Resolve the studyIds and filter the datasetName
        ObjectMap parsedSampleStr = parseFeatureId(userId, datasetStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String datasetName = parsedSampleStr.getString("featureName");

        Query query = new Query(DatasetDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(DatasetDBAdaptor.QueryParams.NAME.key(), datasetName);
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.datasets.id");
        QueryResult<Dataset> queryResult = datasetDBAdaptor.get(query, qOptions);
        if (queryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one dataset id found based on " + datasetName);
        } else if (queryResult.getNumResults() == 0) {
            return -1L;
        } else {
            return queryResult.first().getId();
        }
    }

    @Override
    public DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.VIEW);

        URI fileUri = getUri(get(fileId, null, sessionId).first());
        boolean ignoreCase = options.getBoolean("ignoreCase");
        boolean multi = options.getBoolean("multi");
        return catalogIOManagerFactory.get(fileUri).getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    @Override
    public DataInputStream download(long fileId, int start, int limit, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.DOWNLOAD);

        URI fileUri = getUri(get(fileId, null, sessionId).first());

        return catalogIOManagerFactory.get(fileUri).getFileObject(fileUri, start, limit);
    }

    @Override
    public DataInputStream head(long fileId, int lines, QueryOptions options, String sessionId) throws CatalogException {
        return download(fileId, 0, lines, options, sessionId);
    }

    @Override
    public QueryResult index(String fileIdStr, String studyStr, String type, Map<String, String> params, String sessionId)
            throws CatalogException {
        MyResourceIds resourceIds = getIds(fileIdStr, studyStr, sessionId);
        List<Long> fileFolderIdList = resourceIds.getResourceIds();
        long studyId = resourceIds.getStudyId();
        String userId = resourceIds.getUser();

        // Check they all belong to the same study
        for (Long fileId : fileFolderIdList) {
            if (fileId == -1) {
                throw new CatalogException("Could not find file or folder " + fileIdStr);
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
                outDir = new File().setId(getId(userId, path));
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
                description = "Transform variants from " + fileIdStr;
            } else if (load && !transform) {
                description = "Load variants from " + fileIdStr;
                jobName = "variant_load";
            } else {
                description = "Index variants from " + fileIdStr;
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
                    throw new CatalogException("Could not find file or folder " + fileIdStr);
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
                    throw new CatalogException("Could not find file or folder " + fileIdStr);
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
        params.put("sid", sessionId);
        List<File> outputList = outDir.getId() > 0 ? Arrays.asList(outDir) : Collections.emptyList();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(IndexDaemon.INDEX_TYPE, indexDaemonType);
        logger.info("job description: " + description);
        jobQueryResult = catalogManager.getJobManager().queue(studyId, jobName, description, "opencga-analysis.sh",
                Job.Type.INDEX, params, fileIdList, outputList, outDir, userId, attributes);
        jobQueryResult.first().setToolName(jobName);

        return jobQueryResult;
    }

    @Override
    public void setFileIndex(long fileId, FileIndex index, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
        fileDBAdaptor.update(fileId, parameters);

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    @Override
    public void setDiskUsage(long fileId, long size, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.SIZE.key(), size);
        fileDBAdaptor.update(fileId, parameters);

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    @Override
    public void setModificationDate(long fileId, String date, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.MODIFICATION_DATE.key(), date);
        fileDBAdaptor.update(fileId, parameters);

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    @Override
    public void setUri(long fileId, String uri, String sessionId) throws CatalogException {
        String userId = userManager.getId(sessionId);
        long studyId = getStudyId(fileId);
        authorizationManager.checkFilePermission(studyId, fileId, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.URI.key(), uri);
        fileDBAdaptor.update(fileId, parameters);

        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
    }

    @Override
    public List<QueryResult<FileAclEntry>> updateAcl(String file, String studyStr, String memberIds, File.FileAclParams fileAclParams,
                                                     String sessionId) throws CatalogException {
        int count = 0;
        count += StringUtils.isNotEmpty(file) ? 1 : 0;
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
            MyResourceIds ids = catalogManager.getSampleManager().getIds(fileAclParams.getSample(), studyStr, sessionId);

            Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ids.getResourceIds());
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key());
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(ids.getStudyId(), query, options, sessionId);

            Set<Long> fileSet = fileQueryResult.getResult().stream().map(File::getId).collect(Collectors.toSet());
            file = StringUtils.join(fileSet, ",");

            studyStr = Long.toString(ids.getStudyId());
        }

        // Obtain the resource ids
        MyResourceIds resourceIds = getIds(file, studyStr, sessionId);
        // Increase the list with the files/folders within the list of ids that correspond with folders
        resourceIds = getRecursiveFilesAndFolders(resourceIds);

        // Check the user has the permissions needed to change permissions over those files
        for (Long fileId : resourceIds.getResourceIds()) {
            authorizationManager.checkFilePermission(resourceIds.getStudyId(), fileId, resourceIds.getUser(),
                    FileAclEntry.FilePermissions.SHARE);
        }

        // Validate that the members are actually valid members
        List<String> members;
        if (memberIds != null && !memberIds.isEmpty()) {
            members = Arrays.asList(memberIds.split(","));
        } else {
            members = Collections.emptyList();
        }
        CatalogMemberValidator.checkMembers(catalogDBAdaptorFactory, resourceIds.getStudyId(), members);
//        catalogManager.getStudyManager().membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

        String collectionName = MongoDBAdaptorFactory.FILE_COLLECTION;

        switch (fileAclParams.getAction()) {
            case SET:
                return authorizationManager.setAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
            case ADD:
                return authorizationManager.addAcls(resourceIds.getStudyId(), resourceIds.getResourceIds(), members, permissions,
                        collectionName);
            case REMOVE:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, permissions, collectionName);
            case RESET:
                return authorizationManager.removeAcls(resourceIds.getResourceIds(), members, null, collectionName);
            default:
                throw new CatalogException("Unexpected error occurred. No valid action found.");
        }
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
        if (pathList.size() > 0) {
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

}
