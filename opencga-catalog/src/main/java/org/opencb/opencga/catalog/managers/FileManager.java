package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.NotImplementedException;
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
import org.opencb.opencga.catalog.config.CatalogConfiguration;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DatasetDBAdaptor;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogIOException;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.managers.api.IFileManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.DatasetAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.StudyAclEntry;
import org.opencb.opencga.catalog.monitor.daemons.IndexDaemon;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

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
                       CatalogConfiguration catalogConfiguration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory,
                catalogConfiguration);
        fileMetadataReader = new FileMetadataReader(this.catalogManager);
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
    public Long getId(String fileStr, long studyId, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);

        logger.info("Looking for file {}", fileStr);
        if (StringUtils.isNumeric(fileStr)) {
            return Long.parseLong(fileStr);
        }

        // Resolve the studyIds and filter the fileStr
        ObjectMap parsedSampleStr = parseFeatureId(userId, fileStr);
        List<Long> studyIds = Arrays.asList(studyId);
        String fileName = parsedSampleStr.getString("featureName");

        return getId(studyIds, fileName);
    }

    @Override
    public Long getId(String userId, String fileStr) throws CatalogException {
        logger.info("Looking for file {}", fileStr);
        if (StringUtils.isNumeric(fileStr)) {
            return Long.parseLong(fileStr);
        }

        // Resolve the studyIds and filter the fileStr
        ObjectMap parsedSampleStr = parseFeatureId(userId, fileStr);
        List<Long> studyIds = getStudyIds(parsedSampleStr);
        String fileName = parsedSampleStr.getString("featureName");

        return getId(studyIds, fileName);
    }

    @Override
    public void matchUpVariantFiles(List<File> avroFiles, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        for (File avroFile : avroFiles) {
            authorizationManager.checkFilePermission(avroFile.getId(), userId, FileAclEntry.FilePermissions.UPDATE);
            if (!File.Format.AVRO.equals(avroFile.getFormat())) {
                // Skip the file.
                logger.warn("The file {} is not a proper AVRO file", avroFile.getName());
                continue;
            }

            Long studyId = getStudyId(avroFile.getId());
            String variantPathName = avroFile.getPath().replace(".variants.avro.gz", "");
            logger.info("Looking for vcf file in path {}", variantPathName);
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), variantPathName)
                    .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
                    .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(
                            FileIndex.IndexStatus.NONE, FileIndex.IndexStatus.TRANSFORMING, FileIndex.IndexStatus.INDEXING,
                            FileIndex.IndexStatus.READY
                    ));

            QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

            if (fileQueryResult.getNumResults() == 0) {
                // Search in the whole study
                String variantFileName = avroFile.getName().replace(".variants.avro.gz", "");
                logger.info("Looking for vcf file by name {}", variantFileName);
                query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.NAME.key(), variantFileName)
                        .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT)
                        .append(FileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), Arrays.asList(
                                FileIndex.IndexStatus.NONE, FileIndex.IndexStatus.TRANSFORMING, FileIndex.IndexStatus.INDEXING,
                                FileIndex.IndexStatus.READY
                        ));
//                        .append(CatalogFileDBAdaptor.QueryParams.INDEX_TRANSFORMED_FILE.key(), null);
                fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());
            }

            if (fileQueryResult.getNumResults() == 0 || fileQueryResult.getNumResults() > 1) {
                // VCF file not found
                logger.warn("The vcf file corresponding to the file " + avroFile.getName() + " could not be found");
                continue;
            }
            File vcf = fileQueryResult.first();

            // Look for the json file. It should be in the same directory where the avro file is.
            String jsonPathName = avroFile.getPath().replace(".variants.avro.gz", ".file.json.gz");
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), jsonPathName)
                    .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON);
            fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());
            if (fileQueryResult.getNumResults() != 1) {
                // Skip. This should not ever happen
                logger.warn("The json file corresponding to the file " + avroFile.getName() + " could not be found");
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
//            update(json.getId(), params, new QueryOptions(), sessionId);

            // Update avro file
            logger.debug("Updating avro relation");
            relatedFiles = avroFile.getRelatedFiles();
            if (relatedFiles == null) {
                relatedFiles = new ArrayList<>();
            }
            relatedFiles.add(new File.RelatedFile(vcf.getId(), File.RelatedFile.Relation.PRODUCED_FROM));
            params = new ObjectMap(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFiles);
            fileDBAdaptor.update(avroFile.getId(), params);
//            update(avroFile.getId(), params, new QueryOptions(), sessionId);

            // Update vcf file
            logger.debug("Updating vcf relation");
            FileIndex index = vcf.getIndex();
            if (index.getTransformedFile() == null) {
                index.setTransformedFile(new FileIndex.TransformedFile(avroFile.getId(), json.getId()));
            }
            String status = vcf.getIndex().getStatus().getName();
            if (FileIndex.IndexStatus.NONE.equals(status)) {
                // If TRANSFORMED, TRANSFORMING, etc, do not modify the index status
                index.setStatus(new FileIndex.IndexStatus(FileIndex.IndexStatus.TRANSFORMED));
            }
            params = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
            fileDBAdaptor.update(vcf.getId(), params);
//            FileIndex.TransformedFile transformedFile = new FileIndex.TransformedFile(avroFile.getId(), json.getId());
//            params = new ObjectMap()
//                    .append(CatalogFileDBAdaptor.QueryParams.INDEX_TRANSFORMED_FILE.key(), transformedFile)
//                    .append(CatalogFileDBAdaptor.QueryParams.INDEX_STATUS_NAME.key(), FileIndex.IndexStatus.TRANSFORMED);

//            update(vcf.getId(), params, new QueryOptions(), sessionId);

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

    private Long getId(List<Long> studyIds, String fileName) throws CatalogException {
        if (fileName.startsWith("/")) {
            fileName = fileName.substring(1);
        }

        // We search as a path
        Query query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
                .append(FileDBAdaptor.QueryParams.PATH.key(), fileName);
//                .append(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=EMPTY");
        QueryOptions qOptions = new QueryOptions(QueryOptions.INCLUDE, "projects.studies.files.id");
        QueryResult<File> pathQueryResult = fileDBAdaptor.get(query, qOptions);
        if (pathQueryResult.getNumResults() > 1) {
            throw new CatalogException("Error: More than one file id found based on " + fileName);
        }

        if (!fileName.contains("/")) {
            // We search as a fileName as well
            query = new Query(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyIds)
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
                return -1L;
            }
        }

        if (pathQueryResult.getNumResults() == 1) {
            return pathQueryResult.first().getId();
        } else {
            return -1L;
        }
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
    public QueryResult<FileIndex> updateFileIndexStatus(File file, String newStatus, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getId(sessionId);
        authorizationManager.checkFilePermission(file.getId(), userId, FileAclEntry.FilePermissions.UPDATE);

        FileIndex index = file.getIndex();
        if (index != null) {
            if (!FileIndex.IndexStatus.isValid(newStatus)) {
                throw new CatalogException("The status " + newStatus + " is not a valid status.");
            }

//                index.setStatus(newStatus);
            if (FileIndex.IndexStatus.isValid(newStatus)) {
                index.getStatus().setName(newStatus);
                index.getStatus().setCurrentDate();
            }
        } else {
            index = new FileIndex(userId, TimeUtils.getTime(), new FileIndex.IndexStatus(newStatus), -1, new ObjectMap());
        }
        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.INDEX.key(), index);
        fileDBAdaptor.update(file.getId(), params);

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

    @Deprecated
    @Override
    public QueryResult<File> create(ObjectMap objectMap, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Deprecated create method.");
    }

    @Override
    public QueryResult<File> createFolder(long studyId, String path, File.FileStatus status, boolean parents, String description,
                                          QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkPath(path, "folderPath");
        options = ParamUtils.defaultObject(options, QueryOptions::new);
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
            return create(studyId, File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, path, null, description, status, 0, -1,
                    null, -1, null, null, parents, options, sessionId);
        }
        // The folder already exists
        // Check if the user had permissions
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(fileQueryResult.first().getId(), userId, FileAclEntry.FilePermissions.CREATE);
        return fileQueryResult;
    }

    @Override
    public QueryResult<File> create(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                    String creationDate, String description, File.FileStatus status, long diskUsage, long experimentId,
                                    List<Long> sampleIds, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                                    boolean parents, QueryOptions options, String sessionId) throws CatalogException {
        /** Check and set all the params and create a File object **/
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkPath(path, "filePath");

        type = ParamUtils.defaultObject(type, File.Type.FILE);
        format = ParamUtils.defaultObject(format, File.Format.PLAIN);  //TODO: Inference from the file name
        bioformat = ParamUtils.defaultObject(bioformat, File.Bioformat.NONE);
//        creationDate = ParamUtils.defaultString(creationDate, TimeUtils.getTime());
        description = ParamUtils.defaultString(description, "");
        if (type == File.Type.FILE) {
            status = (status == null) ? new File.FileStatus(File.FileStatus.STAGE) : status;
        } else {
            status = (status == null) ? new File.FileStatus(File.FileStatus.READY) : status;
        }
        if (diskUsage < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }
        if (experimentId > 0 && !jobDBAdaptor.experimentExists(experimentId)) {
            throw new CatalogException("Experiment { id: " + experimentId + "} does not exist.");
        }
        sampleIds = ParamUtils.defaultObject(sampleIds, LinkedList<Long>::new);

        for (Long sampleId : sampleIds) {
            if (!sampleDBAdaptor.exists(sampleId)) {
                throw new CatalogException("Sample { id: " + sampleId + "} does not exist.");
            }
        }

        if (jobId > 0 && !jobDBAdaptor.exists(jobId)) {
            throw new CatalogException("Job { id: " + jobId + "} does not exist.");
        }

        stats = ParamUtils.defaultObject(stats, HashMap<String, Object>::new);
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        if (!Objects.equals(status.getName(), File.FileStatus.STAGE) && type == File.Type.FILE) {
//            if (!authorizationManager.getUserRole(userId).equals(User.Role.ADMIN)) {
            throw new CatalogException("Permission denied. Required ROLE_ADMIN to create a file with status != STAGE and INDEXING");
//            }
        }

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

        //Create file object
//        File file = new File(-1, Paths.get(path).getFileName().toString(), type, format, bioformat,
//                path, ownerId, creationDate, description, status, diskUsage, experimentId, sampleIds, jobId,
//                new LinkedList<>(), stats, attributes);

        boolean external = isExternal(studyId, path, uri);
        File file = new File(-1, Paths.get(path).getFileName().toString(), type, format, bioformat, uri, path, TimeUtils.getTime(),
                TimeUtils.getTime(), description, status, external, diskUsage, experimentId, sampleIds, jobId, Collections.emptyList(),
                Collections.emptyList(), null, stats, attributes);

        //Find parent. If parents == true, create folders.
        Path parent = Paths.get(file.getPath()).getParent();
        String parentPath;
//        boolean isRoot = false;
        if (parent == null) {   //If parent == null, the file is in the root of the study
            parentPath = "";
//            isRoot = true;
        } else {
            parentPath = parent.toString() + "/";
        }

        long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
        boolean newParent = false;
        if (parentFileId < 0 && parent != null) {
            if (parents) {
                newParent = true;
                parentFileId = create(studyId, File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, parent.toString(),
                        file.getCreationDate(), "", new File.FileStatus(File.FileStatus.READY), 0, -1,
                        Collections.<Long>emptyList(), -1, Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap(), true,
                        options, sessionId).first().getId();
            } else {
                throw new CatalogDBException("Directory not found " + parent.toString());
            }
        }

        //Check permissions
        if (parentFileId < 0) {
            throw new CatalogException("Unable to create file without a parent file");
        } else {
            if (!newParent) {
                //If parent has been created, for sure we have permissions to create the new file.
                authorizationManager.checkFilePermission(parentFileId, userId, FileAclEntry.FilePermissions.CREATE);
            }
        }


        //Check external file
//        boolean isExternal = isExternal(file);

        if (file.getType() == File.Type.DIRECTORY && Objects.equals(file.getStatus().getName(), File.FileStatus.READY)) {
//            URI fileUri = getFileUri(studyId, file.getPath());
            CatalogIOManager ioManager = catalogIOManagerFactory.get(uri);
            ioManager.createDirectory(uri, parents);
        }

        QueryResult<File> queryResult = fileDBAdaptor.insert(file, studyId, options);
//        auditManager.recordCreation(AuditRecord.Resource.file, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.file, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
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
        ParamUtils.checkParameter(sessionId, "sessionId");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        authorizationManager.checkFilePermission(id, userId, CatalogPermission.READ);
        authorizationManager.checkFilePermission(id, userId, FileAclEntry.FilePermissions.VIEW);

        QueryResult<File> fileQueryResult = fileDBAdaptor.get(id, options);
        authorizationManager.filterFiles(userId, getStudyId(id), fileQueryResult.getResult());
        fileQueryResult.setNumResults(fileQueryResult.getResult().size());
        return fileQueryResult;
    }

    @Override
    public QueryResult<File> getParent(long fileId, QueryOptions options, String sessionId)
            throws CatalogException {

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        File file = get(fileId, null, sessionId).first();
        Path parent = Paths.get(file.getPath()).getParent();
        String parentPath;
        if (parent == null) {
            parentPath = "";
        } else {
            parentPath = parent.toString().endsWith("/") ? parent.toString() : parent.toString() + "/";
        }
        return get(fileDBAdaptor.getId(studyId, parentPath), options, sessionId);
    }

    @Override
    public QueryResult<FileTree> getTree(String fileIdStr, Query query, QueryOptions queryOptions, int maxDepth, String sessionId)
            throws CatalogException {
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

        String userId = catalogManager.getUserManager().getId(sessionId);

        // Check 1. No comma-separated values are valid, only one single File or Directory can be deleted.
        Long fileId = getId(userId, fileIdStr);
        fileDBAdaptor.checkId(fileId);
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);

        // Check if we can obtain the file from the dbAdaptor properly.
        QueryOptions qOptions = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(),
                        FileDBAdaptor.QueryParams.ID.key(), FileDBAdaptor.QueryParams.TYPE.key()));
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, qOptions);
        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
            throw new CatalogException("An error occurred with the database.");
        }

        // Check if the id does not correspond to a directory
        if (!fileQueryResult.first().getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogException("The file introduced is not a directory.");
        }

        // Call recursive method
        FileTree fileTree = getTree(fileQueryResult.first(), query, queryOptions, maxDepth, userId);

        int dbTime = (int) (System.currentTimeMillis() - startTime);
        int numResults = countFilesInTree(fileTree);

        return new QueryResult<>("File tree", dbTime, numResults, numResults, "", "", Arrays.asList(fileTree));
    }

    private FileTree getTree(File folder, Query query, QueryOptions queryOptions, int maxDepth, String userId)
            throws CatalogDBException {

        if (maxDepth == 0) {
            return null;
        }

        try {
            authorizationManager.checkFilePermission(folder.getId(), userId, FileAclEntry.FilePermissions.VIEW);
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
                FileTree subTree = getTree(fileAux, query, queryOptions, maxDepth - 1, userId);
                if (subTree != null) {
                    children.add(subTree);
                }
            } else {
                try {
                    authorizationManager.checkFilePermission(fileAux.getId(), userId, FileAclEntry.FilePermissions.VIEW);
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
        ParamUtils.checkParameter(sessionId, "sessionId");
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        if (studyId <= 0) {
            throw new CatalogDBException("Permission denied. Only the files of one study can be seen at a time.");
        } else {
            if (!authorizationManager.memberHasPermissionsInStudy(studyId, userId)) {
                throw CatalogAuthorizationException.deny(userId, "view", "files", studyId, null);
            }
            query.put(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        }

        QueryResult<File> queryResult = fileDBAdaptor.get(query, options);
        authorizationManager.filterFiles(userId, studyId, queryResult.getResult());
        queryResult.setNumResults(queryResult.getResult().size());

        return queryResult;
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
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

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
            QueryResult<File> tmpQueryResult = fileDBAdaptor.get(query, options);
            authorizationManager.filterFiles(userId, studyId, tmpQueryResult.getResult());
            if (tmpQueryResult.getResult().size() > 0) {
                fileQueryResult.getResult().addAll(tmpQueryResult.getResult());
                fileQueryResult.setDbTime(fileQueryResult.getDbTime() + tmpQueryResult.getDbTime());
            }
        }
        fileQueryResult.setNumResults(fileQueryResult.getResult().size());
        fileQueryResult.setNumTotalResults(fileQueryResult.getResult().size());

        return fileQueryResult;
    }

    @Override
    public QueryResult<File> update(Long fileId, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        ParamUtils.checkParameter(sessionId, "sessionId");
        if (fileId <= 0) {
            throw new CatalogException("File not found.");
        }
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        File file = get(fileId, null, sessionId).first();

        if (isRootFolder(file)) {
            throw new CatalogException("Can not modify root folder");
        }

        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.UPDATE);
        for (String s : parameters.keySet()) {
            switch (s) { //Special cases
                //Can be modified anytime
                case "format":
                case "bioformat":
                case "description":
                case "status.name":
                case "attributes":
                case "stats":
                case "index":
                case "sampleIds":
                case "jobId":
                case "relatedFiles":
                    break;
                case "uri":
                    logger.info("File {id: " + fileId + "} uri modified. New value: " + parameters.get("uri"));
                    break;
                //Can only be modified when file.status == STAGE
                case "creationDate":
                case "modificationDate":
                case "diskUsage":
//                            if (!file.getName().equals(File.Status.STAGE)) {
//                                throw new CatalogException("Parameter '" + s + "' can't be changed when " +
//                                        "status == " + file.getName().name() + ". " +
//                                        "Required status STAGE or admin account");
//                            }
                    break;
                //Path and Name must be changed with "raname" and/or "move" methods.
                case "path":
                case "name":
                    break;
                case "type":
                default:
                    throw new CatalogException("Parameter '" + s + "' can't be changed. "
                            + "Requires admin account");
            }
        }

        //Path and Name must be changed with "raname" and/or "move" methods.
        if (parameters.containsKey("name")) {
            logger.info("Rename file using update method!");
            rename(fileId, parameters.getString("name"), sessionId);
        }
        if (parameters.containsKey("path")) {
            logger.info("Move file using update method!");
            move(fileId, parameters.getString("path"), options, sessionId);
        }

        String ownerId = studyDBAdaptor.getOwnerId(fileDBAdaptor.getStudyIdByFileId(fileId));
        fileDBAdaptor.update(fileId, parameters);
        QueryResult<File> queryResult = fileDBAdaptor.get(fileId, options);
        auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, parameters, null, null);
        userDBAdaptor.updateUserLastModified(ownerId);
        return queryResult;
    }

//    @Override
//    public QueryResult<File> delete(Long id, QueryOptions options, String sessionId) throws CatalogException {
//        return deleteOld(id, options, sessionId);
//    }

//    @Deprecated
//    @Override
//    public QueryResult<File> delete(Long fileId, QueryOptions options, String sessionId)
//            throws CatalogException {        //Safe delete: Don't delete. Just rename file and set {deleting:true}
//        ParamUtils.checkParameter(sessionId, "sessionId");
//        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//
//        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DELETE);
//
//        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
//        long projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
//        String ownerId = projectDBAdaptor.getProjectOwnerId(projectId);
//
//        File file = fileDBAdaptor.getFile(fileId, null).first();
//
//        if (isRootFolder(file)) {
//            throw new CatalogException("Can not delete root folder");
//        }
//
//        QueryResult<File> result = checkCanDeleteFile(file, userId);
//        if (result != null) {
//            return result;
//        }
//
//        userDBAdaptor.updateUserLastModified(ownerId);
//
//        ObjectMap objectMap = new ObjectMap();
//        objectMap.put(CatalogFileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.TRASHED);
//        objectMap.put(CatalogFileDBAdaptor.QueryParams.STATUS_DATE.key(), System.currentTimeMillis());
//
//        switch (file.getType()) {
//            case DIRECTORY: {
//                QueryResult<File> allFilesInFolder = fileDBAdaptor.getAllFilesInFolder(studyId, file.getPath(), null);
//                // delete recursively. Walk tree depth first
//                for (File subfolder : allFilesInFolder.getResult()) {
//                    if (subfolder.getType() == File.Type.DIRECTORY) {
//                        delete(subfolder.getId(), null, sessionId);
//                    }
//                }
//                //Check can delete files
//                for (File subfile : allFilesInFolder.getResult()) {
//                    if (subfile.getType() == File.Type.FILE) {
//                        checkCanDeleteFile(subfile, userId);
//                    }
//                }
//                for (File subfile : allFilesInFolder.getResult()) {
//                    if (subfile.getType() == File.Type.FILE) {
//                        delete(subfile.getId(), null, sessionId);
//                    }
//                }
//
//                QueryResult<File> queryResult = fileDBAdaptor.update(fileId, objectMap);
////                QueryResult<File> queryResult = rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
//                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, objectMap, null, null);
//                return queryResult; //TODO: Return the modified file
//            }
//            case FILE: {
////                rename(fileId, ".deleted_" + TimeUtils.getTime() + "_" + file.getName(), sessionId);
//                QueryResult<File> queryResult = fileDBAdaptor.update(fileId, objectMap);
//                auditManager.recordUpdate(AuditRecord.Resource.file, fileId, userId, objectMap, null, null);
//                return queryResult; //TODO: Return the modified file
//            }
//            default:
//                break;
//        }
//        return null;
//    }

    @Override
    public List<QueryResult<File>> delete(String fileIdStr, QueryOptions options, String sessionId) throws CatalogException, IOException {
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

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        // FIXME use userManager instead of userDBAdaptor
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        // Check 1. No comma-separated values are valid, only one single File or Directory can be deleted.
        List<Long> fileIds = getIds(userId, fileIdStr);
        List<QueryResult<File>> queryResultList = new ArrayList<>(fileIds.size());
        // TODO: All the throws should be catched and put in the error field of queryResult
        for (Long fileId : fileIds) {
            fileDBAdaptor.checkId(fileId);

            // Check 2. User has the proper permissions to delete the file.
            authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DELETE);

            // Check if we can obtain the file from the dbAdaptor properly.
            QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, options);
            if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
                throw new CatalogException("Cannot delete file '" + fileIdStr + "'. There was an error with the database.");
            }
            File file = fileQueryResult.first();

            // Check 3.
            // If file is not externally linked or if it is external but with DELETE_EXTERNAL_FILES set to true then can be deleted.
            // This prevents external linked files to be accidentally deleted.
            // If file is linked externally and DELETE_EXTERNAL_FILES is false then we just unlink the file.
            if (file.isExternal() && !options.getBoolean(DELETE_EXTERNAL_FILES, false)) {
                queryResultList.add(unlink(fileIdStr, options, sessionId));
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
            // We cannot delete a folder containing files or folders with status missing or staged
            long studyId = -1;
            if (file.getType().equals(File.Type.DIRECTORY)) {
                studyId = fileDBAdaptor.getStudyIdByFileId(fileId);

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
            if (file.getType().equals(File.Type.DIRECTORY)) {
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

            if (options.getBoolean(SKIP_TRASH, false)) {
                deletedFileResult = deleteFromDisk(file, userId, options);
            } else {
                if (fileStatus.equalsIgnoreCase(File.FileStatus.READY)) {

                    if (file.getType().equals(File.Type.FILE)) {
                        deletedFileResult = fileDBAdaptor.delete(fileId, new QueryOptions());
                    } else {
                        if (studyId == -1) {
                            studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
                        }

                        // Send to trash all the files and subfolders
                        Query query = new Query()
                                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), "~" + file.getPath() + "*")
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.READY);
                        QueryResult<File> allFiles = fileDBAdaptor.get(query,
                                new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.ID.key()));

                        if (allFiles != null && allFiles.getNumResults() > 0) {
                            for (File fileToDelete : allFiles.getResult()) {
                                fileDBAdaptor.delete(fileToDelete.getId(), new QueryOptions());
                            }
                        }

                        deletedFileResult = fileDBAdaptor.get(fileId, new QueryOptions());
                    }
                }
            }
            queryResultList.add(deletedFileResult);
        }

        return queryResultList;
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

    private QueryResult<File> deleteFromDisk(File fileOrDirectory, String userId, QueryOptions options)
            throws CatalogException, IOException {
        QueryResult<File> removedFileResult;

        // Check permissions for the current file
        authorizationManager.checkFilePermission(fileOrDirectory.getId(), userId, FileAclEntry.FilePermissions.DELETE);

        // Not external file
        URI fileUri = getUri(fileOrDirectory);
        Path filesystemPath = Paths.get(fileUri);
//        FileUtils.checkFile(filesystemPath);
        CatalogIOManager ioManager = catalogIOManagerFactory.get(fileUri);

        long studyId = fileDBAdaptor.getStudyIdByFileId(fileOrDirectory.getId());
        String suffixName = ".DELETED_" + TimeUtils.getTime();

        // If file is not a directory then we can just delete it from disk and update Catalog.
        if (fileOrDirectory.getType().equals(File.Type.FILE)) {
            // 1. Set the file status to deleting
            ObjectMap update = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING);
            fileDBAdaptor.delete(fileOrDirectory.getId(), update, options);

            // 2. Delete the file from disk
            ioManager.deleteFile(fileUri);

            // 3. Update the file status in the database. Set to delete
            update = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), fileOrDirectory.getPath() + suffixName);
            removedFileResult = fileDBAdaptor.delete(fileOrDirectory.getId(), update, options);
        } else {
            // Directories can be marked to be deferred removed by setting FORCE_DELETE to false, then File daemon will remove it.
            // In this mode directory is just renamed and URIs and Paths updated in Catalog. By default removal is deferred.
            if (!options.getBoolean(FORCE_DELETE, false)
                    && !fileOrDirectory.getStatus().getName().equals(File.FileStatus.PENDING_DELETE)) {
                // Rename the directory in the filesystem.
                URI newURI = Paths.get(Paths.get(fileUri).toString() + suffixName).toUri();

                String basePath = Paths.get(fileOrDirectory.getPath()).toString();
                String suffixedPath = basePath + suffixName;

                // Get all the files that starts with path
                logger.debug("Looking for files and folders inside {}", fileOrDirectory.getPath());
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                        .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + fileOrDirectory.getPath() + "*")
                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), fileOrDirectory.getStatus().getName());
                QueryResult<File> queryResult = fileDBAdaptor.get(query, new QueryOptions());

                if (queryResult != null && queryResult.getNumResults() > 0) {
                    logger.debug("Renaming {} to {}", fileUri.toString(), newURI.toString());
                    ioManager.rename(fileUri, newURI);

                    logger.debug("Changing the URI in catalog to {} and setting the status to {}", newURI.toString(),
                            File.FileStatus.PENDING_DELETE);

                    // We update the uri and status of all the files and folders so it can be later deleted by the daemon
                    for (File file : queryResult.getResult()) {

                        String newUri = file.getUri().toString().replace(fileUri.toString(), newURI.toString());
                        String newPath = file.getPath().replace(basePath, suffixedPath);

                        System.out.println("newPath = " + newPath);

                        logger.debug("Replacing old uri {} per {} and setting the status to {}", file.getUri().toString(),
                                newUri, File.FileStatus.PENDING_DELETE);

                        ObjectMap update = new ObjectMap()
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.PENDING_DELETE)
                                .append(FileDBAdaptor.QueryParams.URI.key(), newUri)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), newPath);
                        fileDBAdaptor.delete(file.getId(), update, new QueryOptions());
                    }
                } else {
                    // The uri in the disk has been changed but not in the database !!
                    throw new CatalogException("ERROR: Could not retrieve all the files and folders hanging from " + fileUri.toString());
                }

            } else if (options.getBoolean(FORCE_DELETE, false)) {
                // Physically delete all the files hanging from the folder
                Files.walkFileTree(filesystemPath, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                        try {
                            // Look for the file in catalog
                            Query query = new Query()
                                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                    .append(FileDBAdaptor.QueryParams.URI.key(), path.toUri().toString());

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

                            // 1. Set the file status to deleting
                            ObjectMap update = new ObjectMap()
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETING);
                            fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                            logger.debug("Deleting file '" + path.toString() + "' from filesystem and Catalog");

                            // 2. Delete the file from disk
                            ioManager.deleteFile(path.toUri());

                            // 3. Update the file status and path in the database. Set to delete
                            update = new ObjectMap()
                                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);

                            QueryResult<File> deleteQueryResult = fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                            if (deleteQueryResult == null || deleteQueryResult.getNumResults() != 1) {
                                // The file could not be removed from catalog. This should ONLY be happening when the file that
                                // has been removed from the filesystem was not registered in catalog.
                                logger.error("Internal error: The file {} could not be deleted from the database." + path.toString());
                            }

                            logger.debug("DELETE: {} successfully removed from the filesystem and catalog", path.toString());
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
                                    if (folderUri.endsWith("/")) {
                                        folderUri = folderUri.substring(0, folderUri.length() - 1);
                                    }
                                    Query query = new Query()
                                            .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                            .append(FileDBAdaptor.QueryParams.URI.key(), folderUri);

                                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, new QueryOptions());

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

                                    ObjectMap update = new ObjectMap()
                                            .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.DELETED);
//                                            .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), "");

                                    QueryResult<File> deleteQueryResult = fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                                    if (deleteQueryResult == null || deleteQueryResult.getNumResults() != 1) {
                                        // The file could not be removed from catalog. This should ONLY be happening when the file that
                                        // has been removed from the filesystem was not registered in catalog.
                                        logger.error("Internal error: The file {} could not be deleted from the database."
                                                + dir.toString());
                                    }

                                    logger.debug("REMOVE: {} successfully removed from the filesystem and catalog", dir.toString());
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
            removedFileResult = fileDBAdaptor.get(fileOrDirectory.getId(), new QueryOptions());
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
                authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_FILES);
            }
            return;
        }

        String stringPath = path.toString();
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
                authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.CREATE);
            }
            return;
        }

        URI completeURI = Paths.get(studyURI).resolve(path).toUri();

        // Create the folder in catalog
        File folder = new File(-1, path.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, completeURI,
                stringPath, TimeUtils.getTime(), TimeUtils.getTime(), "", new File.FileStatus(File.FileStatus.READY),
                false, 0, -1, Collections.emptyList(), -1, Collections.emptyList(), Collections.emptyList(), null, null, null);
        fileDBAdaptor.insert(folder, studyId, new QueryOptions());
    }

    public QueryResult<File> link(URI uriOrigin, String pathDestiny, long studyId, ObjectMap params, String sessionId)
            throws CatalogException, IOException {

        CatalogIOManager ioManager = catalogIOManagerFactory.get(uriOrigin);
        if (!ioManager.exists(uriOrigin)) {
            throw new CatalogIOException("File " + uriOrigin + " does not exist");
        }

        studyDBAdaptor.checkId(studyId);
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_FILES);

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
        if (Paths.get(uriOrigin).toFile().isDirectory()) {
            externalPathDestinyStr = Paths.get(pathDestiny).resolve(Paths.get(uriOrigin).getFileName()).toString() + "/";
        } else {
            externalPathDestinyStr = Paths.get(pathDestiny).resolve(Paths.get(uriOrigin).getFileName()).toString();
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
                .append(FileDBAdaptor.QueryParams.URI.key(), uriOrigin)
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!="
                        + File.FileStatus.REMOVED)
                .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);


        if (fileDBAdaptor.count(query).first() > 0) {
            // Create a regular expression on URI to return everything linked from that URI
            query.put(FileDBAdaptor.QueryParams.URI.key(), "~^" + uriOrigin);
            query.remove(FileDBAdaptor.QueryParams.PATH.key());

            // Limit the number of results and only some fields
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.LIMIT, 100)
                    .append(QueryOptions.INCLUDE, Arrays.asList(
                            FileDBAdaptor.QueryParams.ID.key(),
                            FileDBAdaptor.QueryParams.NAME.key(),
                            FileDBAdaptor.QueryParams.TYPE.key(),
                            FileDBAdaptor.QueryParams.PATH.key(),
                            FileDBAdaptor.QueryParams.URI.key(),
                            FileDBAdaptor.QueryParams.FORMAT.key(),
                            FileDBAdaptor.QueryParams.BIOFORMAT.key()
                    ));

            return fileDBAdaptor.get(query, queryOptions);
        }

        // Check if the uri was linked to other path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), uriOrigin)
                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!="
                        + File.FileStatus.REMOVED)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);
        if (fileDBAdaptor.count(query).first() > 0) {
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
            String path = fileDBAdaptor.get(query, queryOptions).first().getPath();
            throw new CatalogException(uriOrigin + " was already linked to catalog on a this other path " + path);
        }

        boolean parents = params.getBoolean("parents", false);
        // FIXME: Implement resync
        boolean resync  = params.getBoolean("resync", false);
        String description = params.getString("description", "");

        // Because pathDestiny can be null, we will use catalogPath as the virtual destiny where the files will be located in catalog.
        Path catalogPath = Paths.get(pathDestiny);

        if (pathDestiny.isEmpty()) {
            // If no destiny is given, everything will be linked to the root folder of the study.
            authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_FILES);
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
                authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.CREATE);
            }
        }

        Path pathOrigin = Paths.get(uriOrigin);
        Path externalPathDestiny = Paths.get(externalPathDestinyStr);
        if (Paths.get(uriOrigin).toFile().isFile()) {

            // Check if there is already a file in the same path
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr);

            // Create the file
            if (fileDBAdaptor.count(query).first() == 0) {
                long diskUsage = Files.size(Paths.get(uriOrigin));

                File subfile = new File(-1, externalPathDestiny.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                        File.Bioformat.NONE, uriOrigin, externalPathDestinyStr, TimeUtils.getTime(), TimeUtils.getTime(), description,
                        new File.FileStatus(File.FileStatus.READY), true, diskUsage, -1, Collections.emptyList(), -1,
                        Collections.emptyList(), Collections.emptyList(), null, Collections.emptyMap(), Collections.emptyMap());
                QueryResult<File> queryResult = fileDBAdaptor.insert(subfile, studyId, new QueryOptions());
                File file = fileMetadataReader.setMetadataInformation(queryResult.first(), queryResult.first().getUri(),
                        new QueryOptions(), sessionId, false);
                queryResult.setResult(Arrays.asList(file));

                // If it is an avro file, we will try to link it with the correspondent original file
                try {
                    if (File.Format.AVRO.equals(file.getFormat())) {
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
            // This list will contain the list of avro files detected during the link
            List<File> avroFiles = new ArrayList<>();

            // We remove the / at the end for replacement purposes in the walkFileTree
            String finalExternalPathDestinyStr = externalPathDestinyStr.substring(0, externalPathDestinyStr.length() - 1);

            // Link all the files and folders present in the uri
            Files.walkFileTree(pathOrigin, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {

                    try {
                        String destinyPath = dir.toString().replace(Paths.get(uriOrigin).toString(), finalExternalPathDestinyStr);

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
                            File folder = new File(-1, dir.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN,
                                    File.Bioformat.NONE, dir.toUri(), destinyPath, TimeUtils.getTime(), TimeUtils.getTime(),
                                    description, new File.FileStatus(File.FileStatus.READY), true, 0, -1,
                                    Collections.emptyList(), -1, Collections.emptyList(), Collections.emptyList(), null,
                                    Collections.emptyMap(), Collections.emptyMap());
                            fileDBAdaptor.insert(folder, studyId, new QueryOptions());
                        }

                    } catch (CatalogDBException e) {
                        logger.error("An error occurred when trying to create folder {}", dir.toString());
//                        e.printStackTrace();
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) throws IOException {
                    try {
                        String destinyPath = filePath.toString().replace(Paths.get(uriOrigin).toString(), finalExternalPathDestinyStr);

                        if (destinyPath.startsWith("/")) {
                            destinyPath = destinyPath.substring(1);
                        }

                        Query query = new Query()
                                .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                        if (fileDBAdaptor.count(query).first() == 0) {
                            long diskUsage = Files.size(filePath);
                            // If the file does not exist, we create it
                            File subfile = new File(-1, filePath.getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                                    File.Bioformat.NONE, filePath.toUri(), destinyPath, TimeUtils.getTime(), TimeUtils.getTime(),
                                    description, new File.FileStatus(File.FileStatus.READY), true, diskUsage, -1, Collections.emptyList(),
                                    -1, Collections.emptyList(), Collections.emptyList(), null, Collections.emptyMap(),
                                    Collections.emptyMap());
                            QueryResult<File> queryResult = fileDBAdaptor.insert(subfile, studyId, new QueryOptions());
                            File file = fileMetadataReader.setMetadataInformation(queryResult.first(), queryResult.first().getUri(),
                                    new QueryOptions(), sessionId, false);
                            if (File.Format.AVRO.equals(file.getFormat())) {
                                logger.info("Detected avro file {}", file.getPath());
                                avroFiles.add(file);
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
                    // We update the diskUsage of the folder
                    // TODO: Check this. Maybe we should not be doing this here.
//                    String destinyPath = dir.toString().replace(Paths.get(uriOrigin).toString(), catalogPath.toString());
//
//                    if (destinyPath.startsWith("/")) {
//                        destinyPath = destinyPath.substring(1, destinyPath.length());
//                    }
//
//                    if (!destinyPath.isEmpty()) {
//                        destinyPath += "/";
//                    }
//
//                    long diskUsage = Files.size(Paths.get(destinyPath));
//
//                    Query query = new Query()
//                            .append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
//                            .append(CatalogFileDBAdaptor.QueryParams.PATH.key(), destinyPath);
//
//                    ObjectMap objectMap = new ObjectMap(CatalogFileDBAdaptor.QueryParams.DISK_USAGE.key(), diskUsage);
//
//                    try {
//                        fileDBAdaptor.update(query, objectMap);
//                    } catch (CatalogDBException e) {
//                        logger.error("Link: There was an error when trying to update the diskUsage of the folder");
//                        e.printStackTrace();
//                    }

                    return FileVisitResult.CONTINUE;
                }
            });

            // Try to link avro files with their corresponding original files if any
            try {
                if (avroFiles.size() > 0) {
                    matchUpVariantFiles(avroFiles, sessionId);
                }
            } catch (CatalogException e) {
                logger.warn("Matching avro to variant file: {}", e.getMessage());
            }

            // Check if the uri was already linked to that same path
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.URI.key(), "~^" + uriOrigin)
                    .append(FileDBAdaptor.QueryParams.STUDY_ID.key(), studyId)
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), "!=" + Status.TRASHED + ";!=" + Status.DELETED + ";!="
                            + File.FileStatus.REMOVED)
                    .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);

            // Limit the number of results and only some fields
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.LIMIT, 100)
                    .append(QueryOptions.INCLUDE, Arrays.asList(
                            FileDBAdaptor.QueryParams.ID.key(),
                            FileDBAdaptor.QueryParams.NAME.key(),
                            FileDBAdaptor.QueryParams.TYPE.key(),
                            FileDBAdaptor.QueryParams.PATH.key(),
                            FileDBAdaptor.QueryParams.URI.key(),
                            FileDBAdaptor.QueryParams.FORMAT.key(),
                            FileDBAdaptor.QueryParams.BIOFORMAT.key()
                    ));

            return fileDBAdaptor.get(query, queryOptions);
        }

    }

    public QueryResult<File> unlink(String fileIdStr, QueryOptions options, String sessionId) throws CatalogException, IOException {
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        // FIXME use userManager instead of userDBAdaptor
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        // Check 1. No comma-separated values are valid, only one single File or Directory can be deleted.
        long fileId = getId(userId, fileIdStr);
        fileDBAdaptor.checkId(fileId);
        long studyId = getStudyId(fileId);

        // Check 2. User has the proper permissions to delete the file.
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DELETE);

        // Check if we can obtain the file from the dbAdaptor properly.
        QueryResult<File> fileQueryResult = fileDBAdaptor.get(fileId, options);
        if (fileQueryResult == null || fileQueryResult.getNumResults() != 1) {
            throw new CatalogException("Cannot delete file '" + fileIdStr + "'. There was an error with the database.");
        }

        File file = fileQueryResult.first();

        // Check 3.
        if (!file.isExternal()) {
            throw new CatalogException("Only previously linked files can be unlinked. Please, use delete instead.");
        }

        String suffixName = ".REMOVED_" + TimeUtils.getTime();
        String basePath = Paths.get(file.getPath()).toString();
        String suffixedPath = basePath + suffixName;
        if (file.getType().equals(File.Type.FILE)) {
            logger.debug("Unlinking file {}", file.getUri().toString());

            ObjectMap update = new ObjectMap()
                    .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), suffixedPath);

            return fileDBAdaptor.delete(file.getId(), update, new QueryOptions());
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

                        ObjectMap update = new ObjectMap()
                                .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                                .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath().replaceFirst(basePath, suffixedPath));

                        fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                        logger.debug("{} unlinked", file.toString());
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

                                ObjectMap update = new ObjectMap()
                                        .append(FileDBAdaptor.QueryParams.STATUS_NAME.key(), File.FileStatus.REMOVED)
                                        .append(FileDBAdaptor.QueryParams.PATH.key(),
                                                file.getPath().replaceFirst(basePath, suffixedPath));

                                fileDBAdaptor.delete(file.getId(), update, new QueryOptions());

                                logger.debug("{} unlinked", dir.toString());
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

    @Deprecated
    public QueryResult<File> unlink(long fileId, String sessionId) throws CatalogException {
        return null;
//        fileDBAdaptor.checkFileId(fileId);
//
//        QueryResult<File> fileQueryResult = fileDBAdaptor.getFile(fileId, new QueryOptions());
//
//        if (fileQueryResult == null || fileQueryResult.getNumResults() == 0) {
//            throw new CatalogException("Internal error: Cannot find " + fileId);
//        }
//
//        File file = fileQueryResult.first();
//
//        if (isRootFolder(file)) {
//            throw new CatalogException("Can not delete root folder");
//        }
//
//        if (!file.isExternal()) {
//            throw new CatalogException("Cannot unlink a file that has not been linked.");
//        }
//
//        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
//        authorizationManager.checkFilePermission(fileId, userId, FileAcl.FilePermissions.DELETE);
//
//        List<File> filesToDelete;
//        if (file.getType().equals(File.Type.DIRECTORY)) {
//            filesToDelete = fileDBAdaptor.get(
//                    new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath()),
//                    new QueryOptions("include", "projects.studies.files.id")).getResult();
//        } else {
//            filesToDelete = Collections.singletonList(file);
//        }
//
//        for (File f : filesToDelete) {
//            fileDBAdaptor.delete(f.getId(), new QueryOptions());
//        }
//
//        return queryResult;
    }

    @Override
    public QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
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
    public QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.groupBy(query, field, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Override
    public QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        ParamUtils.checkObj(fields, "fields");
        ParamUtils.checkObj(studyId, "studyId");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
//        query.append(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key(), studyId);
        QueryResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.groupBy(query, fields, options);
        }

        return ParamUtils.defaultObject(queryResult, QueryResult::new);
    }

    @Deprecated
    private QueryResult<File> checkCanDeleteFile(File file, String userId) throws CatalogException {
        authorizationManager.checkFilePermission(file.getId(), userId, FileAclEntry.FilePermissions.DELETE);

        switch (file.getStatus().getName()) {
            case File.FileStatus.TRASHED:
                //Send warning message
                String warningMsg = "File already deleted. {id: " + file.getId() + ", status: '" + file.getStatus() + "'}";
                logger.warn(warningMsg);
                return new QueryResult<File>("Delete file", 0, 0, 0,
                        warningMsg,
                        null, Collections.emptyList());
            case File.FileStatus.READY:
                break;
            case File.FileStatus.STAGE:
            case File.FileStatus.MISSING:
            default:
                throw new CatalogException("File is not ready. {"
                        + "id: " + file.getId() + ", "
                        + "path:\"" + file.getPath() + "\","
                        + "status: '" + file.getStatus().getName() + "'}");
        }
        return null;
    }

    @Override
    public QueryResult<File> rename(long fileId, String newName, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkFileName(newName, "name");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        long studyId = fileDBAdaptor.getStudyIdByFileId(fileId);
        long projectId = studyDBAdaptor.getProjectIdByStudyId(studyId);
        String ownerId = projectDBAdaptor.getOwnerId(projectId);

        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.UPDATE);
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
//        String userId = catalogDBAdaptor.getUserIdBySessionId(sessionId);
//        int studyId = catalogDBAdaptor.getStudyIdByFileId(fileId);
//        int projectId = catalogDBAdaptor.getProjectIdByStudyId(studyId);
//        String ownerId = catalogDBAdaptor.getProjectOwnerId(projectId);
//
//        if (!authorizationManager.getFileACL(userId, fileId).isWrite()) {
//            throw new CatalogManagerException("Permission denied. User can't rename this file");
//        }
//        QueryResult<File> fileResult = catalogDBAdaptor.getFile(fileId);
//        if (fileResult.getResult().isEmpty()) {
//            return new QueryResult("Delete file", 0, 0, 0, "File not found", null, null);
//        }
//        File file = fileResult.getResult().get(0);
    }

    @Override
    public QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files,
                                              Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        ParamUtils.checkParameter(name, "name");
        ParamUtils.checkObj(files, "files");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        description = ParamUtils.defaultString(description, "");
        attributes = ParamUtils.defaultObject(attributes, HashMap<String, Object>::new);

        authorizationManager.checkStudyPermission(studyId, userId, StudyAclEntry.StudyPermissions.CREATE_DATASETS);

        for (Long fileId : files) {
            if (fileDBAdaptor.getStudyIdByFileId(fileId) != studyId) {
                throw new CatalogException("Can't create a dataset with files from different studies.");
            }
            authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.VIEW);
        }

        Dataset dataset = new Dataset(-1, name, TimeUtils.getTime(), description, files, new Status(), attributes);
        QueryResult<Dataset> queryResult = datasetDBAdaptor.insert(dataset, studyId, options);
//        auditManager.recordCreation(AuditRecord.Resource.dataset, queryResult.first().getId(), userId, queryResult.first(), null, null);
        auditManager.recordAction(AuditRecord.Resource.dataset, AuditRecord.Action.create, AuditRecord.Magnitude.low,
                queryResult.first().getId(), userId, null, queryResult.first(), null, null);
        return queryResult;
    }

    @Override
    public QueryResult<Dataset> readDataset(long dataSetId, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);

        QueryResult<Dataset> queryResult = datasetDBAdaptor.get(dataSetId, options);

        for (Long fileId : queryResult.first().getFiles()) {
            authorizationManager.checkDatasetPermission(fileId, userId, DatasetAclEntry.DatasetPermissions.VIEW);
        }

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
    public QueryResult<DatasetAclEntry> getDatasetAcls(String datasetStr, List<String> members, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long datasetId = getDatasetId(userId, datasetStr);
        authorizationManager.checkDatasetPermission(datasetId, userId, DatasetAclEntry.DatasetPermissions.SHARE);
        Long studyId = getStudyIdByDataset(datasetId);

        // Split and obtain the set of members (users + groups), users and groups
        Set<String> memberSet = new HashSet<>();
        Set<String> userIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();

        for (String member: members) {
            memberSet.add(member);
            if (!member.startsWith("@")) {
                userIds.add(member);
            } else {
                groupIds.add(member);
            }
        }


        // Obtain the groups the user might belong to in order to be able to get the permissions properly
        // (the permissions might be given to the group instead of the user)
        // Map of group -> users
        Map<String, List<String>> groupUsers = new HashMap<>();

        if (userIds.size() > 0) {
            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
            // We add the groups where the users might belong to to the memberSet
            if (groups.getNumResults() > 0) {
                for (Group group : groups.getResult()) {
                    for (String tmpUserId : group.getUserIds()) {
                        if (userIds.contains(tmpUserId)) {
                            memberSet.add(group.getName());

                            if (!groupUsers.containsKey(group.getName())) {
                                groupUsers.put(group.getName(), new ArrayList<>());
                            }
                            groupUsers.get(group.getName()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<DatasetAclEntry> datasetAclQueryResult = datasetDBAdaptor.getAcl(datasetId, memberList);

        if (members.size() == 0) {
            return datasetAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one sampleAcl per member
        Map<String, DatasetAclEntry> datasetAclHashMap = new HashMap<>();
        for (DatasetAclEntry datasetAcl : datasetAclQueryResult.getResult()) {
            if (memberList.contains(datasetAcl.getMember())) {
                if (datasetAcl.getMember().startsWith("@")) {
                    // Check if the user was demanding the group directly or a user belonging to the group
                    if (groupIds.contains(datasetAcl.getMember())) {
                        datasetAclHashMap.put(datasetAcl.getMember(),
                                new DatasetAclEntry(datasetAcl.getMember(), datasetAcl.getPermissions()));
                    } else {
                        // Obtain the user(s) belonging to that group whose permissions wanted the userId
                        if (groupUsers.containsKey(datasetAcl.getMember())) {
                            for (String tmpUserId : groupUsers.get(datasetAcl.getMember())) {
                                if (userIds.contains(tmpUserId)) {
                                    datasetAclHashMap.put(tmpUserId, new DatasetAclEntry(tmpUserId, datasetAcl.getPermissions()));
                                }
                            }
                        }
                    }
                } else {
                    // Add the user
                    datasetAclHashMap.put(datasetAcl.getMember(), new DatasetAclEntry(datasetAcl.getMember(), datasetAcl.getPermissions()));
                }
            }
        }

        // We recreate the output that is in DatasetAclHashMap but in the same order the members were queried.
        List<DatasetAclEntry> datasetAclList = new ArrayList<>(datasetAclHashMap.size());
        for (String member : members) {
            if (datasetAclHashMap.containsKey(member)) {
                datasetAclList.add(datasetAclHashMap.get(member));
            }
        }

        // Update queryResult info
        datasetAclQueryResult.setId(datasetStr);
        datasetAclQueryResult.setNumResults(datasetAclList.size());
        datasetAclQueryResult.setNumTotalResults(datasetAclList.size());
        datasetAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        datasetAclQueryResult.setResult(datasetAclList);

        return datasetAclQueryResult;
    }

    @Override
    public DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.VIEW);

        URI fileUri = getUri(get(fileId, null, sessionId).first());
        boolean ignoreCase = options.getBoolean("ignoreCase");
        boolean multi = options.getBoolean("multi");
        return catalogIOManagerFactory.get(fileUri).getGrepFileObject(fileUri, pattern, ignoreCase, multi);
    }

    @Override
    public DataInputStream download(long fileId, int start, int limit, QueryOptions options, String sessionId) throws CatalogException {
        ParamUtils.checkParameter(sessionId, "sessionId");
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.DOWNLOAD);

        URI fileUri = getUri(get(fileId, null, sessionId).first());

        return catalogIOManagerFactory.get(fileUri).getFileObject(fileUri, start, limit);
    }

    @Override
    public DataInputStream head(long fileId, int lines, QueryOptions options, String sessionId) throws CatalogException {
        return download(fileId, 0, lines, options, sessionId);
    }

    @Override
    public QueryResult index(String fileIdStr, String type, Map<String, String> params, String sessionId) throws CatalogException {
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        List<Long> fileFolderIdList = getIds(userId, fileIdStr);

        long studyId = -1;
        // Check we can index all of them
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

        String outDirPath = params.get("outdir");
        if (outDirPath != null && !StringUtils.isNumeric(outDirPath) && outDirPath.contains("/") && !outDirPath.endsWith("/")) {
            outDirPath = outDirPath + "/";
        }
        long outDirId = getId(userId, outDirPath);
        if (outDirId > 0) {
            authorizationManager.checkFilePermission(outDirId, userId, FileAclEntry.FilePermissions.CREATE);
            if (fileDBAdaptor.getStudyIdByFileId(outDirId) != studyId) {
                throw new CatalogException("The output directory does not correspond to the same study of the files");
            }
        } else if (outDirPath != null) {
            ObjectMap parsedSampleStr = parseFeatureId(userId, outDirPath);
            String path = (String) parsedSampleStr.get("featureName");
            logger.info("Outdir {}", path);
            if (path.contains("/")) {
                if (!path.endsWith("/")) {
                    path = path + "/";
                }
                // It is a path, so we will try to create the folder
                createFolder(studyId, path, new File.FileStatus(), true, "", new QueryOptions(), sessionId);
                outDirId = getId(userId, path);
                logger.info("Outdir {} -> {}", outDirId, path);
            }
        } else {
            if (fileFolderIdList.size() == 1) {
                // Leave the output files in the same directory
                long fileId = fileFolderIdList.get(0);
                QueryResult<File> file = fileDBAdaptor.get(fileId, new QueryOptions());
                if (file.getNumResults() == 1) {
                    if (file.first().getType().equals(File.Type.DIRECTORY)) {
                        outDirId = fileId;
                    } else {
                        outDirId = getParent(fileId, new QueryOptions(), sessionId).first().getId();
                    }
                }
            } else {
                // Leave the output files in the root directory
                outDirId = getId(userId, studyId + ":/");
                logger.info("Getting out dir from {}:/", studyId);
            }
        }

        QueryResult<Job> jobQueryResult = null;
        if (type.equals("VCF")) {
            List<Long> fileIdList = new ArrayList<>();

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
                    // Retrieve all the VCF files that can be found within the directory
                    String path = file.first().getPath().endsWith("/") ? file.first().getPath() : file.first().getPath() + "/";
                    Query query = new Query(FileDBAdaptor.QueryParams.FORMAT.key(), Arrays.asList(File.Format.VCF, File.Format.GVCF))
                            .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + path + "*");
                    QueryResult<File> fileQueryResult = fileDBAdaptor.get(query, queryOptions);

                    if (fileQueryResult.getNumResults() == 0) {
                        throw new CatalogException("No VCF files could be found in directory " + file.first().getPath());
                    }

                    for (File fileTmp : fileQueryResult.getResult()) {
                        authorizationManager.checkFilePermission(fileTmp.getId(), userId, FileAclEntry.FilePermissions.VIEW);
                        authorizationManager.checkFilePermission(fileTmp.getId(), userId, FileAclEntry.FilePermissions.UPDATE);

                        fileIdList.add(fileTmp.getId());
                    }

                } else {
                    if (!File.Format.VCF.equals(file.first().getFormat()) && !File.Format.GVCF.equals(file.first().getFormat())) {
                        throw new CatalogException("The file " + file.first().getName() + " is not a VCF file.");
                    }

                    authorizationManager.checkFilePermission(file.first().getId(), userId, FileAclEntry.FilePermissions.VIEW);
                    authorizationManager.checkFilePermission(file.first().getId(), userId, FileAclEntry.FilePermissions.UPDATE);

                    fileIdList.add(file.first().getId());
                }
            }

            if (fileIdList.size() == 0) {
                throw new CatalogException("Cannot send to index. No files could be found to be indexed.");
            }

            String fileIds = StringUtils.join(fileIdList, ",");
            params.put("file-id", fileIds);
            params.put("outdir", Long.toString(outDirId));
            params.put("sid", sessionId);
            List<Long> outputList = outDirId > 0 ? Arrays.asList(outDirId) : Collections.emptyList();
            Map<String, Object> attributes = new HashMap<>();
            attributes.put(IndexDaemon.INDEX_TYPE, IndexDaemon.VARIANT_TYPE);
            jobQueryResult = catalogManager.getJobManager().queue(studyId, "VariantIndex", "opencga-analysis.sh",
                    Job.Type.INDEX, params, fileIdList, outputList, outDirId, userId, attributes);
            jobQueryResult.first().setToolName("variantIndex");
        } else if (type.equals("BAM")) {
            logger.debug("Index bam files to do");
            jobQueryResult = new QueryResult<>();
        }

        return jobQueryResult;
    }

//    private void indexVariants(File file, long outDirId, ObjectMap params) throws CatalogException {
//
//        long studyId = fileDBAdaptor.getStudyIdByFileId(file.getId());
//
//        if (outDirId > 0) {
//            // Check that the input file and the output file corresponds to the same study
//            long studyIdByFileId = fileDBAdaptor.getStudyIdByFileId(outDirId);
//            if (studyId != studyIdByFileId) {
//                throw new CatalogException("The study of the file to be indexed is different from the study where the index file will be "
//                        + "saved.");
//            }
//        } else {
//            // Obtain the id of the directory where the file is stored to store the indexed files
//            Path parent = Paths.get(file.getPath()).getParent();
//            String parentPath;
//            if (parent == null) {
//                parentPath = "";
//            } else {
//                parentPath = parent.toString().endsWith("/") ? parent.toString() : parent.toString() + "/";
//            }
//            Query query = new Query(CatalogFileDBAdaptor.QueryParams.PATH.key(), "~^" + parentPath);
//            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, CatalogFileDBAdaptor.QueryParams.ID.key());
//            QueryResult<File> outputFileDir = fileDBAdaptor.get(query, queryOptions);
//
//            if (outputFileDir.getNumResults() != 1) {
//                logger.error("Could not obtain the id for the path {} for the file {} with id {}", parentPath, file.getPath(),
//                        file.getId());
//                throw new CatalogException("Internal error: Could not obtain the path to store indexed file");
//            }
//
//            outDirId = outputFileDir.first().getId();
//        }
//
//
//
//
//
//    }

    @Override
    public QueryResult<FileAclEntry> getAcls(String fileStr, List<String> members, String sessionId) throws CatalogException {
        long startTime = System.currentTimeMillis();
        String userId = userDBAdaptor.getUserIdBySessionId(sessionId);
        Long fileId = getId(userId, fileStr);
        authorizationManager.checkFilePermission(fileId, userId, FileAclEntry.FilePermissions.SHARE);
        Long studyId = getStudyId(fileId);

        // Split and obtain the set of members (users + groups), users and groups
        Set<String> memberSet = new HashSet<>();
        Set<String> userIds = new HashSet<>();
        Set<String> groupIds = new HashSet<>();

        for (String member: members) {
            memberSet.add(member);
            if (!member.startsWith("@")) {
                userIds.add(member);
            } else {
                groupIds.add(member);
            }
        }


        // Obtain the groups the user might belong to in order to be able to get the permissions properly
        // (the permissions might be given to the group instead of the user)
        // Map of group -> users
        Map<String, List<String>> groupUsers = new HashMap<>();

        if (userIds.size() > 0) {
            List<String> tmpUserIds = userIds.stream().collect(Collectors.toList());
            QueryResult<Group> groups = studyDBAdaptor.getGroup(studyId, null, tmpUserIds);
            // We add the groups where the users might belong to to the memberSet
            if (groups.getNumResults() > 0) {
                for (Group group : groups.getResult()) {
                    for (String tmpUserId : group.getUserIds()) {
                        if (userIds.contains(tmpUserId)) {
                            memberSet.add(group.getName());

                            if (!groupUsers.containsKey(group.getName())) {
                                groupUsers.put(group.getName(), new ArrayList<>());
                            }
                            groupUsers.get(group.getName()).add(tmpUserId);
                        }
                    }
                }
            }
        }
        List<String> memberList = memberSet.stream().collect(Collectors.toList());
        QueryResult<FileAclEntry> fileAclQueryResult = fileDBAdaptor.getAcl(fileId, memberList);

        if (members.size() == 0) {
            return fileAclQueryResult;
        }

        // For the cases where the permissions were given at group level, we obtain the user and return it as if they were given to the user
        // instead of the group.
        // We loop over the results and recreate one fileAcl per member
        Map<String, FileAclEntry> fileAclHashMap = new HashMap<>();
        for (FileAclEntry fileAcl : fileAclQueryResult.getResult()) {
            if (memberList.contains(fileAcl.getMember())) {
                if (fileAcl.getMember().startsWith("@")) {
                    // Check if the user was demanding the group directly or a user belonging to the group
                    if (groupIds.contains(fileAcl.getMember())) {
                        fileAclHashMap.put(fileAcl.getMember(), new FileAclEntry(fileAcl.getMember(), fileAcl.getPermissions()));
                    } else {
                        // Obtain the user(s) belonging to that group whose permissions wanted the userId
                        if (groupUsers.containsKey(fileAcl.getMember())) {
                            for (String tmpUserId : groupUsers.get(fileAcl.getMember())) {
                                if (userIds.contains(tmpUserId)) {
                                    fileAclHashMap.put(tmpUserId, new FileAclEntry(tmpUserId, fileAcl.getPermissions()));
                                }
                            }
                        }
                    }
                } else {
                    // Add the user
                    fileAclHashMap.put(fileAcl.getMember(), new FileAclEntry(fileAcl.getMember(), fileAcl.getPermissions()));
                }
            }
        }

        // We recreate the output that is in fileAclHashMap but in the same order the members were queried.
        List<FileAclEntry> fileAclList = new ArrayList<>(fileAclHashMap.size());
        for (String member : members) {
            if (fileAclHashMap.containsKey(member)) {
                fileAclList.add(fileAclHashMap.get(member));
            }
        }

        // Update queryResult info
        fileAclQueryResult.setId(fileStr);
        fileAclQueryResult.setNumResults(fileAclList.size());
        fileAclQueryResult.setNumTotalResults(fileAclList.size());
        fileAclQueryResult.setDbTime((int) (System.currentTimeMillis() - startTime));
        fileAclQueryResult.setResult(fileAclList);

        return fileAclQueryResult;
    }

//    private File.Type getType(URI uri, boolean exists) throws CatalogException {
//        ParamsUtils.checkObj(uri, "uri");
//        return uri.getPath().endsWith("/") ? File.Type.DIRECTORY : File.Type.FILE;
//    }

//    private File.Bioformat setBioformat(File file, String sessionId) throws CatalogException {
//        ParamsUtils.checkObj(file, "file");
//
//
//        File.Bioformat bioformat = null;
//        ObjectMap parameters = new ObjectMap();
//        for (Map.Entry<File.Bioformat, Pattern> entry : bioformatMap.entrySet()) {
//            if (entry.getValue().matcher(file.getPath()).matches()) {
//                bioformat = entry.getKey();
//                break;
//            }
//        }
//
//        if (bioformat == File.Bioformat.VARIANT) {
//
//        }
//
//
//        update(file.getId(), parameters, new QueryOptions(), sessionId);
//
//        return File.Bioformat.NONE;
//    }

}
