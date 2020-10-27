/*
 * Copyright 2015-2020 OpenCB
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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.core.result.Error;
import org.opencb.commons.utils.CollectionUtils;
import org.opencb.commons.utils.FileUtils;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.MongoDBAdaptorFactory;
import org.opencb.opencga.catalog.exceptions.*;
import org.opencb.opencga.catalog.io.IOManager;
import org.opencb.opencga.catalog.io.IOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetDataResult;
import org.opencb.opencga.catalog.stats.solr.CatalogSolrManager;
import org.opencb.opencga.catalog.utils.*;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.HookConfiguration;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.CustomStatus;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.auth.authorization.CatalogAuthorizationManager.checkPermissions;
import static org.opencb.opencga.catalog.utils.FileMetadataReader.FILE_VARIANT_STATS_VARIABLE_SET;
import static org.opencb.opencga.core.common.JacksonUtils.getDefaultObjectMapper;
import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class FileManager extends AnnotationSetManager<File> {

    private static final QueryOptions INCLUDE_STUDY_URI;
    public static final QueryOptions INCLUDE_FILE_IDS;
    public static final QueryOptions INCLUDE_FILE_URI;
    public static final QueryOptions INCLUDE_FILE_URI_PATH;
    public  static final QueryOptions EXCLUDE_FILE_ATTRIBUTES;
    private static final Comparator<File> ROOT_FIRST_COMPARATOR;
    private static final Comparator<File> ROOT_LAST_COMPARATOR;

    protected static Logger logger;
    private FileMetadataReader fileMetadataReader;
    private UserManager userManager;
    private StudyManager studyManager;
    private IOManagerFactory ioManagerFactory;

    private final String defaultFacet = "creationYear>>creationMonth;format;bioformat;format>>bioformat;status;"
            + "size[0..214748364800]:10737418240;numSamples[0..10]:1";

    static {
        INCLUDE_FILE_IDS = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.UID.key(), FileDBAdaptor.QueryParams.UUID.key(),
                FileDBAdaptor.QueryParams.STUDY_UID.key(), FileDBAdaptor.QueryParams.TYPE.key()));
        INCLUDE_FILE_URI = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.UID.key(), FileDBAdaptor.QueryParams.UUID.key(),
                FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.STUDY_UID.key(), FileDBAdaptor.QueryParams.TYPE.key()));
        INCLUDE_FILE_URI_PATH = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.UID.key(), FileDBAdaptor.QueryParams.UUID.key(),
                FileDBAdaptor.QueryParams.INTERNAL_STATUS.key(), FileDBAdaptor.QueryParams.FORMAT.key(),
                FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.EXTERNAL.key(),
                FileDBAdaptor.QueryParams.STUDY_UID.key(), FileDBAdaptor.QueryParams.TYPE.key()));
        EXCLUDE_FILE_ATTRIBUTES = new QueryOptions(QueryOptions.EXCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.ATTRIBUTES.key(),
                FileDBAdaptor.QueryParams.ANNOTATION_SETS.key(), FileDBAdaptor.QueryParams.STATS.key()));
        INCLUDE_STUDY_URI = new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.URI.key());

        ROOT_FIRST_COMPARATOR = (f1, f2) -> (f1.getPath() == null ? 0 : f1.getPath().length())
                - (f2.getPath() == null ? 0 : f2.getPath().length());
        ROOT_LAST_COMPARATOR = (f1, f2) -> (f2.getPath() == null ? 0 : f2.getPath().length())
                - (f1.getPath() == null ? 0 : f1.getPath().length());

        logger = LoggerFactory.getLogger(FileManager.class);
    }

    FileManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                DBAdaptorFactory catalogDBAdaptorFactory, IOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, configuration);

        this.userManager = catalogManager.getUserManager();
        this.studyManager = catalogManager.getStudyManager();
        this.ioManagerFactory = ioManagerFactory;
        this.fileMetadataReader = new FileMetadataReader(this.catalogManager);
    }

    @Override
    Enums.Resource getEntity() {
        return Enums.Resource.FILE;
    }

    @Override
    OpenCGAResult<File> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user) throws CatalogException {
        // We make this comparison because in File, the absence of a fileName means the user is actually looking for the / directory
        if (StringUtils.isNotEmpty(entry) || entry == null) {
            ParamUtils.checkIsSingleID(entry);
        }
        return internalGet(studyUid, Collections.singletonList(entry), query, options, user, false);
    }

//    @Override
//    OpenCGAResult<File> internalGet(long studyUid, String fileName, @Nullable Query query, QueryOptions options, String user)
//            throws CatalogException {
//        // We make this comparison because in File, the absence of a fileName means the user is actually looking for the / directory
//        if (StringUtils.isNotEmpty(fileName) || fileName == null) {
//            ParamUtils.checkIsSingleID(fileName);
//        }
//
//        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
//
//        FileDBAdaptor.QueryParams queryParam = FileDBAdaptor.QueryParams.PATH;
//        if (UuidUtils.isOpenCgaUuid(fileName)) {
//            queryParam = FileDBAdaptor.QueryParams.UUID;
//        } else {
//            fileName = fileName.replace(":", "/");
//            if (fileName.startsWith("/")) {
//                fileName = fileName.substring(1);
//            }
//        }
//
//        // We search the file
//        Query queryCopy = query == null ? new Query() : new Query(query);
//        queryCopy.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
//                .append(queryParam.key(), fileName);
//        OpenCGAResult<File> pathDataResult = fileDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
//        if (pathDataResult.getNumResults() > 1) {
//            throw new CatalogException("Error: More than one file id found based on " + fileName);
//        } else if (pathDataResult.getNumResults() == 1) {
//            return pathDataResult;
//        }
//
//        if (queryParam == FileDBAdaptor.QueryParams.PATH && !fileName.contains("/")) {
//            queryParam = FileDBAdaptor.QueryParams.NAME;
//
//            // We search as a fileName as well
//            queryCopy = query == null ? new Query() : new Query(query);
//            queryCopy.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
//                    .append(FileDBAdaptor.QueryParams.NAME.key(), fileName);
//            OpenCGAResult<File> nameDataResult = fileDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
//            if (nameDataResult.getNumResults() > 1) {
//                throw new CatalogException("Error: More than one file id found based on " + fileName);
//            } else if (nameDataResult.getNumResults() == 1) {
//                return nameDataResult;
//            }
//        }
//
//        // The file could not be found or the user does not have permissions to see it
//        // Check if the file can be found without adding the user restriction
//        OpenCGAResult<File> resultsNoCheck = fileDBAdaptor.get(queryCopy, queryOptions);
//        if (resultsNoCheck.getNumResults() == 1) {
//            throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the file " + fileName);
//        }
//        if (queryParam == FileDBAdaptor.QueryParams.NAME) {
//            // The last search was performed by name but we can also search by path just in case
//            queryCopy.put(FileDBAdaptor.QueryParams.PATH.key(), fileName);
//            resultsNoCheck = fileDBAdaptor.get(queryCopy, queryOptions);
//            if (resultsNoCheck.getNumResults() == 1) {
//                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see the file " + fileName);
//            }
//        }
//
//        throw new CatalogException("File " + fileName + " not found");
//    }

    @Override
    InternalGetDataResult<File> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                            String user, boolean ignoreException) throws CatalogException {
        if (ListUtils.isEmpty(entryList)) {
            throw new CatalogException("Missing file entries.");
        }
        List<String> uniqueList = ListUtils.unique(entryList);

        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        Query queryCopy = query == null ? new Query() : new Query(query);
        queryCopy.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);

        Function<File, String> fileStringFunction = File::getPath;
        boolean canBeSearchedAsName = true;
        List<String> correctedFileList = new ArrayList<>(uniqueList.size());
        FileDBAdaptor.QueryParams idQueryParam = null;
        for (String entry : uniqueList) {
            FileDBAdaptor.QueryParams param = FileDBAdaptor.QueryParams.PATH;
            if (UuidUtils.isOpenCgaUuid(entry)) {
                correctedFileList.add(entry);
                param = FileDBAdaptor.QueryParams.UUID;
                fileStringFunction = File::getUuid;
            } else {
                String fileName = entry.replace(":", "/");
                if (fileName.startsWith("/")) {
                    // Remove the starting /. Absolute paths are not supported.
                    fileName = fileName.substring(1);
                }
                correctedFileList.add(fileName);

                if (fileName.contains("/")) {
                    canBeSearchedAsName = false;
                }
            }
            if (idQueryParam == null) {
                idQueryParam = param;
            }
            if (idQueryParam != param) {
                throw new CatalogException("Found uuids and paths in the same query. Please, choose one or do two different queries.");
            }
        }
        queryCopy.put(idQueryParam.key(), correctedFileList);

        // Ensure the field by which we are querying for will be kept in the results
        queryOptions = keepFieldInQueryOptions(queryOptions, idQueryParam.key());

        OpenCGAResult<File> fileDataResult = fileDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
        if (fileDataResult.getNumResults() != correctedFileList.size() && idQueryParam == FileDBAdaptor.QueryParams.PATH
                && canBeSearchedAsName) {
            // We also search by name
            queryCopy = query == null ? new Query() : new Query(query);
            queryCopy.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(FileDBAdaptor.QueryParams.NAME.key(), correctedFileList);

            // Ensure the field by which we are querying for will be kept in the results
            queryOptions = keepFieldInQueryOptions(queryOptions, FileDBAdaptor.QueryParams.NAME.key());

            OpenCGAResult<File> nameDataResult = fileDBAdaptor.get(studyUid, queryCopy, queryOptions, user);
            if (nameDataResult.getNumResults() > fileDataResult.getNumResults()) {
                fileDataResult = nameDataResult;
                fileStringFunction = File::getName;
            }
        }

        if (fileDataResult.getNumResults() > correctedFileList.size()) {
            throw new CatalogException("Error: More than one file found for at least one of the files introduced");
        } else if (ignoreException || fileDataResult.getNumResults() == correctedFileList.size()) {
            return keepOriginalOrder(correctedFileList, fileStringFunction, fileDataResult, ignoreException, false);
        } else {
            // The file could not be found or the user does not have permissions to see it
            // Check if the file can be found without adding the user restriction
            OpenCGAResult<File> resultsNoCheck = fileDBAdaptor.get(queryCopy, queryOptions);
            if (resultsNoCheck.getNumResults() == correctedFileList.size()) {
                throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the files.");
            }
            if (canBeSearchedAsName) {
                // The last query was performed by name, so we now search by path
                queryCopy = query == null ? new Query() : new Query(query);
                queryCopy.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                        .append(FileDBAdaptor.QueryParams.PATH.key(), correctedFileList);
                resultsNoCheck = fileDBAdaptor.get(queryCopy, queryOptions);
                if (resultsNoCheck.getNumResults() == correctedFileList.size()) {
                    throw new CatalogAuthorizationException("Permission denied. " + user + " is not allowed to see some or none of the "
                            + "files.");
                }
            }

            throw CatalogException.notFound("files", getMissingFields(uniqueList, fileDataResult.getResults(), fileStringFunction));
        }
    }

    private OpenCGAResult<File> getFile(long studyUid, String fileUuid, QueryOptions options) throws CatalogException {
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FileDBAdaptor.QueryParams.UUID.key(), fileUuid);
        return fileDBAdaptor.get(query, options);
    }

    public URI getUri(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        if (file.getUri() != null) {
            return file.getUri();
        } else {
            OpenCGAResult<File> fileDataResult = fileDBAdaptor.get(file.getUid(), INCLUDE_STUDY_URI);
            if (fileDataResult.getNumResults() == 0) {
                throw new CatalogException("File " + file.getUid() + " not found");
            }
            return fileDataResult.first().getUri();
        }
    }

    public Study getStudy(File file, String sessionId) throws CatalogException {
        ParamUtils.checkObj(file, "file");
        ParamUtils.checkObj(sessionId, "session id");

        if (file.getStudyUid() <= 0) {
            throw new CatalogException("Missing study uid field in file");
        }

        String user = userManager.getUserId(sessionId);

        Query query = new Query(StudyDBAdaptor.QueryParams.UID.key(), file.getStudyUid());
        OpenCGAResult<Study> studyDataResult = studyDBAdaptor.get(query, QueryOptions.empty(), user);
        if (studyDataResult.getNumResults() == 1) {
            return studyDataResult.first();
        } else {
            authorizationManager.checkCanViewStudy(file.getStudyUid(), user);
            throw new CatalogException("Incorrect study uid");
        }
    }

    public void matchUpVariantFiles(String studyStr, List<File> transformedFiles, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        for (File transformedFile : transformedFiles) {
            authorizationManager.checkFilePermission(study.getUid(), transformedFile.getUid(), userId, FileAclEntry.FilePermissions.WRITE);
            String variantPathName = getMainVariantFile(transformedFile.getPath());
            if (variantPathName == null) {
                // Skip the file.
                logger.debug("The file {} is not a variant transformed file", transformedFile.getName());
                continue;
            }

            // Search in the same path
            logger.info("Looking for vcf file in path {}", variantPathName);
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), variantPathName)
                    .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);

            List<File> fileList = fileDBAdaptor.get(query, new QueryOptions()).getResults();

            if (fileList.isEmpty()) {
                // Search by name in the whole study
                String variantFileName = getMainVariantFile(transformedFile.getName());
                logger.info("Looking for vcf file by name {}", variantFileName);
                query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(FileDBAdaptor.QueryParams.NAME.key(), variantFileName)
                        .append(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.VARIANT);
                fileList = new ArrayList<>(fileDBAdaptor.get(query, new QueryOptions()).getResults());

                // In case of finding more than one file, try to find the proper one.
                if (fileList.size() > 1) {
                    // Discard files already with a transformed file.
                    fileList.removeIf(file -> file.getInternal().getIndex() != null
                            && file.getInternal().getIndex().getTransformedFile() != null
                            && file.getInternal().getIndex().getTransformedFile().getId() != transformedFile.getUid());
                }
                if (fileList.size() > 1) {
                    // Discard files not transformed or indexed.
                    fileList.removeIf(file -> file.getInternal().getIndex() == null
                            || file.getInternal().getIndex().getStatus() == null
                            || file.getInternal().getIndex().getStatus().getName() == null
                            || file.getInternal().getIndex().getStatus().getName().equals(FileIndex.IndexStatus.NONE));
                }
            }


            if (fileList.size() != 1) {
                // VCF file not found
                logger.warn("The vcf file corresponding to the file " + transformedFile.getName() + " could not be found");
                continue;
            }
            File vcf = fileList.get(0);

            // Look for the json file. It should be in the same directory where the transformed file is.
            String jsonPathName = getVariantMetadataFile(transformedFile.getPath());
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), jsonPathName)
                    .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.JSON);
            fileList = fileDBAdaptor.get(query, new QueryOptions()).getResults();
            if (fileList.size() != 1) {
                // Skip. This should not ever happen
                logger.warn("The json file corresponding to the file " + transformedFile.getName() + " could not be found");
                continue;
            }
            File json = fileList.get(0);

            /* Update relations */
            FileRelatedFile producedFromRelation = new FileRelatedFile(vcf, FileRelatedFile.Relation.PRODUCED_FROM);

            // Update json file
            logger.debug("Updating json relation");
            List<FileRelatedFile> relatedFiles = ParamUtils.defaultObject(json.getRelatedFiles(), ArrayList::new);
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
            FileIndex index = vcf.getInternal().getIndex();
            if (index.getTransformedFile() == null) {
                index.setTransformedFile(new FileIndex.TransformedFile(transformedFile.getUid(), json.getUid()));
            }
            String status = FileIndex.IndexStatus.NONE;
            if (vcf.getInternal().getIndex() != null && vcf.getInternal().getIndex().getStatus() != null
                    && vcf.getInternal().getIndex().getStatus().getName() != null) {
                status = vcf.getInternal().getIndex().getStatus().getName();
            }
            if (FileIndex.IndexStatus.NONE.equals(status)) {
                // If TRANSFORMED, TRANSFORMING, etc, do not modify the index status
                index.setStatus(new FileIndex.IndexStatus(FileIndex.IndexStatus.TRANSFORMED, "Found transformed file"));
            }
            ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.INTERNAL_INDEX.key(), index);
            fileDBAdaptor.update(vcf.getUid(), params, QueryOptions.empty());

            // Update variant stats
            Path statsFile = Paths.get(json.getUri().getRawPath());
            try (InputStream is = FileUtils.newInputStream(statsFile)) {
                VariantFileMetadata fileMetadata = getDefaultObjectMapper().readValue(is, VariantFileMetadata.class);
                VariantSetStats stats = fileMetadata.getStats();

                AnnotationSet annotationSet = AvroToAnnotationConverter.convertToAnnotationSet(stats, FILE_VARIANT_STATS_VARIABLE_SET);
                catalogManager.getFileManager()
                        .update(studyStr, vcf.getPath(), new FileUpdateParams().setAnnotationSets(Collections.singletonList(annotationSet)),
                                new QueryOptions(Constants.ACTIONS,
                                        Collections.singletonMap(ANNOTATION_SETS, ParamUtils.CompleteUpdateAction.SET)), sessionId);


            } catch (IOException e) {
                throw new CatalogException("Error reading file \"" + statsFile + "\"", e);
            }
        }
    }

    public OpenCGAResult<FileIndex> updateFileIndexStatus(File file, String newStatus, String message, String sessionId)
            throws CatalogException {
        return updateFileIndexStatus(file, newStatus, message, null, sessionId);
    }

    public OpenCGAResult<FileIndex> updateFileIndexStatus(File file, String newStatus, String message, Integer release, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyDBAdaptor.get(file.getStudyUid(), StudyManager.INCLUDE_STUDY_ID).first();

        ObjectMap auditParams = new ObjectMap()
                .append("file", file)
                .append("newStatus", newStatus)
                .append("message", message)
                .append("release", release)
                .append("token", token);

        authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        FileIndex index = file.getInternal().getIndex();
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
        ObjectMap params = null;
        try {
            params = new ObjectMap(FileDBAdaptor.QueryParams.INTERNAL_INDEX.key(), new ObjectMap(getUpdateObjectMapper()
                    .writeValueAsString(index)));
        } catch (JsonProcessingException e) {
            throw new CatalogException("Cannot parse index object: " + e.getMessage(), e);
        }
        OpenCGAResult update = fileDBAdaptor.update(file.getUid(), params, QueryOptions.empty());
        auditManager.auditUpdate(userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(), study.getUuid(),
                auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

        return new OpenCGAResult<>(update.getTime(), update.getEvents(), 1, Collections.singletonList(index), 1);
    }

    @Deprecated
    public OpenCGAResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException {
        OpenCGAResult<File> fileDataResult = fileDBAdaptor.get(fileId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.STUDY_UID.key())));

        if (fileDataResult.getNumResults() == 0) {
            return fileDataResult;
        }

        String userId = userManager.getUserId(sessionId);
        authorizationManager.checkFilePermission(fileDataResult.first().getStudyUid(), fileId, userId, FileAclEntry.FilePermissions.VIEW);

        return getParents(fileDataResult.first().getStudyUid(), fileDataResult.first().getPath(), true, options);
    }

    public OpenCGAResult<File> createFolder(String studyStr, String path, boolean parents, String description, QueryOptions options,
                                            String token) throws CatalogException {
        return createFolder(studyStr, path, parents, description, "", options, token);
    }

    public OpenCGAResult<File> createFolder(String studyStr, String path, boolean parents, String description, String jobId,
                                            QueryOptions options, String token) throws CatalogException {
        ParamUtils.checkPath(path, "folderPath");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        if (!path.endsWith("/")) {
            path = path + "/";
        }

        OpenCGAResult<File> fileDataResult;
        switch (checkPathExists(path, study.getUid())) {
            case FREE_PATH:
                File file = new File(File.Type.DIRECTORY, File.Format.NONE, File.Bioformat.NONE, path, description,
                        FileInternal.initialize(), 0, null, null, jobId, null, null);
                fileDataResult = create(studyStr, file, parents, null, options, token);
                break;
            case DIRECTORY_EXISTS:
                Query query = new Query()
                        .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                        .append(FileDBAdaptor.QueryParams.PATH.key(), path);
                fileDataResult = fileDBAdaptor.get(study.getUid(), query, options, userId);
                fileDataResult.getEvents().add(new Event(Event.Type.WARNING, path, "Folder already existed"));
                break;
            case FILE_EXISTS:
            default:
                throw new CatalogException("A file with the same name of the folder already exists in Catalog");
        }

        return fileDataResult;
    }

    public OpenCGAResult<File> createFile(String studyStr, String path, String description, boolean parents, String content,
                                          String sessionId) throws CatalogException {
        ParamUtils.checkPath(path, "filePath");

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        if (path.startsWith("/")) {
            path = path.substring(1);
        }

        switch (checkPathExists(path, study.getUid())) {
            case FREE_PATH:
                return create(studyStr, File.Type.FILE, File.Format.PLAIN, File.Bioformat.UNKNOWN, path, description,
                        0, null, null, parents, content, new QueryOptions(),
                        sessionId);
            case FILE_EXISTS:
            case DIRECTORY_EXISTS:
            default:
                throw new CatalogException("A file or folder with the same name already exists in the path of Catalog");
        }
    }

    public OpenCGAResult<File> create(String studyStr, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                                      String description, long size, Map<String, Object> stats, Map<String, Object> attributes,
                                      boolean parents, String content, QueryOptions options, String token) throws CatalogException {
        File file = new File(type, format, bioformat, path, description, FileInternal.initialize(), size, Collections.emptyList(), null, "",
                stats, attributes);
        return create(studyStr, file, parents, content, options, token);
    }

    @Override
    public OpenCGAResult<File> create(String studyStr, File entry, QueryOptions options, String token) throws CatalogException {
        throw new NotImplementedException("Call to create passing parents and content variables");
    }

    public OpenCGAResult<File> create(String studyStr, File file, boolean parents, String content, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("file", file)
                .append("parents", parents)
                .append("content", content)
                .append("options", options)
                .append("token", token);
        try {
            File parentFile = getParents(study.getUid(), file.getPath(), false, INCLUDE_FILE_URI_PATH).first();
            authorizationManager.checkFilePermission(study.getUid(), parentFile.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

            OpenCGAResult<File> result = create(study, file, parents, content, options, token);
            auditManager.auditCreate(userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Resource.FILE, file.getId(), "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    void validateNewFile(Study study, File file, String sessionId, boolean overwrite) throws CatalogException {
        /** Check and set all the params and create a File object **/
        ParamUtils.checkObj(file, "File");
        ParamUtils.checkPath(file.getPath(), "path");
        file.setType(ParamUtils.defaultObject(file.getType(), File.Type.FILE));
        file.setFormat(ParamUtils.defaultObject(file.getFormat(), File.Format.PLAIN));
        file.setBioformat(ParamUtils.defaultObject(file.getBioformat(), File.Bioformat.NONE));
        file.setDescription(ParamUtils.defaultString(file.getDescription(), ""));
        file.setRelatedFiles(ParamUtils.defaultObject(file.getRelatedFiles(), ArrayList::new));
        file.setSampleIds(ParamUtils.defaultObject(file.getSampleIds(), ArrayList::new));
        file.setCreationDate(TimeUtils.getTime());
        file.setJobId(ParamUtils.defaultString(file.getJobId(), ""));
        file.setModificationDate(file.getCreationDate());
        file.setTags(ParamUtils.defaultObject(file.getTags(), ArrayList::new));
        file.setInternal(ParamUtils.defaultObject(file.getInternal(), FileInternal::new));
        file.getInternal().setIndex(ParamUtils.defaultObject(file.getInternal().getIndex(), FileIndex.initialize()));
        file.getInternal().setStatus(ParamUtils.defaultObject(file.getInternal().getStatus(), new FileStatus(FileStatus.READY)));
        file.getInternal().setSampleMap(ParamUtils.defaultObject(file.getInternal().getSampleMap(), HashMap::new));
        file.setStatus(ParamUtils.defaultObject(file.getStatus(), CustomStatus::new));
        file.setStats(ParamUtils.defaultObject(file.getStats(), HashMap::new));
        file.setAttributes(ParamUtils.defaultObject(file.getAttributes(), HashMap::new));

//        validateNewSamples(study, file, sessionId);

        if (file.getSize() < 0) {
            throw new CatalogException("Error: DiskUsage can't be negative!");
        }

        // Fix path
        if (file.getType() == File.Type.DIRECTORY && !file.getPath().endsWith("/")) {
            file.setPath(file.getPath() + "/");
        }
        if (file.getType() == File.Type.FILE && file.getPath().endsWith("/")) {
            file.setPath(file.getPath().substring(0, file.getPath().length() - 1));
        }
        file.setName(Paths.get(file.getPath()).getFileName().toString());
        file.setId(file.getPath().replace("/", ":"));

        URI uri;
        try {
            if (file.getType() == File.Type.DIRECTORY) {
                uri = getFileUri(study.getUid(), file.getPath(), true);
            } else {
                uri = getFileUri(study.getUid(), file.getPath(), false);
            }
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }
        file.setUri(uri);

        if (!overwrite) {
            // Check if it already exists
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath());
            if (fileDBAdaptor.count(query).getNumMatches() > 0) {
                logger.warn("The file '{}' already exists in catalog", file.getPath());
                throw new CatalogException("The file '" + file.getPath() + "' already exists in catalog");
            }
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.URI.key(), uri);
            OpenCGAResult<File> fileResult = fileDBAdaptor.get(query,
                    new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key()));
            if (fileResult.getNumResults() > 0) {
                logger.warn("The uri '{}' of the file is already in catalog but in path '{}'.", uri, fileResult.first().getPath());
                throw new CatalogException("The uri '" + uri + "' of the file is already in catalog but in path '"
                        + fileResult.first().getPath() + "'");
            }
        }

        boolean external = isExternal(study, file.getPath(), uri);
        file.setExternal(external);
        file.setRelease(studyManager.getCurrentRelease(study));

        validateNewAnnotationSets(study.getVariableSets(), file.getAnnotationSets());

        file.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
        checkHooks(file, study.getFqn(), HookConfiguration.Stage.CREATE);
    }

    private OpenCGAResult<File> register(Study study, File file, List<Sample> existingSamples, List<Sample> nonExistingSamples,
                                         boolean parents, QueryOptions options, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        long studyId = study.getUid();

        //Find parent. If parents == true, create folders.
        String parentPath = getParentPath(file.getPath());

        long parentFileId = fileDBAdaptor.getId(studyId, parentPath);
        boolean newParent = false;
        if (parentFileId < 0 && StringUtils.isNotEmpty(parentPath)) {
            if (parents) {
                newParent = true;
                File parentFile = new File(File.Type.DIRECTORY, File.Format.NONE, File.Bioformat.NONE, parentPath, "",
                        new FileInternal(new FileStatus(FileStatus.READY), new FileIndex(), Collections.emptyMap()), 0,
                        Collections.emptyList(), null, "", Collections.emptyMap(), Collections.emptyMap());
                validateNewFile(study, parentFile, sessionId, false);
                parentFileId = register(study, parentFile, existingSamples, nonExistingSamples, parents, options, sessionId)
                        .first().getUid();
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

        fileDBAdaptor.insert(studyId, file, existingSamples, nonExistingSamples, study.getVariableSets(), options);
        OpenCGAResult<File> queryResult = getFile(studyId, file.getUuid(), options);
        // We obtain the permissions set in the parent folder and set them to the file or folder being created
        OpenCGAResult<Map<String, List<String>>> allFileAcls = authorizationManager.getAllFileAcls(studyId, parentFileId, userId, false);
        // Propagate ACLs
        if (allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(studyId, Arrays.asList(queryResult.first().getUid()), allFileAcls.getResults().get(0),
                    Enums.Resource.FILE);
        }

        matchUpVariantFiles(study.getFqn(), queryResult.getResults(), sessionId);

        return queryResult;
    }

    private OpenCGAResult<File> create(Study study, File file, boolean parents, String content, QueryOptions options, String sessionId)
            throws CatalogException {
        validateNewFile(study, file, sessionId, false);

        IOManager ioManager;
        try {
            ioManager = ioManagerFactory.get(file.getUri());
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(file.getUri(), e);
        }
        if (file.getType() == File.Type.FILE && StringUtils.isNotEmpty(content)) {
            // We set parents to true because the file has been successfully registered, which means the directories are already registered
            // in catalog
            ioManager.createDirectory(Paths.get(file.getUri()).getParent().toUri(), true);
            InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
            ioManager.copy(inputStream, file.getUri());
        }

        List<Sample> nonExistingSamples = new LinkedList<>();
        List<Sample> existingSamples = new LinkedList<>();
        if (file.getType() == File.Type.FILE && ioManager.exists(file.getUri())) {
            new FileMetadataReader(catalogManager).addMetadataInformation(study.getFqn(), file);
            validateNewSamples(study, file, existingSamples, nonExistingSamples, sessionId);
        }

        OpenCGAResult<File> result;
        try {
            result = register(study, file, existingSamples, nonExistingSamples, parents, options, sessionId);
        } catch (CatalogException e) {
            if (file.getType() == File.Type.FILE && StringUtils.isNotEmpty(content)) {
                ioManager.deleteFile(file.getUri());
            }
            throw new CatalogException("Error registering file: " + e.getMessage(), e);
        }

        return result;
    }

    private void validateNewSamples(Study study, File file, List<Sample> existingSamples, List<Sample> nonExistingSamples, String sessionId)
            throws CatalogException {
        if (file.getSampleIds() == null || file.getSampleIds().isEmpty()) {
            return;
        }

        String userId = catalogManager.getUserManager().getUserId(sessionId);

        InternalGetDataResult<Sample> sampleResult = catalogManager.getSampleManager().internalGet(study.getUid(), file.getSampleIds(),
                SampleManager.INCLUDE_SAMPLE_IDS, userId, true);

        existingSamples.addAll(sampleResult.getResults());
        for (InternalGetDataResult<Sample>.Missing missing : sampleResult.getMissing()) {
            Sample sample = new Sample().setId(missing.getId());
            catalogManager.getSampleManager().validateNewSample(study, sample, userId);
            nonExistingSamples.add(sample);
        }
    }

    /**
     * Upload a file in Catalog.
     *
     * @param studyStr        study where the file will be uploaded.
     * @param fileInputStream Input stream of the file to be uploaded.
     * @param file            File object containing at least the basic metadata necessary for a successful upload: path
     * @param overwrite       Overwrite the current file if any.
     * @param parents         boolean indicating whether unexisting parent folders should also be created automatically.
     * @param calculateChecksum boolean indicating whether to calculate the checksum of the uploaded file.
     * @param token       session id of the user performing the upload.
     * @return a OpenCGAResult with the file uploaded.
     * @throws CatalogException if the user does not have permissions or any other unexpected issue happens.
     */
    public OpenCGAResult<File> upload(String studyStr, InputStream fileInputStream, File file, boolean overwrite, boolean parents,
                                      boolean calculateChecksum, String token) throws CatalogException {
        // Check basic parameters
        ParamUtils.checkObj(fileInputStream, "fileInputStream");

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("file", file)
                .append("overwrite", overwrite)
                .append("parents", parents)
                .append("calculateChecksum", calculateChecksum)
                .append("token", token);
        try {
            validateNewFile(study, file, token, overwrite);

            File overwrittenFile = null;
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), file.getPath());
            OpenCGAResult<File> fileDataResult = fileDBAdaptor.get(query, QueryOptions.empty());
            if (fileDataResult.getNumResults() > 0) {
                if (overwrite) {
                    overwrittenFile = fileDataResult.first();
                } else {
                    throw new CatalogException("Path " + file.getPath() + " already in use");
                }
            }

            OpenCGAResult<File> parentFolders = getParents(study.getUid(), file.getPath(), false, QueryOptions.empty());
            if (parentFolders.getNumResults() == 0) {
                // There always must be at least the root folder
                throw new CatalogException("Unexpected error happened.");
            }

            // Check permissions over the most internal path
            authorizationManager.checkFilePermission(study.getUid(), parentFolders.first().getUid(), userId,
                    FileAclEntry.FilePermissions.UPLOAD);
            authorizationManager.checkFilePermission(study.getUid(), parentFolders.first().getUid(), userId,
                    FileAclEntry.FilePermissions.WRITE);

            // We obtain the basic studyPath where we will upload the file temporarily
            java.nio.file.Path studyPath = Paths.get(study.getUri());

            IOManager ioManager;
            try {
                ioManager = ioManagerFactory.get(file.getUri());
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(file.getUri(), e);
            }
            // We attempt to create it first because it may be that the parent directories were not created because they don't contain any
            // files yet

            if (parentFolders.first().getType() == File.Type.FILE && !overwrite) {
                throw new CatalogException("Cannot upload file in '" + file.getPath() + "'. " + parentFolders.first().getPath()
                        + "' is already an existing file path.");
            } else if (parentFolders.first().getType() == File.Type.DIRECTORY) {
                ioManager.createDirectory(parentFolders.first().getUri(), true);
            }
            ioManager.checkWritableUri(parentFolders.first().getUri());

            java.nio.file.Path tempFilePath = studyPath.resolve("tmp_" + file.getName()).resolve(file.getName());
            URI tempDirectory = tempFilePath.getParent().toUri();
            logger.info("Uploading file... Temporal file path: {}", tempFilePath.toString());

            // Create the temporal directory and upload the file
            try {
                if (!ioManager.exists(tempFilePath.getParent().toUri())) {
                    logger.debug("Creating temporal folder: {}", tempFilePath.getParent());
                    ioManager.createDirectory(tempDirectory, true);
                }

                // Start uploading the file to the temporal directory
                // Upload the file to a temporary folder
                ioManager.copy(fileInputStream, tempFilePath.toUri());
            } catch (Exception e) {
                logger.error("Error uploading file {}", file.getName(), e);

                // Clean temporal directory
                ioManager.deleteDirectory(tempDirectory);

                throw new CatalogException("Error uploading file " + file.getName(), e);
            }
            URI sourceUri = tempFilePath.toUri();

            List<Sample> existingSamples = new LinkedList<>();
            List<Sample> nonExistingSamples = new LinkedList<>();

            // Move the file from the temporal directory
            try {
                // Create the directories where the file will be placed (if they weren't created before)
                ioManager.createDirectory(Paths.get(file.getUri()).getParent().toUri(), true);

                String checksum = null;
                if (calculateChecksum) {
                    checksum = ioManager.calculateChecksum(sourceUri);
                }
                if (overwrite) {
                    ioManager.move(sourceUri, file.getUri(), StandardCopyOption.REPLACE_EXISTING);
                } else {
                    ioManager.move(sourceUri, file.getUri());
                }
                if (calculateChecksum && !checksum.equals(ioManager.calculateChecksum(file.getUri()))) {
                    throw new CatalogIOException("Error moving file from " + sourceUri + " to " + file.getUri());
                }

                // Remove the temporal directory
                ioManager.deleteDirectory(tempDirectory);

                file.setChecksum(checksum);

                // Improve metadata information and extract samples if any
                new FileMetadataReader(catalogManager).addMetadataInformation(study.getFqn(), file);
                validateNewSamples(study, file, existingSamples, nonExistingSamples, token);
            } catch (CatalogException e) {
                ioManager.deleteDirectory(tempDirectory);
                logger.error("Upload file: {}", e.getMessage(), e);
                throw new CatalogException("Upload file failed. Could not move the content to " + file.getUri() + ": " + e.getMessage());
            }

            // Register the file in catalog
            try {
                if (overwrittenFile != null) {
                    // We need to update the existing file document
                    ObjectMap params = new ObjectMap();
                    QueryOptions queryOptions = new QueryOptions();

                    params.put(FileDBAdaptor.QueryParams.SIZE.key(), file.getSize());
                    params.put(FileDBAdaptor.QueryParams.URI.key(), file.getUri());
                    params.put(FileDBAdaptor.QueryParams.EXTERNAL.key(), file.isExternal());
                    params.put(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.READY);
                    params.put(FileDBAdaptor.QueryParams.CHECKSUM.key(), file.getChecksum());

                    if (file.getSampleIds() != null && !file.getSampleIds().isEmpty()) {
                        params.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), file.getSampleIds());

                        // Set new samples
                        Map<String, Object> actionMap = new HashMap<>();
                        actionMap.put(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), ParamUtils.UpdateAction.SET.name());
                        queryOptions.put(Constants.ACTIONS, actionMap);
                    }
                    if (!file.getAttributes().isEmpty()) {
                        Map<String, Object> attributes = overwrittenFile.getAttributes();
                        attributes.putAll(file.getAttributes());
                        params.put(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), attributes);
                    }
                    if (!file.getStats().isEmpty()) {
                        Map<String, Object> stats = overwrittenFile.getStats();
                        stats.putAll(file.getStats());
                        params.put(FileDBAdaptor.QueryParams.STATS.key(), stats);
                    }

                    fileDBAdaptor.update(overwrittenFile.getUid(), params, null, queryOptions);
                } else {
                    // We need to register a new file
                    register(study, file, existingSamples, nonExistingSamples, parents, QueryOptions.empty(), token);
                }
            } catch (CatalogException e) {
                ioManager.deleteFile(file.getUri());
                logger.error("Upload file: {}", e.getMessage(), e);
                throw new CatalogException("Upload file failed. Could not register the file in the DB: " + e.getMessage());
            }

            auditManager.auditCreate(userId, Enums.Action.UPLOAD, Enums.Resource.FILE, file.getId(), file.getUuid(),
                    study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return fileDBAdaptor.get(query, QueryOptions.empty());
        } catch (CatalogException e) {
            auditManager.auditCreate(userId, Enums.Action.UPLOAD, Enums.Resource.FILE, file.getId(), "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Moves a file not yet registered in OpenCGA from origin to finalDestiny in the file system and then registers it in the study.
     *
     * @param studyStr      Study to which the file will belong.
     * @param fileSource    Current location of the file (file system).
     * @param folderDestiny Directory where the file needs to be moved (file system).
     * @param path          Directory in catalog where the file will be registered (catalog).
     * @param token         Token of the user.
     * @return An OpenCGAResult with the file registry after moving it to the final destination.
     * @throws CatalogException CatalogException.
     */
    public OpenCGAResult<File> moveAndRegister(String studyStr, Path fileSource, @Nullable Path folderDestiny, @Nullable String path,
                                               String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyStr", studyStr)
                .append("fileSource", fileSource)
                .append("folderDestiny", folderDestiny)
                .append("path", path)
                .append("token", token);

        try {
            try {
                FileUtils.checkFile(fileSource);
            } catch (IOException e) {
                throw new CatalogException("File '" + fileSource + "' not found", e);
            }
            String fileName = fileSource.toFile().getName();

            if (folderDestiny == null && path == null) {
                throw new CatalogException("'folderDestiny' and 'path' cannot be both null.");
            }

            boolean external = false;
            if (folderDestiny == null) {
                if (path.startsWith("/")) {
                    path = path.substring(1);
                }
                File parentFolder = getParents(study.getUid(), path, false, INCLUDE_FILE_URI_PATH).first();

                // We get the relative path
                String relativePath = Paths.get(parentFolder.getPath()).relativize(Paths.get(path)).toString();
                folderDestiny = Paths.get(parentFolder.getUri()).resolve(relativePath);
            }

            if (folderDestiny.toString().startsWith(study.getUri().getPath())) {
                if (StringUtils.isNotEmpty(path)) {
                    String myPath = path;
                    if (!myPath.endsWith("/")) {
                        myPath += "/";
                    }
                    myPath += fileName;

                    String relativePath = Paths.get(study.getUri().getPath()).relativize(folderDestiny.resolve(fileName)).toString();
                    if (!relativePath.equals(myPath)) {
                        throw new CatalogException("Destination uri within the workspace and path do not match");
                    }
                } else {
                    //Set the path to whichever path would corresponding based on the workspace uri
                    path = Paths.get(study.getUri().getPath()).relativize(folderDestiny).toString();
                }

                File parentFolder = getParents(study.getUid(), path, false, INCLUDE_FILE_URI_PATH).first();
                authorizationManager.checkFilePermission(study.getUid(), parentFolder.getUid(), userId, FileAclEntry.FilePermissions.WRITE);
            } else {
                // It will be moved to an external folder. Only admins can move to that directory
                if (!authorizationManager.isOwnerOrAdmin(study.getUid(), userId)) {
                    throw new CatalogAuthorizationException("Only owners or administrative users are allowed to move to folders different "
                            + "than the main OpenCGA workspace");
                }
                external = true;
            }

            String filePath = path;
            if (!filePath.endsWith("/")) {
                filePath += "/";
            }
            filePath += fileName;
            // Check the path is not in use
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.PATH.key(), filePath)
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            if (fileDBAdaptor.count(query).getNumMatches() > 0) {
                throw new CatalogException("Path '" + filePath + "' already in use in OpenCGA");
            }

            URI folderDestinyUri = folderDestiny.toUri();
            IOManager ioManager = null;
            try {
                ioManager = ioManagerFactory.get(folderDestinyUri);
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(folderDestinyUri, e);
            }
            // Check uri-path
            try {
                if (!ioManager.exists(folderDestinyUri)) {
                    ioManager.createDirectory(folderDestinyUri, true);
                }

                ioManager.move(fileSource.toUri(), folderDestiny.resolve(fileName).toUri(), StandardCopyOption.REPLACE_EXISTING);
            } catch (CatalogIOException e) {
                throw new CatalogException("Unexpected error. Could not move file from '" + fileSource + "' to '" + folderDestiny + "'", e);
            }

            OpenCGAResult<File> result;
            if (external) {
                result = link(study.getFqn(), folderDestiny.resolve(fileName).toUri(), path, new ObjectMap("parents", true), token);
            } else {
                result = createFile(study.getFqn(), filePath, "", true, null, token);
            }

            auditManager.audit(userId, Enums.Action.MOVE_AND_REGISTER, Enums.Resource.FILE, result.first().getId(),
                    result.first().getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.MOVE_AND_REGISTER, Enums.Resource.FILE, "", "", study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Deprecated
    public OpenCGAResult<File> get(Long fileId, QueryOptions options, String sessionId) throws CatalogException {
        return get(null, String.valueOf(fileId), options, sessionId);
    }

    public OpenCGAResult<FileTree> getTree(@Nullable String studyId, String fileId, int maxDepth, QueryOptions options, String token)
            throws CatalogException {
        long startTime = System.currentTimeMillis();

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("fileId", fileId)
                .append("options", options)
                .append("maxDepth", maxDepth)
                .append("token", token);
        try {
            if (maxDepth < 1) {
                throw new CatalogException("Depth cannot be lower than 1");
            }
            if (options.containsKey(QueryOptions.INCLUDE)) {
                // Add type and path to the queryOptions
                List<String> asStringListOld = options.getAsStringList(QueryOptions.INCLUDE);
                Set<String> newList = new HashSet<>(asStringListOld);
                newList.add(FileDBAdaptor.QueryParams.TYPE.key());
                newList.add(FileDBAdaptor.QueryParams.PATH.key());
                options.put(QueryOptions.INCLUDE, new ArrayList<>(newList));
            } else {
                if (options.containsKey(QueryOptions.EXCLUDE)) {
                    // Avoid excluding type and path from queryoptions
                    List<String> asStringListOld = options.getAsStringList(QueryOptions.EXCLUDE);
                    Set<String> newList = new HashSet<>(asStringListOld);
                    newList.remove(FileDBAdaptor.QueryParams.TYPE.key());
                    newList.remove(FileDBAdaptor.QueryParams.PATH.key());
                    if (newList.size() > 0) {
                        options.put(QueryOptions.EXCLUDE, new ArrayList<>(newList));
                    } else {
                        options.remove(QueryOptions.EXCLUDE);
                    }
                }
            }

            File file = internalGet(study.getUid(), fileId, options, userId).first();

            // Check if the id does not correspond to a directory
            if (!file.getType().equals(File.Type.DIRECTORY)) {
                throw new CatalogException("The file introduced is not a directory.");
            }

            // Build regex to obtain all the files/directories up to certain depth
            String baseRegex = "([^\\/]+)";
            StringBuilder pathRegex = new StringBuilder(baseRegex);
            for (int i = 1; i < maxDepth; i++) {
                pathRegex.append("[\\/]?").append(baseRegex).append("?");
            }
            // It can end in a directory or not
            pathRegex.append("[\\/]?$");
            Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + pathRegex.toString());
            // We want to know beforehand the number of matches we will get to be able to abort before iterating
            options.put(QueryOptions.COUNT, true);

            FileTreeBuilder treeBuilder = new FileTreeBuilder(file);
            int numResults;
            try (DBIterator<File> iterator = fileDBAdaptor.iterator(study.getUid(), query, options, userId)) {
                if (iterator.getNumMatches() > MAX_LIMIT) {
                    throw new CatalogException("Please, decrease the maximum depth. More than " + MAX_LIMIT + " files found");
                }
                numResults = (int) iterator.getNumMatches() + 1;
                while (iterator.hasNext()) {
                    treeBuilder.add(iterator.next());
                }
            }
            FileTree fileTree = treeBuilder.toFileTree();
            int dbTime = (int) (System.currentTimeMillis() - startTime);

            auditManager.audit(userId, Enums.Action.TREE, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(dbTime, Collections.emptyList(), numResults, Collections.singletonList(fileTree), numResults);
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.TREE, Enums.Resource.FILE, fileId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<File> getFilesFromFolder(String folderStr, String studyStr, QueryOptions options, String sessionId)
            throws CatalogException {
        ParamUtils.checkObj(folderStr, "folder");
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        File file = internalGet(study.getUid(), folderStr, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FileDBAdaptor.QueryParams.PATH.key(), FileDBAdaptor.QueryParams.TYPE.key())), userId).first();

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (!file.getType().equals(File.Type.DIRECTORY)) {
            throw new CatalogDBException("File {path:'" + file.getPath() + "'} is not a folder.");
        }
        Query query = new Query(FileDBAdaptor.QueryParams.DIRECTORY.key(), file.getPath());
        return search(studyStr, query, options, sessionId);
    }

    @Override
    public DBIterator<File> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        Query finalQuery = new Query(query);
        // Fix query if it contains any annotation
        AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
        AnnotationUtils.fixQueryOptionAnnotation(options);
        fixQueryObject(study, finalQuery, userId);
        finalQuery.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        return fileDBAdaptor.iterator(study.getUid(), query, options, userId);
    }

    @Override
    public OpenCGAResult<File> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        Query finalQuery = new Query(query);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            AnnotationUtils.fixQueryOptionAnnotation(options);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            OpenCGAResult<File> queryResult = fileDBAdaptor.get(study.getUid(), finalQuery, options, userId);
            auditManager.auditSearch(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditSearch(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult<?> distinct(String studyId, String field, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("field", new Query(query))
                .append("query", new Query(query))
                .append("token", token);
        try {
            FileDBAdaptor.QueryParams param = FileDBAdaptor.QueryParams.getParam(field);
            if (param == null) {
                throw new CatalogException("Unknown '" + field + "' parameter.");
            }
            Class<?> clazz = getTypeClass(param.type());

            fixQueryObject(study, query, userId);
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);

            query.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<?> result = fileDBAdaptor.distinct(study.getUid(), field, query, userId, clazz);

            auditManager.auditDistinct(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.auditDistinct(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    void fixQueryObject(Study study, Query query, String user) throws CatalogException {
        super.fixQueryObject(query);

        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.ID.key()))) {
            OpenCGAResult<File> queryResult = internalGet(study.getUid(), query.getAsStringList(FileDBAdaptor.QueryParams.ID.key()),
                    INCLUDE_FILE_IDS, user, true);
            query.remove(FileDBAdaptor.QueryParams.ID.key());
            query.put(FileDBAdaptor.QueryParams.UID.key(), queryResult.getResults().stream().map(File::getUid)
                    .collect(Collectors.toList()));
        }

        validateQueryPath(query, FileDBAdaptor.QueryParams.PATH.key());
        validateQueryPath(query, FileDBAdaptor.QueryParams.DIRECTORY.key());

        // Convert jobId=NONE to jobId=""
        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.JOB_ID.key()))
                && "NONE".equalsIgnoreCase(query.getString(FileDBAdaptor.QueryParams.JOB_ID.key()))) {
            query.put(FileDBAdaptor.QueryParams.JOB_ID.key(), "");
        }

//        // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
//        if (StringUtils.isNotEmpty(query.getString(FileDBAdaptor.QueryParams.SAMPLES.key()))) {
//            OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().internalGet(study.getUid(),
//                    query.getAsStringList(FileDBAdaptor.QueryParams.SAMPLES.key()), SampleManager.INCLUDE_SAMPLE_IDS, user, true);
//            query.put(FileDBAdaptor.QueryParams.SAMPLE_UIDS.key(), sampleDataResult.getResults().stream().map(Sample::getUid)
//                    .collect(Collectors.toList()));
//            query.remove(FileDBAdaptor.QueryParams.SAMPLES.key());
//        }
    }

    private void validateQueryPath(Query query, String key) {
        if (StringUtils.isNotEmpty(query.getString(key))) {
            // Path never starts with /
            List<String> pathList = query.getAsStringList(key);
            List<String> finalPathList = new ArrayList<>(pathList.size());

            for (String path : pathList) {
                String auxPath = path;
                if (auxPath.startsWith("/")) {
                    auxPath = auxPath.substring(1);
                }
                if (FileDBAdaptor.QueryParams.DIRECTORY.key().equals(key) && !auxPath.endsWith("/")) {
                    auxPath = auxPath + "/";
                }
                finalPathList.add(auxPath);
            }

            query.put(key, StringUtils.join(finalPathList, ","));
        }
    }

    @Override
    public OpenCGAResult<File> count(String studyId, Query query, String token) throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("token", token);
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, query);
            // The samples introduced could be either ids or names. As so, we should use the smart resolutor to do this.
            fixQueryObject(study, query, userId);

            query.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
            OpenCGAResult<Long> queryResultAux = fileDBAdaptor.count(query, userId);

            auditManager.auditCount(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>(queryResultAux.getTime(), queryResultAux.getEvents(), 0, Collections.emptyList(),
                    queryResultAux.getNumMatches());
        } catch (CatalogException e) {
            auditManager.auditCount(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    @Override
    public OpenCGAResult delete(String studyStr, List<String> fileIds, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, fileIds, params, false, token);
    }

    public OpenCGAResult delete(String studyStr, List<String> fileIds, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileIds", fileIds)
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // We need to avoid processing subfolders or subfiles of an already processed folder independently
        Set<String> processedPaths = new HashSet<>();
        boolean physicalDelete = params.getBoolean(Constants.SKIP_TRASH, false);

        auditManager.initAuditBatch(operationUuid);
        OpenCGAResult<File> result = OpenCGAResult.empty();
        for (String id : fileIds) {
            String fileId = id;
            String fileUuid = "";

            try {
                OpenCGAResult<File> internalResult = internalGet(study.getUid(), id, INCLUDE_FILE_URI_PATH, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("File '" + id + "' not found");
                }
                File file = internalResult.first();
                // We set the proper values for the audit
                fileId = file.getId();
                fileUuid = file.getUuid();

                if (subpathInPath(file.getPath(), processedPaths)) {
                    // We skip this folder because it is a subfolder or subfile within an already processed folder
                    continue;
                }

                OpenCGAResult updateResult = delete(study, file, physicalDelete, userId);
                result.append(updateResult);

                // We store the processed path as is
                if (file.getType() == File.Type.DIRECTORY) {
                    processedPaths.add(file.getPath());
                }

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, fileId, e.getMessage());
                result.getEvents().add(event);

                logger.error("Could not delete file {}: {}", fileId, e.getMessage(), e);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FILE, fileId, fileUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        return endResult(result, ignoreException);
    }

    @Override
    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, String token) throws CatalogException {
        return delete(studyStr, query, params, false, token);
    }

    public OpenCGAResult delete(String studyStr, Query query, ObjectMap params, boolean ignoreException, String token)
            throws CatalogException {
        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));
        params = ParamUtils.defaultObject(params, ObjectMap::new);

        OpenCGAResult dataResult = OpenCGAResult.empty();

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, new QueryOptions(QueryOptions.INCLUDE,
                StudyDBAdaptor.QueryParams.VARIABLE_SET.key()));

        StopWatch watch = StopWatch.createStarted();
        String operationUuid = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", new Query(query))
                .append("params", params)
                .append("ignoreException", ignoreException)
                .append("token", token);

        // We try to get an iterator containing all the files to be deleted
        DBIterator<File> fileIterator;
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            fileIterator = fileDBAdaptor.iterator(study.getUid(), finalQuery, INCLUDE_FILE_URI_PATH, userId);
        } catch (CatalogException e) {
            auditManager.auditDelete(operationUuid, userId, Enums.Resource.FILE, "", "", study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            throw e;
        }

        // We need to avoid processing subfolders or subfiles of an already processed folder independently
        Set<String> processedPaths = new HashSet<>();
        boolean physicalDelete = params.getBoolean(Constants.SKIP_TRASH, false);

        long numMatches = 0;

        auditManager.initAuditBatch(operationUuid);
        while (fileIterator.hasNext()) {
            File file = fileIterator.next();

            if (subpathInPath(file.getPath(), processedPaths)) {
                // We skip this folder because it is a subfolder or subfile within an already processed folder
                continue;
            }

            try {
                OpenCGAResult result = delete(study, file, physicalDelete, userId);
                dataResult.append(result);

                // We store the processed path as is
                if (file.getType() == File.Type.DIRECTORY) {
                    processedPaths.add(file.getPath());
                }

                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                String errorMsg;

                if (file.getType() == File.Type.FILE) {
                    errorMsg = "Cannot delete file " + file.getPath() + ": " + e.getMessage();
                } else {
                    errorMsg = "Cannot delete folder " + file.getPath() + ": " + e.getMessage();
                }
                dataResult.getEvents().add(new Event(Event.Type.ERROR, file.getPath(), e.getMessage()));

                logger.error(errorMsg, e);
                auditManager.auditDelete(operationUuid, userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationUuid);

        dataResult.setTime((int) watch.getTime(TimeUnit.MILLISECONDS));
        dataResult.setNumMatches(dataResult.getNumMatches() + numMatches);

        return endResult(dataResult, ignoreException);
    }

    private OpenCGAResult delete(Study study, File file, boolean physicalDelete, String userId)
            throws CatalogException {
        // Check if the file or the folder plus any nested files/folders can be deleted
        checkCanDeleteFile(study, file.getPath(), false, Collections.singletonList(FileStatus.PENDING_DELETE), userId);

        String currentStatus = file.getInternal().getStatus().getName();
        if (FileStatus.DELETED.equals(currentStatus)) {
            throw new CatalogException("The file was already deleted");
        }
        if (FileStatus.DELETING.equals(currentStatus)) {
            throw new CatalogException("The file is already being deleted");
        }
        if (!FileStatus.PENDING_DELETE.equals(currentStatus)) {
            throw new CatalogException("The status of file should be " + FileStatus.PENDING_DELETE);
        }

        if (physicalDelete) {
            return physicalDelete(study, file);
        } else {
            return sendToTrash(file);
        }
    }

    /**
     * Given a registered folder in OpenCGA, it will scan its contents to register any nested file/folder that might not be registered.
     *
     * @param studyId  Study id.
     * @param folderId Folder id, path or uuid.
     * @param token    Token of the user. The user will need to have read and write access to the folderId.
     * @return An OpenCGAResult containing the number of files that have been added and the full list of files registered (old and new).
     * @throws CatalogException If there is any of the following errors:
     *                          Study not found, folderId does not exist or user does not have permissions.
     */
    public OpenCGAResult<File> syncUntrackedFiles(String studyId, String folderId, String token) throws CatalogException {
        return syncUntrackedFiles(studyId, folderId, uri -> true, "", token);
    }

    public OpenCGAResult<File> syncUntrackedFiles(String studyId, String folderId, Predicate<URI> filter, String token)
            throws CatalogException {
        return syncUntrackedFiles(studyId, folderId, filter, "", token);
    }

    public OpenCGAResult<File> syncUntrackedFiles(String studyId, String folderId, Predicate<URI> filter, String jobId, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        File folder = internalGet(study.getUid(), folderId, INCLUDE_FILE_URI_PATH, userId).first();

        if (folder.getType() == File.Type.FILE) {
            throw new CatalogException("Provided folder '" + folderId + "' is actually a file");
        }

        authorizationManager.checkFilePermission(study.getUid(), folder.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        IOManager ioManager;
        try {
            ioManager = ioManagerFactory.get(folder.getUri());
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(folder.getUri(), e);
        }
        Iterator<URI> iterator = ioManager.listFilesStream(folder.getUri()).iterator();

        if (filter == null) {
            filter = uri -> true;
        }

        long numMatches = 0;

        OpenCGAResult<File> result = OpenCGAResult.empty();
        List<File> fileList = new ArrayList<>();
        List<Event> eventList = new ArrayList<>();
        while (iterator.hasNext()) {
            URI fileUri = iterator.next().normalize();

            numMatches++;

            if (!filter.test(fileUri)) {
                continue;
            }

            String relativeFilePath = folder.getUri().relativize(fileUri).getPath();
            String finalCatalogPath = Paths.get(folder.getPath()).resolve(relativeFilePath).toString();
            if (relativeFilePath.endsWith("/") && !finalCatalogPath.endsWith("/")) {
                finalCatalogPath += "/";
            }

            try {
                File registeredFile = internalGet(study.getUid(), finalCatalogPath, INCLUDE_FILE_URI_PATH, userId).first();
                if (!registeredFile.getUri().equals(fileUri)) {
                    eventList.add(new Event(Event.Type.WARNING, registeredFile.getPath(), "The uri registered in Catalog '"
                            + registeredFile.getUri().getPath() + "' for the path does not match the uri that would have been synced '"
                            + fileUri.getPath() + "'"));
                }
                fileList.add(registeredFile);
            } catch (CatalogException e) {
                File file = registerFile(study, finalCatalogPath, fileUri, jobId, token).first();

                result.setNumInserted(result.getNumInserted() + 1);
                fileList.add(file);
            }
        }
        result.setNumMatches(numMatches);
        result.setEvents(eventList);
        result.setResults(fileList);
        result.setNumResults(fileList.size());

        return result;
    }

    public OpenCGAResult<File> unlink(@Nullable String studyId, String fileId, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyId)
                .append("file", fileId)
                .append("token", token);

        try {
            ParamUtils.checkParameter(fileId, "File");

            File file = internalGet(study.getUid(), fileId, QueryOptions.empty(), userId).first();

            if (!file.isExternal()) {
                throw new CatalogException("Only previously linked files can be unlinked. Please, use delete instead.");
            }

            // Check if the file or the folder plus any nested files/folders can be deleted
            checkCanDeleteFile(study, file.getPath(), true, Collections.singletonList(FileStatus.PENDING_DELETE), userId);

            OpenCGAResult result = unlink(file);
            auditManager.audit(userId, Enums.Action.UNLINK, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return result;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.UNLINK, Enums.Resource.FILE, fileId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    /**
     * Delete the file from the file system and from OpenCGA.
     *
     * @param study Study object.
     * @param file File or folder.
     * @return a OpenCGAResult object.
     */
    private OpenCGAResult physicalDelete(Study study, File file) throws CatalogException {
        URI fileUri = getUri(file);
        IOManager ioManager = null;
        try {
            ioManager = ioManagerFactory.get(fileUri);
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(fileUri, e);
        }

        OpenCGAResult result = OpenCGAResult.empty();
        if (file.getType() == File.Type.FILE) {
            // 1. Set the file status to deleting
            ObjectMap update = new ObjectMap(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.DELETING);
            fileDBAdaptor.update(file.getUid(), update, QueryOptions.empty());

            // 2. Delete file from the file system
            logger.debug("Deleting file '{} ({})' with uri '{}' from the file system", file.getPath(), file.getUid(), fileUri);
            if (ioManager.exists(fileUri)) {
                try {
                    ioManager.deleteFile(fileUri);
                } catch (CatalogIOException e) {
                    logger.error("Could not delete physically the file '{} ({})'. File deletion aborted.", file.getPath(), file.getUid());
                    // FIXME: Do we restore the status to READY
                    throw new CatalogException("Error deleting file " + file.getPath() + " physically: " + e.getMessage(), e.getCause());
                }
            } else {
                // FIXME: What do we do if the file does not exist in the file system
                logger.warn("Could not delete file '{}'. The file is not accessible or does not exist.", fileUri);
            }

            // 3. Delete file from the database
            result = fileDBAdaptor.delete(file, FileStatus.DELETED);
        } else {
            // 1. Set the folder and all nested files/folders to DELETING
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*");
            ObjectMap update = new ObjectMap(FileDBAdaptor.QueryParams.INTERNAL_STATUS_NAME.key(), FileStatus.DELETING);
            fileDBAdaptor.update(query, update, QueryOptions.empty());

            // 2. Delete files to be deleted from the file system
            QueryOptions options = new QueryOptions(INCLUDE_FILE_URI_PATH)
                    .append(QueryOptions.SORT, FileDBAdaptor.QueryParams.PATH.key())
                    .append(QueryOptions.ORDER, QueryOptions.DESCENDING);
            DBIterator<File> iterator = fileDBAdaptor.iterator(query, options);
            while (iterator.hasNext()) {
                File tmpFile = iterator.next();
                if (ioManager.isDirectory(tmpFile.getUri())) {
                    // If the directory is not empty, it might be that there are other files/folders not registered in OpenCGA, so we only
                    // delete the directory if the directory is empty
                    if (ioManager.listFiles(tmpFile.getUri()).isEmpty()) {
                        ioManager.deleteDirectory(tmpFile.getUri());
                    }
                } else {
                    ioManager.deleteFile(tmpFile.getUri());
                }
            }

            // 3. Delete the folder and all nested files/folders to DELETED
            result = fileDBAdaptor.delete(file, FileStatus.DELETED);
        }

        return result;
    }

    private OpenCGAResult sendToTrash(File file) throws CatalogException {
        return fileDBAdaptor.delete(file, FileStatus.TRASHED);
    }

    private OpenCGAResult unlink(File file) throws CatalogException {
        return fileDBAdaptor.delete(file, FileStatus.REMOVED);
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

    public OpenCGAResult<File> updateAnnotations(String studyStr, String fileStr, String annotationSetId,
                                                 Map<String, Object> annotations, ParamUtils.CompleteUpdateAction action,
                                                 QueryOptions options, String token) throws CatalogException {
        if (annotations == null || annotations.isEmpty()) {
            throw new CatalogException("Missing array of annotations.");
        }
        FileUpdateParams updateParams = new FileUpdateParams()
                .setAnnotationSets(Collections.singletonList(new AnnotationSet(annotationSetId, "", annotations)));
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        options.put(Constants.ACTIONS, new ObjectMap(AnnotationSetManager.ANNOTATIONS, action));

        return update(studyStr, fileStr, updateParams, options, token);
    }

    public OpenCGAResult<File> removeAnnotations(String studyStr, String fileStr, String annotationSetId,
                                                 List<String> annotations, QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, fileStr, annotationSetId, new ObjectMap("remove", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.REMOVE, options, token);
    }

    public OpenCGAResult<File> resetAnnotations(String studyStr, String fileStr, String annotationSetId, List<String> annotations,
                                                QueryOptions options, String token) throws CatalogException {
        return updateAnnotations(studyStr, fileStr, annotationSetId, new ObjectMap("reset", StringUtils.join(annotations, ",")),
                ParamUtils.CompleteUpdateAction.RESET, options, token);
    }

    public OpenCGAResult<File> update(String studyStr, Query query, FileUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        return update(studyStr, query, updateParams, false, options, token);
    }

    public OpenCGAResult<File> update(String studyStr, Query query, FileUpdateParams updateParams, boolean ignoreException,
                                      QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse FileUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("query", query)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        Query finalQuery = new Query(ParamUtils.defaultObject(query, Query::new));

        DBIterator<File> iterator;
        try {
            // Fix query if it contains any annotation
            AnnotationUtils.fixQueryAnnotationSearch(study, finalQuery);
            fixQueryObject(study, finalQuery, userId);
            finalQuery.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

            iterator = fileDBAdaptor.iterator(study.getUid(), finalQuery, EXCLUDE_FILE_ATTRIBUTES, userId);
        } catch (CatalogException e) {
            auditManager.auditUpdate(operationId, userId, Enums.Resource.FILE, "", "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<File> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            File file = iterator.next();
            try {
                OpenCGAResult<File> updateResult = update(study, file, updateParams, options, userId, token);
                result.append(updateResult);

                auditManager.auditUpdate(operationId, userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, file.getId(), e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot update file {}: {}", file.getId(), e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);
        return endResult(result, ignoreException);
    }

    public OpenCGAResult<File> update(String studyStr, String fileId, FileUpdateParams updateParams, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse FileUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileId", fileId)
                .append("updateParams", updateMap)
                .append("options", options)
                .append("token", token);

        OpenCGAResult<File> result = OpenCGAResult.empty();
        String fileUuid = "";
        try {
            OpenCGAResult<File> internalResult = internalGet(study.getUid(), fileId, EXCLUDE_FILE_ATTRIBUTES, userId);
            if (internalResult.getNumResults() == 0) {
                throw new CatalogException("File '" + fileId + "' not found");
            }
            File file = internalResult.first();

            // We set the proper values for the audit
            fileId = file.getId();
            fileUuid = file.getUuid();

            OpenCGAResult<File> updateResult = update(study, file, updateParams, options, userId, token);
            result.append(updateResult);

            auditManager.auditUpdate(userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
        } catch (CatalogException e) {
            Event event = new Event(Event.Type.ERROR, fileId, e.getMessage());
            result.getEvents().add(event);

            logger.error("Cannot update file {}: {}", fileId, e.getMessage());
            auditManager.auditUpdate(operationId, userId, Enums.Resource.FILE, fileId, fileUuid, study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
        return result;
    }

    /**
     * Update a File from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param fileIds   List of file ids. Could be either the id, path or uuid.
     * @param updateParams Data model filled only with the parameters to be updated.
     * @param options      QueryOptions object.
     * @param token  Session id of the user logged in.
     * @return A OpenCGAResult.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public OpenCGAResult<File> update(String studyStr, List<String> fileIds, FileUpdateParams updateParams, QueryOptions options,
                                      String token) throws CatalogException {
        return update(studyStr, fileIds, updateParams, false, options, token);
    }

    public OpenCGAResult<File> update(String studyStr, List<String> fileIds, FileUpdateParams updateParams, boolean ignoreException,
                                      QueryOptions options, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        ObjectMap updateMap;
        try {
            updateMap = updateParams != null ? updateParams.getUpdateMap() : null;
        } catch (JsonProcessingException e) {
            throw new CatalogException("Could not parse FileUpdateParams object: " + e.getMessage(), e);
        }

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileIds", fileIds)
                .append("updateParams", updateMap)
                .append("ignoreException", ignoreException)
                .append("options", options)
                .append("token", token);

        auditManager.initAuditBatch(operationId);
        OpenCGAResult<File> result = OpenCGAResult.empty();
        for (String id : fileIds) {
            String fileId = id;
            String fileUuid = "";

            try {
                OpenCGAResult<File> internalResult = internalGet(study.getUid(), fileId, EXCLUDE_FILE_ATTRIBUTES, userId);
                if (internalResult.getNumResults() == 0) {
                    throw new CatalogException("File '" + id + "' not found");
                }
                File file = internalResult.first();

                // We set the proper values for the audit
                fileId = file.getId();
                fileUuid = file.getUuid();

                OpenCGAResult<File> updateResult = update(study, file, updateParams, options, userId, token);
                result.append(updateResult);

                auditManager.auditUpdate(userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(), study.getUuid(),
                        auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            } catch (CatalogException e) {
                Event event = new Event(Event.Type.ERROR, id, e.getMessage());
                result.getEvents().add(event);

                logger.error("Cannot update file {}: {}", fileId, e.getMessage());
                auditManager.auditUpdate(operationId, userId, Enums.Resource.FILE, fileId, fileUuid, study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            }
        }
        auditManager.finishAuditBatch(operationId);
        return endResult(result, ignoreException);
    }

    private OpenCGAResult<File> update(Study study, File file, FileUpdateParams updateParams, QueryOptions options, String userId,
                                       String token) throws CatalogException {
        ObjectMap parameters = new ObjectMap();
        if (updateParams != null) {
            try {
                parameters = updateParams.getUpdateMap();
            } catch (JsonProcessingException e) {
                throw new CatalogException("Could not parse FileUpdateParams object: " + e.getMessage(), e);
            }
        }
        ParamUtils.checkUpdateParametersMap(parameters);

        options = ParamUtils.defaultObject(options, QueryOptions::new);

        if (parameters.containsKey(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
            Map<String, Object> actionMap = new HashMap<>(options.getMap(Constants.ACTIONS, Collections.emptyMap()));
            if (!actionMap.containsKey(AnnotationSetManager.ANNOTATION_SETS)
                    && !actionMap.containsKey(AnnotationSetManager.ANNOTATIONS)) {
                logger.warn("Assuming the user wants to add the list of annotation sets provided");
                actionMap.put(AnnotationSetManager.ANNOTATION_SETS, ParamUtils.UpdateAction.ADD);
                options.put(Constants.ACTIONS, actionMap);
            }
        }

        if (parameters.containsKey(FileDBAdaptor.QueryParams.RELATED_FILES.key())) {
            List<FileRelatedFile> relatedFileList = new ArrayList<>();
            for (SmallRelatedFileParams relatedFile : updateParams.getRelatedFiles()) {
                if (StringUtils.isEmpty(relatedFile.getFile()) || relatedFile.getRelation() == null) {
                    throw new CatalogException("Missing file or relation in relatedFiles list");
                }
                File relatedFileFile = internalGet(study.getUid(), relatedFile.getFile(), null, INCLUDE_FILE_URI_PATH, userId).first();
                relatedFileList.add(new FileRelatedFile(relatedFileFile, relatedFile.getRelation()));
            }
            parameters.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), relatedFileList);

            Map<String, Object> actionMap = options.getMap(Constants.ACTIONS, new HashMap<>());
            if (!actionMap.containsKey(FileDBAdaptor.QueryParams.RELATED_FILES.key())) {
                logger.warn("Assuming the user wants to add the list of related files provided");
                actionMap.put(FileDBAdaptor.QueryParams.RELATED_FILES.key(), ParamUtils.UpdateAction.ADD.name());
                options.put(Constants.ACTIONS, actionMap);
            }
        }

        // Check permissions...
        // Only check write annotation permissions if the user wants to update the annotation sets
        if (updateParams != null && updateParams.getAnnotationSets() != null) {
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId,
                    FileAclEntry.FilePermissions.WRITE_ANNOTATIONS);
        }
        // Only check update permissions if the user wants to update anything apart from the annotation sets
        if ((parameters.size() == 1 && !parameters.containsKey(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                || parameters.size() > 1) {
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);
        }

        if (isRootFolder(file)) {
            throw new CatalogException("Cannot modify root folder");
        }

        // We make a query to check both if the samples exists and if the user has permissions to see them
        if (updateParams != null && ListUtils.isNotEmpty(updateParams.getSampleIds())) {
            catalogManager.getSampleManager().internalGet(study.getUid(), updateParams.getSampleIds(), SampleManager.INCLUDE_SAMPLE_IDS,
                    userId, false);
        }

        //Name must be changed with "rename".
        if (updateParams != null && StringUtils.isNotEmpty(updateParams.getName())) {
            logger.info("Rename file using update method!");
            rename(study.getFqn(), file.getPath(), updateParams.getName(), token);
            parameters.remove(FileDBAdaptor.QueryParams.NAME.key());
        }

        checkUpdateAnnotations(study, file, parameters, options, VariableSet.AnnotableDataModels.FILE, fileDBAdaptor, userId);

        return fileDBAdaptor.update(file.getUid(), parameters, study.getVariableSets(), options);
    }

    @Deprecated
    public OpenCGAResult<File> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String token)
            throws CatalogException {
        ParamUtils.checkObj(parameters, "Parameters");
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId, StudyManager.INCLUDE_VARIABLE_SET);

        File file = internalGet(study.getUid(), entryStr, QueryOptions.empty(), userId).first();

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileId", entryStr)
                .append("updateParams", parameters)
                .append("options", options)
                .append("token", token);
        try {
            // Check permissions...
            // Only check write annotation permissions if the user wants to update the annotation sets
            if (parameters.containsKey(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key())) {
                authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId,
                        FileAclEntry.FilePermissions.WRITE_ANNOTATIONS);
            }
            // Only check update permissions if the user wants to update anything apart from the annotation sets
            if ((parameters.size() == 1 && !parameters.containsKey(FileDBAdaptor.QueryParams.ANNOTATION_SETS.key()))
                    || parameters.size() > 1) {
                authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);
            }

            try {
                ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> FileDBAdaptor.UpdateParams.getParam(a) != null);
            } catch (CatalogParameterException e) {
                throw new CatalogException("Could not update: " + e.getMessage(), e);
            }

            // We make a query to check both if the samples exists and if the user has permissions to see them
            if (parameters.get(FileDBAdaptor.QueryParams.SAMPLE_IDS.key()) != null
                    && ListUtils.isNotEmpty(parameters.getAsStringList(FileDBAdaptor.QueryParams.SAMPLE_IDS.key()))) {
                List<String> sampleIds = parameters.getAsStringList(FileDBAdaptor.QueryParams.SAMPLE_IDS.key());
                catalogManager.getSampleManager().internalGet(study.getUid(), sampleIds, SampleManager.INCLUDE_SAMPLE_IDS, userId, false);
            }

            //Name must be changed with "rename".
            if (parameters.containsKey(FileDBAdaptor.QueryParams.NAME.key())) {
                logger.info("Rename file using update method!");
                rename(studyStr, file.getPath(), parameters.getString(FileDBAdaptor.QueryParams.NAME.key()), token);
            }

            OpenCGAResult<File> queryResult = unsafeUpdate(study, file, parameters, options, userId);
            auditManager.auditUpdate(userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return queryResult;
        } catch (CatalogException e) {
            auditManager.auditUpdate(userId, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    OpenCGAResult<File> unsafeUpdate(Study study, File file, ObjectMap parameters, QueryOptions options, String userId)
            throws CatalogException {
        if (isRootFolder(file)) {
            throw new CatalogException("Cannot modify root folder");
        }

        try {
            ParamUtils.checkAllParametersExist(parameters.keySet().iterator(), (a) -> FileDBAdaptor.UpdateParams.getParam(a) != null);
        } catch (CatalogParameterException e) {
            throw new CatalogException("Could not update: " + e.getMessage(), e);
        }

        checkUpdateAnnotations(study, file, parameters, options, VariableSet.AnnotableDataModels.FILE, fileDBAdaptor, userId);

        fileDBAdaptor.update(file.getUid(), parameters, study.getVariableSets(), options);
        return fileDBAdaptor.get(file.getUid(), options);
    }

    public OpenCGAResult<File> link(String studyStr, FileLinkParams params, boolean parents, String token) throws CatalogException {
        // We make two attempts to link to ensure the behaviour remains even if it is being called at the same time link from different
        // threads
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("params", params)
                .append("parents", parents)
                .append("token", token);
        try {
            OpenCGAResult<File> result = privateLink(study, params, parents, token);
            auditManager.auditCreate(userId, Enums.Action.LINK, Enums.Resource.FILE, result.first().getId(),
                    result.first().getUuid(), study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return result;
        } catch (CatalogException e) {
            try {
                OpenCGAResult<File> result = privateLink(study, params, parents, token);
                auditManager.auditCreate(userId, Enums.Action.LINK, Enums.Resource.FILE, result.first().getId(),
                        result.first().getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
                return result;
            } catch (CatalogException e2) {
                auditManager.auditCreate(userId, Enums.Action.LINK, Enums.Resource.FILE, params.getUri(), "",
                        study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                new Error(0, "", e2.getMessage())));
                throw new CatalogException(e2.getMessage(), e2);
            }
        }
    }

    @Deprecated
    public OpenCGAResult<File> link(String studyStr, URI uriOrigin, String pathDestiny, ObjectMap params, String token)
            throws CatalogException {
        params = ParamUtils.defaultObject(params, ObjectMap::new);
        FileLinkParams linkParams = new FileLinkParams()
                .setDescription(params.getString("description", ""))
                .setPath(pathDestiny)
                .setUri(uriOrigin.toString())
                .setRelatedFiles(params.getAsList("relatedFiles", SmallRelatedFileParams.class));
        return link(studyStr, linkParams, params.getBoolean("parents", false), token);
    }

    @Override
    public OpenCGAResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        ParamUtils.checkObj(field, "field");
        ParamUtils.checkObj(sessionId, "sessionId");

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.VIEW_FILES);

        // TODO: In next release, we will have to check the count parameter from the queryOptions object.
        boolean count = true;
        query.append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        OpenCGAResult queryResult = null;
        if (count) {
            // We do not need to check for permissions when we show the count of files
            queryResult = fileDBAdaptor.rank(query, field, numResults, asc);
        }

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    @Override
    public OpenCGAResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException {
        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);
        if (fields == null || fields.size() == 0) {
            throw new CatalogException("Empty fields parameter.");
        }

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        fixQueryObject(study, query, userId);

        // Add study id to the query
        query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());

        // We do not need to check for permissions when we show the count of files
        OpenCGAResult queryResult = fileDBAdaptor.groupBy(query, fields, options, userId);

        return ParamUtils.defaultObject(queryResult, OpenCGAResult::new);
    }

    OpenCGAResult<File> rename(String studyStr, String fileStr, String newName, String sessionId) throws CatalogException {
        ParamUtils.checkFileName(newName, "name");

        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);

        File file = internalGet(study.getUid(), fileStr, EXCLUDE_FILE_ATTRIBUTES, userId).first();
        authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);

        if (file.getName().equals(newName)) {
            OpenCGAResult result = OpenCGAResult.empty();
            result.setEvents(Collections.singletonList(new Event(Event.Type.WARNING, newName, "File already had that name.")));
            return result;
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

        IOManager ioManager = null;
        try {
            ioManager = ioManagerFactory.get(file.getUri());
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(file.getUri(), e);
        }
        URI oldUri = file.getUri();
        URI newUri = Paths.get(oldUri).getParent().resolve(newName).toUri();
//        URI studyUri = file.getUri();
        boolean isExternal = file.isExternal(); //If the file URI is not null, the file is external located.

        switch (file.getType()) {
            case DIRECTORY:
                if (!isExternal) {  //Only rename non external files
                    // TODO? check if something in the subtree is not READY?
                    if (ioManager.exists(oldUri)) {
                        ioManager.rename(oldUri, newUri);   // io.move() 1
                    }
                }
                fileDBAdaptor.rename(file.getUid(), newPath, newUri.toString(), null);
                break;
            case FILE:
                if (!isExternal) {  //Only rename non external files
                    ioManager.rename(oldUri, newUri);
                }
                fileDBAdaptor.rename(file.getUid(), newPath, newUri.toString(), null);
                break;
            default:
                throw new CatalogException("Unknown file type " + file.getType());
        }

        return fileDBAdaptor.get(file.getUid(), QueryOptions.empty());
    }

    public OpenCGAResult<FileContent> grep(String studyId, String fileId, String pattern, boolean ignoreCase, int numLines, String token)
            throws CatalogException {
        long startTime = System.currentTimeMillis();

        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("fileId", fileId)
                .append("pattern", pattern)
                .append("ignoreCase", ignoreCase)
                .append("numLines", numLines)
                .append("token", token);
        try {
            File file = internalGet(study.getUid(), fileId, INCLUDE_FILE_URI, userId).first();
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.VIEW_CONTENT);

            URI fileUri = getUri(file);
            FileContent fileContent;
            try {
                fileContent = ioManagerFactory.get(fileUri).grep(Paths.get(fileUri), pattern, numLines, ignoreCase);
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(fileUri, e);
            }

            auditManager.audit(userId, Enums.Action.GREP, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), 1,
                    Collections.singletonList(fileContent), 1);
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.GREP, Enums.Resource.FILE, fileId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<FileContent> image(String studyStr, String fileId, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        long startTime = System.currentTimeMillis();

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileId", fileId)
                .append("token", token);
        File file;
        try {
            file = internalGet(study.getUid(), fileId, INCLUDE_FILE_URI_PATH, userId).first();
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.VIEW_CONTENT);

            if (file.getFormat() != File.Format.IMAGE) {
                throw new CatalogException("File '" + fileId + "' is not an image. Format of file is '" + file.getFormat() + "'.");
            }

            URI fileUri = getUri(file);
            FileContent fileContent;

            try {
                fileContent = ioManagerFactory.get(fileUri).base64Image(Paths.get(fileUri));
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(fileUri, e);
            }
            auditManager.audit(userId, Enums.Action.IMAGE_CONTENT, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), 1,
                    Collections.singletonList(fileContent), 1);
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.IMAGE_CONTENT, Enums.Resource.FILE, fileId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<FileContent> head(String studyStr, String fileId, long offset, int lines, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        long startTime = System.currentTimeMillis();

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileId", fileId)
                .append("offset", offset)
                .append("lines", lines)
                .append("token", token);
        File file;
        try {
            file = internalGet(study.getUid(), fileId, INCLUDE_FILE_URI, userId).first();
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.VIEW_CONTENT);
            URI fileUri = getUri(file);
            FileContent fileContent;
            try {
                fileContent = ioManagerFactory.get(fileUri).head(Paths.get(fileUri), offset, lines);
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(fileUri, e);
            }
            auditManager.audit(userId, Enums.Action.HEAD_CONTENT, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), 1,
                    Collections.singletonList(fileContent), 1);
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.HEAD_CONTENT, Enums.Resource.FILE, fileId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public OpenCGAResult<FileContent> tail(String studyStr, String fileId, int lines, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        long startTime = System.currentTimeMillis();

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileId", fileId)
                .append("lines", lines)
                .append("token", token);
        File file;
        try {
            file = internalGet(study.getUid(), fileId, INCLUDE_FILE_URI, userId).first();
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.VIEW_CONTENT);
            URI fileUri = getUri(file);
            FileContent fileContent;
            try {
                fileContent = ioManagerFactory.get(fileUri).tail(Paths.get(fileUri), lines);
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(fileUri, e);
            }
            auditManager.audit(userId, Enums.Action.TAIL_CONTENT, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

            return new OpenCGAResult<>((int) (System.currentTimeMillis() - startTime), Collections.emptyList(), 1,
                    Collections.singletonList(fileContent), 1);
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.TAIL_CONTENT, Enums.Resource.FILE, fileId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public DataInputStream download(String studyStr, String fileId, String token) throws CatalogException {
        return download(studyStr, fileId, -1, -1, token);
    }

    public DataInputStream download(String studyStr, String fileId, int start, int limit, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        ObjectMap auditParams = new ObjectMap()
                .append("study", studyStr)
                .append("fileId", fileId)
                .append("start", start)
                .append("limit", limit)
                .append("token", token);
        File file;
        try {
            file = internalGet(study.getUid(), fileId, INCLUDE_FILE_URI, userId).first();
            authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.DOWNLOAD);
            URI fileUri = getUri(file);
            DataInputStream dataInputStream;
            try {
                dataInputStream = ioManagerFactory.get(fileUri).getFileObject(fileUri, start, limit);
            } catch (IOException e) {
                throw CatalogIOException.ioManagerException(fileUri, e);
            }

            auditManager.audit(userId, Enums.Action.DOWNLOAD, Enums.Resource.FILE, file.getId(), file.getUuid(), study.getId(),
                    study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));
            return dataInputStream;
        } catch (CatalogException e) {
            auditManager.audit(userId, Enums.Action.DOWNLOAD, Enums.Resource.FILE, fileId, "", study.getId(), study.getUuid(),
                    auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()));
            throw e;
        }
    }

    public void setFileIndex(String studyStr, String fileId, FileIndex index, String sessionId) throws CatalogException {
        String userId = userManager.getUserId(sessionId);
        Study study = studyManager.resolveId(studyStr, userId);
        long fileUid = internalGet(study.getUid(), fileId, INCLUDE_FILE_IDS, userId).first().getUid();

        authorizationManager.checkFilePermission(study.getUid(), fileUid, userId, FileAclEntry.FilePermissions.WRITE);

        ObjectMap parameters = new ObjectMap(FileDBAdaptor.QueryParams.INTERNAL_INDEX.key(), index);
        fileDBAdaptor.update(fileUid, parameters, QueryOptions.empty());
    }

    // **************************   ACLs  ******************************** //
    public OpenCGAResult<Map<String, List<String>>> getAcls(String studyId, List<String> fileList, String member, boolean ignoreException,
                                                            String token) throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);
        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("fileList", fileList)
                .append("member", member)
                .append("ignoreException", ignoreException)
                .append("token", token);
        try {
            OpenCGAResult<Map<String, List<String>>> fileAclList = OpenCGAResult.empty();
            InternalGetDataResult<File> fileDataResult = internalGet(study.getUid(), fileList, INCLUDE_FILE_IDS, user, ignoreException);

            Map<String, InternalGetDataResult.Missing> missingMap = new HashMap<>();
            if (fileDataResult.getMissing() != null) {
                missingMap = fileDataResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetDataResult.Missing::getId, Function.identity()));
            }
            int counter = 0;
            for (String fileId : fileList) {
                if (!missingMap.containsKey(fileId)) {
                    File file = fileDataResult.getResults().get(counter);
                    try {
                        OpenCGAResult<Map<String, List<String>>> allFileAcls;
                        if (StringUtils.isNotEmpty(member)) {
                            allFileAcls = authorizationManager.getFileAcl(study.getUid(), file.getUid(), user, member);
                        } else {
                            allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), file.getUid(), user, true);
                        }
                        fileAclList.append(allFileAcls);

                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FILE, file.getId(),
                                file.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
                    } catch (CatalogException e) {
                        auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FILE, file.getId(),
                                file.getUuid(), study.getId(), study.getUuid(), auditParams,
                                new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()), new ObjectMap());

                        if (!ignoreException) {
                            throw e;
                        } else {
                            Event event = new Event(Event.Type.ERROR, fileId, missingMap.get(fileId).getErrorMsg());
                            fileAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                                    Collections.singletonList(Collections.emptyMap()), 0));
                        }
                    }
                    counter += 1;
                } else {
                    Event event = new Event(Event.Type.ERROR, fileId, missingMap.get(fileId).getErrorMsg());
                    fileAclList.append(new OpenCGAResult<>(0, Collections.singletonList(event), 0,
                            Collections.singletonList(Collections.emptyMap()), 0));

                    auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FILE, fileId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    new Error(0, "", missingMap.get(fileId).getErrorMsg())), new ObjectMap());
                }
            }
            return fileAclList;
        } catch (CatalogException e) {
            for (String fileId : fileList) {
                auditManager.audit(operationId, user, Enums.Action.FETCH_ACLS, Enums.Resource.FILE, fileId, "", study.getId(),
                        study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR, e.getError()),
                        new ObjectMap());
            }
            throw e;
        }
    }

    public OpenCGAResult<Map<String, List<String>>> updateAcl(String studyId, List<String> fileStrList, String memberList,
                                                              FileAclParams aclParams, ParamUtils.AclAction action, String token)
            throws CatalogException {
        String user = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyId, user);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("fileStrList", fileStrList)
                .append("memberList", memberList)
                .append("aclParams", aclParams)
                .append("action", action)
                .append("token", token);
        String operationId = UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.AUDIT);

        try {
            int count = 0;
            count += fileStrList != null && !fileStrList.isEmpty() ? 1 : 0;
            count += StringUtils.isNotEmpty(aclParams.getSample()) ? 1 : 0;

            if (count > 1) {
                throw new CatalogException("Update ACL: Only one of these parameters are allowed: file or sample per query.");
            } else if (count == 0) {
                throw new CatalogException("Update ACL: At least one of these parameters should be provided: file or sample");
            }

            if (action == null) {
                throw new CatalogException("Invalid action found. Please choose a valid action to be performed.");
            }

            List<String> permissions = Collections.emptyList();
            if (StringUtils.isNotEmpty(aclParams.getPermissions())) {
                permissions = Arrays.asList(aclParams.getPermissions().trim().replaceAll("\\s", "").split(","));
                checkPermissions(permissions, FileAclEntry.FilePermissions::valueOf);
            }


            List<File> extendedFileList;
            if (StringUtils.isNotEmpty(aclParams.getSample())) {
                // Obtain the sample ids
                OpenCGAResult<Sample> sampleDataResult = catalogManager.getSampleManager().internalGet(study.getUid(),
                        Arrays.asList(StringUtils.split(aclParams.getSample(), ",")), SampleManager.INCLUDE_SAMPLE_IDS, user, false);
                Query query = new Query(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(),
                        sampleDataResult.getResults().stream().map(Sample::getId).collect(Collectors.toList()));

                extendedFileList = catalogManager.getFileManager().search(studyId, query, EXCLUDE_FILE_ATTRIBUTES, token).getResults();
            } else {
                extendedFileList = internalGet(study.getUid(), fileStrList, EXCLUDE_FILE_ATTRIBUTES, user, false).getResults();
            }

            authorizationManager.checkCanAssignOrSeePermissions(study.getUid(), user);

            // Increase the list with the files/folders within the list of ids that correspond with folders
            extendedFileList = getRecursiveFilesAndFolders(study.getUid(), extendedFileList);

            // Validate that the members are actually valid members
            List<String> members;
            if (memberList != null && !memberList.isEmpty()) {
                members = Arrays.asList(memberList.split(","));
            } else {
                members = Collections.emptyList();
            }
            authorizationManager.checkNotAssigningPermissionsToAdminsGroup(members);
            checkMembers(study.getUid(), members);

            List<Long> fileUids = extendedFileList.stream().map(File::getUid).collect(Collectors.toList());
            AuthorizationManager.CatalogAclParams catalogAclParams = new AuthorizationManager.CatalogAclParams(fileUids, permissions,
                    Enums.Resource.FILE);
//        studyManager.membersHavePermissionsInStudy(resourceIds.getStudyId(), members);

            OpenCGAResult<Map<String, List<String>>> queryResultList;
            switch (action) {
                case SET:
                    queryResultList = authorizationManager.setAcls(study.getUid(), members, catalogAclParams);
                    break;
                case ADD:
                    queryResultList = authorizationManager.addAcls(study.getUid(), members, catalogAclParams);
                    break;
                case REMOVE:
                    queryResultList = authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                case RESET:
                    catalogAclParams.setPermissions(null);
                    queryResultList = authorizationManager.removeAcls(members, catalogAclParams);
                    break;
                default:
                    throw new CatalogException("Unexpected error occurred. No valid action found.");
            }
            for (File file : extendedFileList) {
                auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.FILE, file.getId(),
                        file.getUuid(), study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS), new ObjectMap());
            }
            return queryResultList;
        } catch (CatalogException e) {
            if (fileStrList != null) {
                for (String fileId : fileStrList) {
                    auditManager.audit(operationId, user, Enums.Action.UPDATE_ACLS, Enums.Resource.FILE, fileId, "",
                            study.getId(), study.getUuid(), auditParams, new AuditRecord.Status(AuditRecord.Status.Result.ERROR,
                                    e.getError()), new ObjectMap());
                }
            }
            throw e;
        }
    }

    public OpenCGAResult<File> getParents(String studyStr, String path, boolean rootFirst, QueryOptions options, String token)
            throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);

        List<String> paths = calculateAllPossiblePaths(path);

        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), paths);
        query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid());
        OpenCGAResult<File> result = fileDBAdaptor.get(study.getUid(), query, options, userId);
        result.getResults().sort(rootFirst ? ROOT_FIRST_COMPARATOR : ROOT_LAST_COMPARATOR);
        return result;
    }


    // **************************   Private methods   ******************************** //
    private boolean isRootFolder(File file) throws CatalogException {
        ParamUtils.checkObj(file, "File");
        return file.getPath().isEmpty();
    }

    /**
     * Fetch all the recursive files and folders within the list of file ids given.
     *
     * @param studyUid Study uid.
     * @param fileList List of files
     * @return a more complete file list containing all the nested files
     */
    private List<File> getRecursiveFilesAndFolders(long studyUid, List<File> fileList) throws CatalogException {
        List<File> fileListCopy = new LinkedList<>();
        fileListCopy.addAll(fileList);

        Set<Long> uidFileSet = new HashSet<>();
        uidFileSet.addAll(fileList.stream().map(File::getUid).collect(Collectors.toSet()));

        List<String> pathList = new ArrayList<>();
        for (File file : fileList) {
            if (file.getType().equals(File.Type.DIRECTORY)) {
                pathList.add("~^" + file.getPath());
            }
        }

        if (CollectionUtils.isNotEmpty(pathList)) {
            // Search for all the files within the list of paths
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                    .append(FileDBAdaptor.QueryParams.PATH.key(), pathList);
            OpenCGAResult<File> fileDataResult1 = fileDBAdaptor.get(query, INCLUDE_FILE_URI_PATH);
            for (File file1 : fileDataResult1.getResults()) {
                if (!uidFileSet.contains(file1.getUid())) {
                    uidFileSet.add(file1.getUid());
                    fileListCopy.add(file1);
                }
            }
        }

        return fileListCopy;
    }

    private List<String> calculateAllPossiblePaths(String filePath) {
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

    //FIXME: This should use org.opencb.opencga.storage.core.variant.io.VariantReaderUtils
    private String getMainVariantFile(String name) {
        if (name.endsWith(".variants.avro.gz") || name.endsWith(".variants.proto.gz") || name.endsWith(".variants.json.gz")) {
            int idx = name.lastIndexOf(".variants.");
            return name.substring(0, idx);
        } else {
            return null;
        }
    }

    private boolean isTransformedFile(String name) {
        return getMainVariantFile(name) != null;
    }

    private String getVariantMetadataFile(String path) {
        String file = getMainVariantFile(path);
        if (file != null) {
            return file + ".file.json.gz";
        } else {
            return null;
        }
    }

    private OpenCGAResult<File> getParents(long studyUid, String filePath, boolean rootFirst, QueryOptions options)
            throws CatalogException {
        List<String> paths = calculateAllPossiblePaths(filePath);

        Query query = new Query(FileDBAdaptor.QueryParams.PATH.key(), paths);
        query.put(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyUid);
        OpenCGAResult<File> result = fileDBAdaptor.get(query, options);
        result.getResults().sort(rootFirst ? ROOT_FIRST_COMPARATOR : ROOT_LAST_COMPARATOR);
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
        File existingParent = getParents(studyId, path, false, null).first();

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

        String studyFilePath = Paths.get(studyUri).resolve(catalogFilePath).toString();
        String originalFilePath = Paths.get(fileUri).toString();

        logger.debug("Study file path: {}", studyFilePath);
        logger.debug("File path: {}", originalFilePath);
        return !studyFilePath.equals(originalFilePath);
    }

    /**
     * Method to check if a file or folder can be deleted. It will check for indexation, status, permissions and file system availability.
     *
     * @param studyStr       Study.
     * @param fileId         File or folder id.
     * @param unlink         Boolean indicating whether the operation only expects to remove the entry from the database or also remove
     *                       the file from disk.
     * @param token          Token of the user for which DELETE permissions will be checked.
     * @throws CatalogException if any of the files cannot be deleted.
     */
    public void checkCanDeleteFile(String studyStr, String fileId, boolean unlink, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        Study study = studyManager.resolveId(studyStr, userId);
        checkCanDeleteFile(study, fileId, unlink, Arrays.asList(FileStatus.READY, FileStatus.TRASHED), userId);
    }

    /**
     * Method to check if a file or folder can be deleted. It will check for indexation, status, permissions and file system availability.
     *
     * @param study          Study.
     * @param fileId         File or folder id.
     * @param unlink         Boolean indicating whether the operation only expects to remove the entry from the database or also remove
     *                       the file from disk.
     * @param acceptedStatus List of valid statuses the file should have. For the public, the file should be in READY or TRASHED status.
     *                       However, if someone calls to the delete/unlink methods, the status of those files should already be in
     *                       PENDING_DELETE.
     * @param userId         user for which DELETE permissions will be checked.
     * @throws CatalogException if any of the files cannot be deleted.
     */
    private void checkCanDeleteFile(Study study, String fileId, boolean unlink, List<String> acceptedStatus, String userId)
            throws CatalogException {

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(FileDBAdaptor.QueryParams.UID.key(),
                FileDBAdaptor.QueryParams.NAME.key(), FileDBAdaptor.QueryParams.TYPE.key(), FileDBAdaptor.QueryParams.RELATED_FILES.key(),
                FileDBAdaptor.QueryParams.SIZE.key(), FileDBAdaptor.QueryParams.URI.key(), FileDBAdaptor.QueryParams.PATH.key(),
                FileDBAdaptor.QueryParams.INTERNAL_INDEX.key(), FileDBAdaptor.QueryParams.INTERNAL_STATUS.key(),
                FileDBAdaptor.QueryParams.EXTERNAL.key()));

        OpenCGAResult<File> fileOpenCGAResult = internalGet(study.getUid(), fileId, options, userId);
        if (fileOpenCGAResult.getNumResults() == 0) {
            throw new CatalogException("File " + fileId + " not found");
        }
        File file = fileOpenCGAResult.first();

        // If the user is the owner or the admin, we won't check if he has permissions for every single file
        boolean checkPermissions = !authorizationManager.isOwnerOrAdmin(study.getUid(), userId);

        Set<Long> indexFiles = new HashSet<>();

        if (unlink && !file.isExternal()) {
            throw new CatalogException("Cannot unlink non-external files. Use delete operation instead");
        } else if (!unlink && file.isExternal()) {
            throw new CatalogException("Cannot delete external files. Use unlink operation instead");
        }

        IOManager ioManager;
        try {
            ioManager = ioManagerFactory.get(file.getUri());
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(file.getUri(), e);
        }
        if (file.getType() == File.Type.FILE) {
            if (checkPermissions) {
                authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.WRITE);
                authorizationManager.checkFilePermission(study.getUid(), file.getUid(), userId, FileAclEntry.FilePermissions.DELETE);
            }

            // File must exist in the file system
            if (!unlink && !ioManager.exists(file.getUri())) {
                throw new CatalogException("File " + file.getUri() + " not found in file system");
            }

            checkValidStatusForDeletion(file, acceptedStatus);
            indexFiles.addAll(getProducedFromIndexFiles(file));
        } else {
            // We cannot delete the root folder
            if (isRootFolder(file)) {
                throw new CatalogException("Root directories cannot be deleted");
            }

            // Query to get all recursive files and folders
            Query query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), "~^" + file.getPath() + "*");

            if (unlink) {
                // Only external files/folders are allowed within the folder
                Query tmpQuery = new Query(query)
                        .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), false);
                long numMatches = fileDBAdaptor.count(tmpQuery).getNumMatches();

                if (numMatches > 0) {
                    throw new CatalogException(numMatches + " local files detected within the external "
                            + "folder " + file.getPath() + ". Please, delete those folders or files manually first");
                }
            } else {
                // Only non-external files/folders are allowed within the folder
                Query tmpQuery = new Query(query)
                        .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);
                long numMatches = fileDBAdaptor.count(tmpQuery).getNumMatches();

                if (numMatches > 0) {
                    throw new CatalogException(numMatches + " external files detected within the local "
                            + "folder " + file.getPath() + ". Please, unlink those folders or files manually first");
                }
            }

            DBIterator<File> iterator = fileDBAdaptor.iterator(query, options);
            while (iterator.hasNext()) {
                File tmpFile = iterator.next();

                if (checkPermissions) {
                    authorizationManager.checkFilePermission(study.getUid(), tmpFile.getUid(), userId, FileAclEntry.FilePermissions.DELETE);
                    authorizationManager.checkFilePermission(study.getUid(), tmpFile.getUid(), userId, FileAclEntry.FilePermissions.WRITE);
                }

                // File must exist in the file system
                if (!unlink && !ioManager.exists(tmpFile.getUri())) {
                    throw new CatalogException("File " + tmpFile.getUri() + " not found in file system");
                }

                checkValidStatusForDeletion(tmpFile, acceptedStatus);
                indexFiles.addAll(getProducedFromIndexFiles(tmpFile));
            }

            // TODO: Validate no file/folder within any registered directory is not registered in OpenCGA
        }


        // Check the original files are not being indexed at the moment
        if (!indexFiles.isEmpty()) {
            Query query = new Query(FileDBAdaptor.QueryParams.UID.key(), new ArrayList<>(indexFiles));
            try (DBIterator<File> iterator = fileDBAdaptor.iterator(query, new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FileDBAdaptor.QueryParams.INTERNAL_INDEX.key(), FileDBAdaptor.QueryParams.UID.key())))) {
                while (iterator.hasNext()) {
                    File next = iterator.next();
                    String status = next.getInternal().getIndex().getStatus().getName();
                    switch (status) {
                        case FileIndex.IndexStatus.READY:
                            // If they are already ready, we only need to remove the reference to the transformed files as they will be
                            // removed
                            next.getInternal().getIndex().setTransformedFile(null);
                            break;
                        case FileIndex.IndexStatus.TRANSFORMED:
                            // We need to remove the reference to the transformed files and change their status from TRANSFORMED to NONE
                            next.getInternal().getIndex().setTransformedFile(null);
                            next.getInternal().getIndex().getStatus().setName(FileIndex.IndexStatus.NONE);
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
    }

    Set<Long> getProducedFromIndexFiles(File fileAux) {
        // Check if the file is produced from other file being indexed and add them to the transformedFromFileIds set
        if (fileAux.getRelatedFiles() != null && !fileAux.getRelatedFiles().isEmpty()) {
            return fileAux.getRelatedFiles().stream()
                    .filter(myFile -> myFile.getRelation() == FileRelatedFile.Relation.PRODUCED_FROM)
                    .map(FileRelatedFile::getFile)
                    .map(File::getUid)
                    .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    void checkValidStatusForDeletion(File file, List<String> expectedStatus) throws CatalogException {
        if (file.getInternal().getStatus() == null) {
            throw new CatalogException("Cannot check file status for deletion");
        }
        for (String status : expectedStatus) {
            if (status.equals(file.getInternal().getStatus().getName())) {
                // Valid status
                return;
            }
        }
        throw new CatalogException("Cannot delete file: " + file.getName() + ". The status is " + file.getInternal().getStatus().getName());
    }

    public DataResult<FacetField> facet(String studyId, Query query, QueryOptions options, boolean defaultStats, String token)
            throws CatalogException, IOException {
        ParamUtils.defaultObject(query, Query::new);
        ParamUtils.defaultObject(options, QueryOptions::new);

        String userId = userManager.getUserId(token);
        // We need to add variableSets and groups to avoid additional queries as it will be used in the catalogSolrManager
        Study study = studyManager.resolveId(studyId, userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(StudyDBAdaptor.QueryParams.VARIABLE_SET.key(), StudyDBAdaptor.QueryParams.GROUPS.key())));

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("query", new Query(query))
                .append("options", options)
                .append("defaultStats", defaultStats)
                .append("token", token);
        try {
            if (defaultStats || StringUtils.isEmpty(options.getString(QueryOptions.FACET))) {
                String facet = options.getString(QueryOptions.FACET);
                options.put(QueryOptions.FACET, StringUtils.isNotEmpty(facet) ? defaultFacet + ";" + facet : defaultFacet);
            }

            AnnotationUtils.fixQueryAnnotationSearch(study, userId, query, authorizationManager);

            try (CatalogSolrManager catalogSolrManager = new CatalogSolrManager(catalogManager)) {
                DataResult<FacetField> result = catalogSolrManager.facetedQuery(study, CatalogSolrManager.FILE_SOLR_COLLECTION, query,
                        options, userId);
                auditManager.auditFacet(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                        new AuditRecord.Status(AuditRecord.Status.Result.SUCCESS));

                return result;
            }
        } catch (CatalogException e) {
            auditManager.auditFacet(userId, Enums.Resource.FILE, study.getId(), study.getUuid(), auditParams,
                    new AuditRecord.Status(AuditRecord.Status.Result.ERROR, new Error(0, "", e.getMessage())));
            throw e;
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

        logger.debug("Path: {}", stringPath);

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

        if (fileDBAdaptor.count(query).getNumMatches() == 0) {
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
        OpenCGAResult<Map<String, List<String>>> allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), parentFileId, userId,
                checkPermissions);

        URI completeURI = Paths.get(studyURI).resolve(path).toUri();

        // Create the folder in catalog
        File folder = new File(path.getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN, File.Bioformat.NONE, completeURI,
                stringPath, null, TimeUtils.getTime(), TimeUtils.getTime(), "", false, 0, null, new FileExperiment(),
                Collections.emptyList(), Collections.emptyList(), "", studyManager.getCurrentRelease(study), Collections.emptyList(), null,
                new CustomStatus(), FileInternal.initialize(), null);
        folder.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
        checkHooks(folder, study.getFqn(), HookConfiguration.Stage.CREATE);
        fileDBAdaptor.insert(study.getUid(), folder, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
                new QueryOptions());
        OpenCGAResult<File> queryResult = getFile(study.getUid(), folder.getUuid(), QueryOptions.empty());
        // Propagate ACLs
        if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(study.getUid(), Arrays.asList(queryResult.first().getUid()), allFileAcls.getResults().get(0),
                    Enums.Resource.FILE);
        }
    }

    private OpenCGAResult<File> privateLink(Study study, FileLinkParams params, boolean parents, String token)
            throws CatalogException {
        ParamUtils.checkObj(params, "FileLinkParams");
        ParamUtils.checkParameter(params.getUri(), "uri");
        URI uriOrigin;
        try {
            uriOrigin = UriUtils.createUri(params.getUri());
        } catch (URISyntaxException e) {
            throw new CatalogException(e.getMessage(), e);
        }

        IOManager ioManager;
        try {
            ioManager = ioManagerFactory.get(uriOrigin);
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(uriOrigin, e);
        }
        if (!ioManager.exists(uriOrigin)) {
            throw new CatalogIOException("File " + uriOrigin + " does not exist");
        }

        final URI normalizedUri;
        try {
            normalizedUri = UriUtils.createUri(uriOrigin.normalize().getPath());
        } catch (URISyntaxException e) {
            throw new CatalogException(e);
        }

        String userId = userManager.getUserId(token);
        authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_FILES);

        params.setPath(ParamUtils.defaultString(params.getPath(), ""));
        if (params.getPath().length() == 1 && (params.getPath().equals(".") || params.getPath().equals("/"))) {
            params.setPath("");
        } else {
            if (params.getPath().startsWith("/")) {
                params.setPath(params.getPath().substring(1));
            }
            if (!params.getPath().isEmpty() && !params.getPath().endsWith("/")) {
                params.setPath(params.getPath() + "/");
            }
        }
        String externalPathDestinyStr;
        if (Paths.get(normalizedUri).toFile().isDirectory()) {
            externalPathDestinyStr = Paths.get(params.getPath()).resolve(Paths.get(normalizedUri).getFileName()).toString() + "/";
        } else {
            externalPathDestinyStr = Paths.get(params.getPath()).resolve(Paths.get(normalizedUri).getFileName()).toString();
        }

        // Check if the path already exists and is not external
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), false);
        if (fileDBAdaptor.count(query).getNumMatches() > 0) {
            throw new CatalogException("Cannot link to " + externalPathDestinyStr + ". The path already existed and is not external.");
        }

        // Check if the uri was already linked to that same path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), normalizedUri)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.PATH.key(), externalPathDestinyStr)
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);


        if (fileDBAdaptor.count(query).getNumMatches() > 0) {
            // Create a regular expression on URI to return everything linked from that URI
            query.put(FileDBAdaptor.QueryParams.URI.key(), "~^" + normalizedUri);
            query.remove(FileDBAdaptor.QueryParams.PATH.key());

            // Limit the number of results and only some fields
            QueryOptions queryOptions = new QueryOptions()
                    .append(QueryOptions.LIMIT, 100);

            return fileDBAdaptor.get(query, queryOptions)
                    .addEvent(new Event(Event.Type.INFO, ParamConstants.FILE_ALREADY_LINKED));
        }

        // Check if the uri was linked to other path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), normalizedUri)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);
        if (fileDBAdaptor.count(query).getNumMatches() > 0) {
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.PATH.key());
            String path = fileDBAdaptor.get(query, queryOptions).first().getPath();
            throw new CatalogException(normalizedUri + " was already linked to other path: " + path);
        }

//         FIXME: Implement resync
//        boolean resync = params.getBoolean("resync", false);
//        String checksum = params.getString(FileDBAdaptor.QueryParams.CHECKSUM.key(), "");

        final List<FileRelatedFile> relatedFiles = params.getRelatedFiles();
        if (relatedFiles != null) {
            for (FileRelatedFile relatedFile : relatedFiles) {
                File tmpFile = internalGet(study.getUid(), relatedFile.getFile().getId(), INCLUDE_FILE_URI_PATH, userId).first();
                relatedFile.setFile(tmpFile);
            }
        }

        // Because pathDestiny can be null, we will use catalogPath as the virtual destiny where the files will be located in catalog.
        Path catalogPath = Paths.get(params.getPath());

        if (params.getPath().isEmpty()) {
            // If no destiny is given, everything will be linked to the root folder of the study.
            authorizationManager.checkStudyPermission(study.getUid(), userId, StudyAclEntry.StudyPermissions.WRITE_FILES);
        } else {
            // Check if the folder exists
            query = new Query()
                    .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                    .append(FileDBAdaptor.QueryParams.PATH.key(), params.getPath());
            if (fileDBAdaptor.count(query).getNumMatches() == 0) {
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
                long fileId = fileDBAdaptor.getId(study.getUid(), params.getPath());
                authorizationManager.checkFilePermission(study.getUid(), fileId, userId, FileAclEntry.FilePermissions.WRITE);
            }
        }

        // This list will contain the list of transformed files detected during the link
        List<File> transformedFiles = new ArrayList<>();

        // We remove the / at the end for replacement purposes in the walkFileTree
        if (externalPathDestinyStr.endsWith("/")) {
            externalPathDestinyStr = externalPathDestinyStr.substring(0, externalPathDestinyStr.length() - 1);
        }
        String finalExternalPathDestinyStr = externalPathDestinyStr;

        // Link all the files and folders present in the uri
        ioManager.walkFileTree(normalizedUri, new SimpleFileVisitor<URI>() {
            @Override
            public FileVisitResult preVisitDirectory(URI dir, BasicFileAttributes attrs) throws IOException {
                try {
                    String destinyPath = Paths.get(dir).toString().replace(Paths.get(normalizedUri).toString(),
                            finalExternalPathDestinyStr);

                    if (!destinyPath.isEmpty() && !destinyPath.endsWith("/")) {
                        destinyPath += "/";
                    }

                    if (destinyPath.startsWith("/")) {
                        destinyPath = destinyPath.substring(1);
                    }

                    Query query = new Query()
                            .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(FileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                    if (fileDBAdaptor.count(query).getNumMatches() == 0) {
                        // If the folder does not exist, we create it

                        String parentPath = getParentPath(destinyPath);
                        long parentFileId = fileDBAdaptor.getId(study.getUid(), parentPath);
                        // We obtain the permissions set in the parent folder and set them to the file or folder being created
                        OpenCGAResult<Map<String, List<String>>> allFileAcls;
                        try {
                            allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), parentFileId, userId, true);
                        } catch (CatalogException e) {
                            throw new RuntimeException(e);
                        }

                        File folder = new File(Paths.get(dir).getFileName().toString(), File.Type.DIRECTORY, File.Format.PLAIN,
                                File.Bioformat.NONE, dir, destinyPath, null, TimeUtils.getTime(),
                                TimeUtils.getTime(), params.getDescription(), true, 0, new Software(), new FileExperiment(),
                                Collections.emptyList(), relatedFiles, "", studyManager.getCurrentRelease(study), Collections.emptyList(),
                                Collections.emptyMap(),
                                params.getStatus() != null ? params.getStatus().toCustomStatus() : new CustomStatus(),
                                FileInternal.initialize(), Collections.emptyMap());
                        folder.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
                        checkHooks(folder, study.getFqn(), HookConfiguration.Stage.CREATE);
                        fileDBAdaptor.insert(study.getUid(), folder, Collections.emptyList(), Collections.emptyList(),
                                Collections.emptyList(), new QueryOptions());
                        OpenCGAResult<File> queryResult = getFile(study.getUid(), folder.getUuid(), QueryOptions.empty());

                        // Propagate ACLs
                        if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                            authorizationManager.replicateAcls(study.getUid(), Arrays.asList(queryResult.first().getUid()),
                                    allFileAcls.getResults().get(0), Enums.Resource.FILE);
                        }
                    }

                } catch (CatalogException e) {
                    logger.error("An error occurred when trying to create folder {}", dir.toString());
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(URI fileUri, BasicFileAttributes attrs) throws IOException {
                try {
                    String destinyPath = Paths.get(fileUri).toString().replace(Paths.get(normalizedUri).toString(),
                            finalExternalPathDestinyStr);

                    if (destinyPath.startsWith("/")) {
                        destinyPath = destinyPath.substring(1);
                    }

                    Query query = new Query()
                            .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                            .append(FileDBAdaptor.QueryParams.PATH.key(), destinyPath);

                    if (fileDBAdaptor.count(query).getNumMatches() == 0) {
                        long size = ioManager.getFileSize(fileUri);
                        // If the file does not exist, we create it
                        String parentPath = getParentPath(destinyPath);
                        long parentFileId = fileDBAdaptor.getId(study.getUid(), parentPath);
                        // We obtain the permissions set in the parent folder and set them to the file or folder being created
                        OpenCGAResult<Map<String, List<String>>> allFileAcls;
                        try {
                            allFileAcls = authorizationManager.getAllFileAcls(study.getUid(), parentFileId, userId, true);
                        } catch (CatalogException e) {
                            throw new RuntimeException(e);
                        }

                        FileInternal internal = FileInternal.initialize();
                        if (params.getInternal() != null) {
                            internal.setSampleMap(params.getInternal().getSampleMap());
                        }

                        File subfile = new File(Paths.get(fileUri).getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                                File.Bioformat.NONE, fileUri, destinyPath, null, TimeUtils.getTime(),
                                TimeUtils.getTime(), params.getDescription(), true, size, new Software(), new FileExperiment(),
                                Collections.emptyList(), relatedFiles, "", studyManager.getCurrentRelease(study), Collections.emptyList(),
                                Collections.emptyMap(),
                                params.getStatus() != null ? params.getStatus().toCustomStatus() : new CustomStatus(), internal,
                                new HashMap<>());
                        subfile.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
                        checkHooks(subfile, study.getFqn(), HookConfiguration.Stage.CREATE);

                        // Improve metadata information and extract samples if any
                        new FileMetadataReader(catalogManager).addMetadataInformation(study.getFqn(), subfile);

                        List<Sample> existingSamples = new LinkedList<>();
                        List<Sample> nonExistingSamples = new LinkedList<>();
                        validateNewSamples(study, subfile, existingSamples, nonExistingSamples, token);

                        fileDBAdaptor.insert(study.getUid(), subfile, existingSamples, nonExistingSamples, Collections.emptyList(),
                                new QueryOptions());
                        subfile = getFile(study.getUid(), subfile.getUuid(), QueryOptions.empty()).first();

                        // Propagate ACLs
                        if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
                            authorizationManager.replicateAcls(study.getUid(), Arrays.asList(subfile.getUid()),
                                    allFileAcls.getResults().get(0), Enums.Resource.FILE);
                        }

                        if (isTransformedFile(subfile.getName())) {
                            logger.info("Detected transformed file {}", subfile.getPath());
                            transformedFiles.add(subfile);
                        }
                    } else {
                        throw new CatalogException("Cannot link the file " + Paths.get(fileUri).getFileName().toString()
                                + ". There is already a file in the path " + destinyPath + " with the same name.");
                    }

                } catch (CatalogException e) {
                    logger.error(e.getMessage());
                }

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(URI file, IOException exc) throws IOException {
                return FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult postVisitDirectory(URI dir, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }
        });

        // Try to link transformed files with their corresponding original files if any
        try {
            if (transformedFiles.size() > 0) {
                matchUpVariantFiles(study.getFqn(), transformedFiles, token);
            }
        } catch (CatalogException e) {
            logger.warn("Matching avro to variant file: {}", e.getMessage());
        }

        // Check if the uri was already linked to that same path
        query = new Query()
                .append(FileDBAdaptor.QueryParams.URI.key(), "~^" + normalizedUri)
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), study.getUid())
                .append(FileDBAdaptor.QueryParams.EXTERNAL.key(), true);

        // Limit the number of results and only some fields
        QueryOptions queryOptions = new QueryOptions()
                .append(QueryOptions.LIMIT, 100);
        return fileDBAdaptor.get(query, queryOptions);
    }

    OpenCGAResult<File> registerFile(Study study, String filePath, URI fileUri, String jobId, String token) throws CatalogException {
        String userId = userManager.getUserId(token);
        IOManager ioManager;
        try {
            ioManager = ioManagerFactory.get(fileUri);
        } catch (IOException e) {
            throw CatalogIOException.ioManagerException(fileUri, e);
        }

        // The file is not registered in Catalog, so we will register it
        long size = ioManager.getFileSize(fileUri);

        String parentPath = getParentPath(filePath);
        File parentFile = internalGet(study.getUid(), parentPath, INCLUDE_FILE_URI_PATH, userId).first();
        // We obtain the permissions set in the parent folder and set them to the file or folder being created
        OpenCGAResult<Map<String, List<String>>> allFileAcls = authorizationManager.getAllFileAcls(study.getUid(),
                parentFile.getUid(), userId, true);

        File subfile = new File(Paths.get(filePath).getFileName().toString(), File.Type.FILE, File.Format.UNKNOWN,
                File.Bioformat.NONE, fileUri, filePath, "", TimeUtils.getTime(), TimeUtils.getTime(),
                "", isExternal(study, filePath, fileUri), size, new Software(), new FileExperiment(), Collections.emptyList(),
                Collections.emptyList(), jobId, studyManager.getCurrentRelease(study), Collections.emptyList(), Collections.emptyMap(),
                new CustomStatus(), FileInternal.initialize(), Collections.emptyMap());
        subfile.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.FILE));
        checkHooks(subfile, study.getFqn(), HookConfiguration.Stage.CREATE);

        // Improve metadata information and extract samples if any
        new FileMetadataReader(catalogManager).addMetadataInformation(study.getFqn(), subfile);
        List<Sample> existingSamples = new LinkedList<>();
        List<Sample> nonExistingSamples = new LinkedList<>();
        validateNewSamples(study, subfile, existingSamples, nonExistingSamples, token);

        fileDBAdaptor.insert(study.getUid(), subfile, existingSamples, nonExistingSamples, Collections.emptyList(), new QueryOptions());
        OpenCGAResult<File> result = getFile(study.getUid(), subfile.getUuid(), QueryOptions.empty());
        subfile = result.first();

        // Propagate ACLs
        if (allFileAcls != null && allFileAcls.getNumResults() > 0) {
            authorizationManager.replicateAcls(study.getUid(), Arrays.asList(subfile.getUid()), allFileAcls.getResults().get(0),
                    Enums.Resource.FILE);
        }

        // If it is a transformed file, we will try to link it with the correspondent original file
        try {
            if (isTransformedFile(subfile.getName())) {
                matchUpVariantFiles(study.getFqn(), Arrays.asList(subfile), token);
            }
        } catch (CatalogException e1) {
            logger.warn("Matching avro to variant file: {}", e1.getMessage());
        }

        return result;
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

    private CheckPath checkPathExists(String path, long studyId) throws CatalogException {
        String myPath = path;
        if (myPath.endsWith("/")) {
            myPath = myPath.substring(0, myPath.length() - 1);
        }

        // We first look for any file called the same way the directory needs to be called
        Query query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), myPath);
        OpenCGAResult<Long> fileDataResult = fileDBAdaptor.count(query);
        if (fileDataResult.getNumMatches() > 0) {
            return CheckPath.FILE_EXISTS;
        }

        query = new Query()
                .append(FileDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(FileDBAdaptor.QueryParams.PATH.key(), myPath + "/");
        fileDataResult = fileDBAdaptor.count(query);

        return fileDataResult.getNumMatches() > 0 ? CheckPath.DIRECTORY_EXISTS : CheckPath.FREE_PATH;
    }
}
