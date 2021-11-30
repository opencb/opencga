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

package org.opencb.opencga.app.cli.main.executors.catalog;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.io.TextOutputWriter;
import org.opencb.opencga.app.cli.main.options.FileCommandOptions;
import org.opencb.opencga.app.cli.main.options.FileCommandOptions.GrepCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AnnotationCommandOptions;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.models.file.*;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.RestResponse;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 03/06/16.
 */
public class FileCommandExecutor extends OpencgaCommandExecutor {

    private FileCommandOptions filesCommandOptions;

    public FileCommandExecutor(FileCommandOptions filesCommandOptions) {
        super(filesCommandOptions.commonCommandOptions);
        this.filesCommandOptions = filesCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing files command line");

        String subCommandString = getParsedSubCommand(filesCommandOptions.jCommander);
        RestResponse queryResponse = null;
        switch (subCommandString) {
//            case "copy":
//                queryResponse = copy();
//                break;
            case "create":
                queryResponse = create();
                break;
            case "info":
                queryResponse = info();
                break;
            case "download":
                queryResponse = download();
                break;
            case "grep":
                queryResponse = grep();
                break;
            case "search":
                queryResponse = search();
                break;
            case "list":
                queryResponse = list();
                break;
            case "tree":
                queryResponse = tree();
                break;
//            case "index":
//                queryResponse = index();
//                break;
            case "head":
                queryResponse = head();
                break;
            case "tail":
                queryResponse = tail();
                break;
            case "stats":
                queryResponse = stats();
                break;
            case "fetch":
                queryResponse = fetch();
                break;
            case "update":
                queryResponse = update();
                break;
            case "upload":
                queryResponse = upload();
                break;
            case "delete":
                queryResponse = delete();
                break;
            case "link":
                queryResponse = link();
                break;
            case "link-run":
                queryResponse = linkRun();
                break;
            case "post-link-run":
                queryResponse = postLinkRun();
                break;
            case "unlink":
                queryResponse = unlink();
                break;
            case "refresh":
                queryResponse = refresh();
                break;
            case "annotation-sets-update":
                queryResponse = updateAnnotations();
                break;
//            case "variants":
//                queryResponse = variants();
//                break;
            case "acl":
                queryResponse = acl();
                break;
            case "acl-update":
                queryResponse = updateAcl();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

        if (!queryResponse.getResponses().isEmpty()) {
            createOutput(queryResponse);
        }
    }

    private RestResponse<File> create() throws ClientException {
        logger.debug("Creating a new folder");

        FileCommandOptions.CreateCommandOptions commandOptions = filesCommandOptions.createCommandOptions;

        FileCreateParamsOld createParams = new FileCreateParamsOld()
                .setDirectory(StringUtils.isEmpty(commandOptions.content))
                .setParents(commandOptions.parents)
                .setContent(commandOptions.content)
                .setDescription(commandOptions.description)
                .setPath(commandOptions.folder);

        ObjectMap params = new ObjectMap(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);

        return openCGAClient.getFileClient().create(createParams, params);
    }


    private RestResponse<File> info() throws ClientException {
        logger.debug("Getting file information");

        FileCommandOptions.InfoCommandOptions commandOptions = filesCommandOptions.infoCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(ParamConstants.FLATTEN_ANNOTATIONS, commandOptions.flattenAnnotations);
        params.put(ParamConstants.DELETED_PARAM, commandOptions.deleted);

        return openCGAClient.getFileClient().info(commandOptions.files, params);
    }

    private RestResponse download() throws ClientException {
        logger.debug("Downloading file");

        FileCommandOptions.DownloadCommandOptions commandOptions = filesCommandOptions.downloadCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.put("OPENCGA_DESTINY", commandOptions.fileDestiny);

        return openCGAClient.getFileClient().download(commandOptions.file, params);
    }

    private RestResponse<FileContent> grep() throws ClientException {
        logger.debug("Grep command: File content");

        GrepCommandOptions commandOptions = filesCommandOptions.grepCommandOptions;

        ObjectMap params = new ObjectMap();
        params.put("ignoreCase", commandOptions.ignoreCase);
        params.putIfNotNull("maxCount", commandOptions.maxCount);
        params.putIfNotNull("pattern", commandOptions.pattern);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        return openCGAClient.getFileClient().grep(commandOptions.file, params);
    }

    private RestResponse<File> search() throws ClientException {
        logger.debug("Searching files");

        FileCommandOptions.SearchCommandOptions commandOptions = filesCommandOptions.searchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.NAME.key(), commandOptions.name);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.PATH.key(), commandOptions.path);
        params.putIfNotNull(FileDBAdaptor.QueryParams.TYPE.key(), commandOptions.type);
        params.putIfNotNull(FileDBAdaptor.QueryParams.BIOFORMAT.key(), StringUtils.join(commandOptions.bioformat, ","));
        params.putIfNotNull(FileDBAdaptor.QueryParams.FORMAT.key(), StringUtils.join(commandOptions.format, ","));
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.INTERNAL_STATUS.key(), commandOptions.status);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.INTERNAL_VARIANT_INDEX_STATUS_ID.key(), commandOptions.internalIndexStatus);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.DIRECTORY.key(), commandOptions.folder);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.CREATION_DATE.key(), commandOptions.creationDate);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.MODIFICATION_DATE.key(), commandOptions.modificationDate);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.DESCRIPTION.key(), commandOptions.description);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.TAGS.key(), commandOptions.tags);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.SIZE.key(), commandOptions.size);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.SAMPLE_IDS.key(), commandOptions.samples);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.JOB_ID.key(), commandOptions.jobId);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.ATTRIBUTES.key(), commandOptions.attributes);
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.ANNOTATION.key(), commandOptions.annotation);
        params.putIfNotNull(FileDBAdaptor.QueryParams.RELEASE.key(), commandOptions.release);
        params.put(ParamConstants.FLATTEN_ANNOTATIONS, commandOptions.flattenAnnotations);
        params.put(ParamConstants.DELETED_PARAM, commandOptions.deleted);
        params.putIfNotEmpty(ParamConstants.ACL_PARAM, commandOptions.acl);
        params.putAll(commandOptions.commonOptions.params);

        params.put(QueryOptions.COUNT, commandOptions.numericOptions.count);
        params.putIfNotEmpty(QueryOptions.INCLUDE, commandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, commandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, commandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, commandOptions.numericOptions.skip);

        return openCGAClient.getFileClient().search(params);
    }

    private RestResponse<File> list() throws ClientException {
        logger.debug("Listing files in folder");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), filesCommandOptions.listCommandOptions.study);
        params.putIfNotEmpty(QueryOptions.INCLUDE, filesCommandOptions.listCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, filesCommandOptions.listCommandOptions.dataModelOptions.exclude);
        params.put(QueryOptions.LIMIT, filesCommandOptions.listCommandOptions.numericOptions.limit);
        params.put(QueryOptions.SKIP, filesCommandOptions.listCommandOptions.numericOptions.skip);
        params.put("count", filesCommandOptions.listCommandOptions.numericOptions.count);

        String folder = ".";
        if (StringUtils.isNotEmpty(filesCommandOptions.listCommandOptions.folderId)) {
            folder = filesCommandOptions.listCommandOptions.folderId;
        }
        return openCGAClient.getFileClient().list(folder, params);
    }

    private RestResponse<FileTree> tree() throws ClientException {
        logger.debug("Obtain a tree view of the files and folders within a folder");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(FileDBAdaptor.QueryParams.STUDY.key(), filesCommandOptions.treeCommandOptions.study);
        params.putIfNotNull("maxDepth", filesCommandOptions.treeCommandOptions.maxDepth);
        params.putIfNotEmpty(QueryOptions.INCLUDE, filesCommandOptions.treeCommandOptions.dataModelOptions.include);
        params.putIfNotEmpty(QueryOptions.EXCLUDE, filesCommandOptions.treeCommandOptions.dataModelOptions.exclude);
        if (writer instanceof TextOutputWriter
                && StringUtils.isEmpty(filesCommandOptions.treeCommandOptions.dataModelOptions.include)
                && StringUtils.isEmpty(filesCommandOptions.treeCommandOptions.dataModelOptions.exclude)) {
            params.put(QueryOptions.INCLUDE, "id,name,path,type,size,internal,status");
        }
        return openCGAClient.getFileClient().tree(filesCommandOptions.treeCommandOptions.folderId, params);
    }

    private RestResponse<FileContent> head() throws ClientException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull(FileDBAdaptor.QueryParams.STUDY.key(), filesCommandOptions.headCommandOptions.study);
        objectMap.putIfNotNull("lines", filesCommandOptions.headCommandOptions.lines);
        objectMap.put("offset", filesCommandOptions.headCommandOptions.offset);
        RestResponse<FileContent> head = openCGAClient.getFileClient().head(filesCommandOptions.headCommandOptions.file, objectMap);
        System.out.println(head.firstResult().getContent());
        return new RestResponse<>();
    }

    private RestResponse<FileContent> tail() throws ClientException {
        ObjectMap objectMap = new ObjectMap();
        objectMap.putIfNotNull(FileDBAdaptor.QueryParams.STUDY.key(), filesCommandOptions.tailCommandOptions.study);
        objectMap.putIfNotNull("lines", filesCommandOptions.tailCommandOptions.lines);
        RestResponse<FileContent> tail = openCGAClient.getFileClient().tail(filesCommandOptions.tailCommandOptions.file, objectMap);
        System.out.println(tail.first().first().getContent());
        return new RestResponse<>();
    }

    private RestResponse<File> update() throws ClientException {
        logger.debug("updating file");

        FileCommandOptions.UpdateCommandOptions commandOptions = filesCommandOptions.updateCommandOptions;

        FileUpdateParams updateParams = new FileUpdateParams()
                .setName(commandOptions.name)
                .setFormat(commandOptions.format)
                .setBioformat(commandOptions.bioformat)
                .setDescription(commandOptions.description)
                .setTags(commandOptions.tags)
                .setSampleIds(commandOptions.sampleIds)
                .setSoftware(new Software(
                        commandOptions.softwareName,
                        commandOptions.softwareVersion,
                        commandOptions.softwareRepository,
                        commandOptions.softwareCommit,
                        commandOptions.softwareWebsite,
                        commandOptions.softwareParams.isEmpty() ? null : commandOptions.softwareParams
                ));

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotNull("samplesAction", commandOptions.samplesAction);
        params.putIfNotNull("tagsAction", commandOptions.tagsAction);

        return openCGAClient.getFileClient().update(commandOptions.file, updateParams, params);
    }

    private RestResponse<File> upload() throws ClientException {
        logger.debug("uploading file");

        FileCommandOptions.UploadCommandOptions commandOptions = filesCommandOptions.uploadCommandOptions;

        ObjectMap params = new ObjectMap()
                .append("fileFormat", ParamUtils.defaultString(commandOptions.fileFormat, File.Format.UNKNOWN.toString()))
                .append("bioformat", ParamUtils.defaultString(commandOptions.bioformat, File.Bioformat.UNKNOWN.toString()))
                .append("parents", commandOptions.parents);

        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty("relativeFilePath", commandOptions.catalogPath);
        params.putIfNotEmpty("description", commandOptions.description);
        params.putIfNotEmpty("fileName", commandOptions.fileName);
        params.putIfNotEmpty("file", commandOptions.inputFile);

        return openCGAClient.getFileClient().upload(params);
    }

    private RestResponse<Job> delete() throws ClientException {
        logger.debug("Deleting file");

        FileCommandOptions.DeleteCommandOptions commandOptions = filesCommandOptions.deleteCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.put("skipTrash", commandOptions.skipTrash);

        return openCGAClient.getFileClient().delete(commandOptions.file, params);
    }

    private RestResponse<File> link() throws ClientException, URISyntaxException {
        logger.debug("Linking the file or folder into catalog.");

        FileCommandOptions.LinkCommandOptions commandOptions = filesCommandOptions.linkCommandOptions;

        FileLinkParams linkParams = new FileLinkParams()
                .setDescription(commandOptions.description)
                .setPath(commandOptions.path);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.put("parents", commandOptions.parents);

        List<File> results = new ArrayList<>(commandOptions.inputs.size());

        for (String input : commandOptions.inputs) {
            linkParams.setUri(UriUtils.createUri(input).toString());
            results.addAll(openCGAClient.getFileClient().link(linkParams, params).allResults());
        }

        return new RestResponse<>(params, Collections.singletonList(new OpenCGAResult<>(-1, Collections.emptyList(), results.size(),
                results, results.size())));
    }

    private RestResponse<Job> linkRun() throws ClientException {
        FileCommandOptions.LinkRunCommandOptions commandOptions = filesCommandOptions.linkRunCommandOptions;

        ObjectMap params = getCommonParams(commandOptions.study, filesCommandOptions.commonCommandOptions.params);
        addJobParams(commandOptions.jobOptions, params);

        FileLinkToolParams data = new FileLinkToolParams(
                commandOptions.inputs,
                commandOptions.path,
                commandOptions.description,
                commandOptions.parents);

        return openCGAClient.getFileClient().runLink(data, params);
    }

    private RestResponse<Job> postLinkRun() throws ClientException {
        FileCommandOptions.PostLinkRunCommandOptions commandOptions = filesCommandOptions.postLinkRunCommandOptions;

        ObjectMap params = getCommonParams(commandOptions.study, filesCommandOptions.commonCommandOptions.params);
        addJobParams(commandOptions.jobOptions, params);

        PostLinkToolParams data = new PostLinkToolParams(commandOptions.files, commandOptions.batchSize);

        return openCGAClient.getFileClient().runPostlink(data, params);
    }

//    private RestResponse relink() throws CatalogException, IOException {
//        logger.debug("Change file location. Provided file must be either STAGED or an external file. [DEPRECATED]");
//
//        QueryOptions queryOptions = new QueryOptions();
//        queryOptions.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), filesCommandOptions.relinkCommandOptions.study);
//        queryOptions.put("calculateChecksum", filesCommandOptions.relinkCommandOptions.calculateChecksum);
//        return  openCGAClient.getFileClient().relink(filesCommandOptions.relinkCommandOptions.file,
//                filesCommandOptions.relinkCommandOptions.uri, queryOptions);
//    }

    private RestResponse<Job> unlink() throws ClientException {
        logger.debug("Unlink an external file from catalog");

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), filesCommandOptions.unlinkCommandOptions.study);
        return openCGAClient.getFileClient().unlink(filesCommandOptions.unlinkCommandOptions.file, params);
    }

    private RestResponse refresh() throws ClientException {
        logger.debug("Refreshing metadata from the selected file or folder. Print updated files.");

        ObjectMap params = new ObjectMap();
        params.putIfNotNull(FileDBAdaptor.QueryParams.STUDY.key(), filesCommandOptions.refreshCommandOptions.study);

        return openCGAClient.getFileClient().refresh(filesCommandOptions.refreshCommandOptions.file, params);
    }

    private RestResponse<ObjectMap> updateAcl() throws ClientException, CatalogException {
        FileCommandOptions.FileAclCommandOptions.AclsUpdateCommandOptions commandOptions = filesCommandOptions.aclsUpdateCommandOptions;

        FileAclUpdateParams updateParams = new FileAclUpdateParams()
                .setFile(extractIdsFromListOrFile(commandOptions.id))
                .setSample(extractIdsFromListOrFile(commandOptions.sample))
                .setPermissions(commandOptions.permissions);

        ObjectMap params = new ObjectMap();
        params.putIfNotNull("study", commandOptions.study);

        return openCGAClient.getFileClient().updateAcl(commandOptions.memberId, commandOptions.action.name(), updateParams, params);
    }

    private RestResponse<FacetField> stats() throws ClientException {
        logger.debug("File aggregation stats");

        FileCommandOptions.StatsCommandOptions commandOptions = filesCommandOptions.statsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty(FileDBAdaptor.QueryParams.STUDY.key(), commandOptions.study);
        params.putIfNotEmpty("creationYear", commandOptions.creationYear);
        params.putIfNotEmpty("creationMonth", commandOptions.creationMonth);
        params.putIfNotEmpty("creationDay", commandOptions.creationDay);
        params.putIfNotEmpty("creationDayOfWeek", commandOptions.creationDayOfWeek);
        params.putIfNotEmpty("name", commandOptions.name);
        params.putIfNotEmpty("type", commandOptions.type);
        params.putIfNotEmpty("format", commandOptions.format);
        params.putIfNotEmpty("bioformat", commandOptions.bioformat);
        params.putIfNotEmpty("status", commandOptions.status);
        params.putIfNotEmpty("numSamples", commandOptions.numSamples);
        params.putIfNotEmpty("numRelatedFiles", commandOptions.numRelatedFiles);
        params.putIfNotEmpty("release", commandOptions.release);
        params.putIfNotEmpty("size", commandOptions.size);
        params.putIfNotEmpty("software", commandOptions.software);
        params.putIfNotEmpty("experiment", commandOptions.experiment);
        params.putIfNotNull("external", commandOptions.external);
        params.putIfNotEmpty(Constants.ANNOTATION, commandOptions.annotation);
        params.put("default", commandOptions.defaultStats);
        params.putIfNotNull("field", commandOptions.field);

        return openCGAClient.getFileClient().aggregationStats(params);
    }

    private RestResponse<Job> fetch() throws ClientException {
        FileCommandOptions.FetchCommandOptions commandOptions = filesCommandOptions.fetchCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        addJobParams(commandOptions.jobOptions, params);

        FileFetch data = new FileFetch(commandOptions.url, commandOptions.path);

        return openCGAClient.getFileClient().fetch(data, params);
    }

    private RestResponse<File> updateAnnotations() throws ClientException, IOException {
        AnnotationCommandOptions.AnnotationSetsUpdateCommandOptions commandOptions = filesCommandOptions.annotationUpdateCommandOptions;

        ObjectMapper mapper = new ObjectMapper();
        ObjectMap annotations = mapper.readValue(new java.io.File(commandOptions.annotations), ObjectMap.class);

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
//        queryParams.putIfNotNull("action", updateCommandOptions.action);

        return openCGAClient.getFileClient().updateAnnotations(commandOptions.id, commandOptions.annotationSetId, annotations, params);
    }

    private RestResponse<ObjectMap> acl() throws ClientException {
        AclCommandOptions.AclsCommandOptions commandOptions = filesCommandOptions.aclsCommandOptions;

        ObjectMap params = new ObjectMap();
        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("member", commandOptions.memberId);

        params.putAll(commandOptions.commonOptions.params);

        return openCGAClient.getFileClient().acl(commandOptions.id, params);
    }

}
