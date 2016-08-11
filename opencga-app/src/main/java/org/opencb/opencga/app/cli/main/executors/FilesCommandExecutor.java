/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.app.cli.main.executors;


import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.executors.commons.AclCommandExecutor;
import org.opencb.opencga.app.cli.main.options.FileCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Created by imedina on 03/06/16.
 */
public class FilesCommandExecutor extends OpencgaCommandExecutor {

    private FileCommandOptions filesCommandOptions;
    private AclCommandExecutor<File, FileAclEntry> aclCommandExecutor;

    public FilesCommandExecutor(FileCommandOptions filesCommandOptions) {
        super(filesCommandOptions.commonCommandOptions);
        this.filesCommandOptions = filesCommandOptions;
        this.aclCommandExecutor = new AclCommandExecutor<>();
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing files command line");

        String subCommandString = getParsedSubCommand(filesCommandOptions.jCommander);
        switch (subCommandString) {
            case "copy":
                createOutput(copy());
                break;
            case "create-folder":
                createOutput(createFolder());
                break;
            case "info":
                createOutput(info());
                break;
            case "download":
                createOutput(download());
                break;
            case "grep":
                createOutput(grep());
                break;
            case "search":
                createOutput(search());
                break;
            case "list":
                createOutput(list());
                break;
            case "index":
                createOutput(index());
                break;
            case "alignment":
                createOutput(alignment());
                break;
            case "fetch":
                createOutput(fetch());
                break;
            case "update":
                createOutput(update());
                break;
            case "upload":
                createOutput(upload());
                break;
            case "delete":
                createOutput(delete());
                break;
            case "link":
                createOutput(link());
                break;
            case "relink":
                createOutput(relink());
                break;
            case "unlink":
                createOutput(unlink());
                break;
            case "refresh":
                createOutput(refresh());
                break;
            case "group-by":
                createOutput(groupBy());
                break;
            case "acl":
                createOutput(aclCommandExecutor.acls(filesCommandOptions.aclsCommandOptions, openCGAClient.getFileClient()));
                break;
            case "acl-create":
                createOutput(aclCommandExecutor.aclsCreate(filesCommandOptions.aclsCreateCommandOptions, openCGAClient.getFileClient()));
                break;
            case "acl-member-delete":
                createOutput(aclCommandExecutor.aclMemberDelete(filesCommandOptions.aclsMemberDeleteCommandOptions,
                        openCGAClient.getFileClient()));
                break;
            case "acl-member-info":
                createOutput(aclCommandExecutor.aclMemberInfo(filesCommandOptions.aclsMemberInfoCommandOptions,
                        openCGAClient.getFileClient()));
                break;
            case "acl-member-update":
                createOutput(aclCommandExecutor.aclMemberUpdate(filesCommandOptions.aclsMemberUpdateCommandOptions,
                        openCGAClient.getFileClient()));
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private QueryResponse<File> copy() throws CatalogException {
        logger.debug("Creating a new file");
        //openCGAClient.getFileClient(). /******************* Falta el create en FileClient.java ?? **//
//        OptionsParser.FileCommands.CreateCommand c = optionsParser.getFileCommands().createCommand;
        FileCommandOptions.CopyCommandOptions copyCommandOptions = filesCommandOptions.copyCommandOptions;
        long studyId = catalogManager.getStudyId(copyCommandOptions.studyId);
        Path inputFile = Paths.get(copyCommandOptions.inputFile);
        URI sourceUri;
        try {
            sourceUri = new URI(null, copyCommandOptions.inputFile, null);
        } catch (URISyntaxException e) {
            throw new CatalogException("Input file is not a proper URI");
        }
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = inputFile.toUri();
        }
        if (!catalogManager.getCatalogIOManagerFactory().get(sourceUri).exists(sourceUri)) {
            throw new CatalogException("File " + sourceUri + " does not exist");
        }

        QueryResult<File> file = catalogManager.createFile(studyId, copyCommandOptions.format, copyCommandOptions.bioformat,
                Paths.get(copyCommandOptions.path, inputFile.getFileName().toString()).toString(), copyCommandOptions.description,
                copyCommandOptions.parents, -1, sessionId);
        new CatalogFileUtils(catalogManager).upload(sourceUri, file.first(), null, sessionId, false, false,
                copyCommandOptions.move, copyCommandOptions.calculateChecksum);
        FileMetadataReader.get(catalogManager).setMetadataInformation(file.first(), null, new QueryOptions(), sessionId, false);
        return new QueryResponse<>(new QueryOptions(), Arrays.asList(file));
    }

    private QueryResponse createFolder() throws CatalogException {
        logger.debug("Creating a new folder");
        ObjectMap o = new ObjectMap();
       // QueryResponse<File> create = openCGAClient.getFileClient().createFolder(filesCommandOptions.infoCommandOptions.id,
          //      filesCommandOptions.infoCommandOptions.path,o);
        System.out.println("PENDING!!.");
        return null;
    }


    private QueryResponse<File> info() throws CatalogException, IOException {
        logger.debug("Getting file information");
        return openCGAClient.getFileClient().get(filesCommandOptions.infoCommandOptions.id, null);
    }

    private QueryResponse download() throws CatalogException {
        logger.debug("Downloading file");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse grep() throws CatalogException {
        logger.debug("Grep command: File content");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse search() throws CatalogException {
        logger.debug("Searching files");
        System.out.println("PENDING!!.");
        return null;
        //QueryResponse<File> listfiles = openCGAClient.getFileClient().search(querry, queryoptions);
        //TODO
    }

    private QueryResponse<File> list() throws CatalogException, IOException {
        logger.debug("Listing files in folder");
        return openCGAClient.getFileClient().getFiles(filesCommandOptions.listCommandOptions.id, null);
    }

    private QueryResponse index() throws CatalogException {
        logger.debug("Indexing file in the selected StorageEngine");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse alignment() throws CatalogException {
        logger.debug("Fetch alignments from a BAM file");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse fetch() throws CatalogException {
        logger.debug("File Fetch");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse share() throws CatalogException {
        logger.debug("Sharing a file");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse unshare() throws CatalogException {
        logger.debug("Unsharing a file");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse update() throws CatalogException {
        logger.debug("updating file");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse<File> upload() throws CatalogException, IOException {
        logger.debug("uploading file");

        ObjectMap objectMap = new ObjectMap()
                .append("fileFormat", filesCommandOptions.uploadCommandOptions.fileFormat)
                .append("bioFormat", filesCommandOptions.uploadCommandOptions.bioFormat)
                .append("parents", filesCommandOptions.uploadCommandOptions.parents);

        if (filesCommandOptions.uploadCommandOptions.catalogPath != null) {
            objectMap.append("relativeFilePath", filesCommandOptions.uploadCommandOptions.catalogPath);
        }

        if (filesCommandOptions.uploadCommandOptions.description != null) {
            objectMap.append("description", filesCommandOptions.uploadCommandOptions.description);
        }

        if (filesCommandOptions.uploadCommandOptions.fileName != null) {
            objectMap.append("fileName", filesCommandOptions.uploadCommandOptions.fileName);
        }

        return openCGAClient.getFileClient().upload(filesCommandOptions.uploadCommandOptions.studyId,
                filesCommandOptions.uploadCommandOptions.inputFile, objectMap);
    }

    private QueryResponse<File> delete() throws CatalogException, IOException {
        logger.debug("Deleting file");

        ObjectMap objectMap = new ObjectMap()
                .append("deleteExternal", filesCommandOptions.deleteCommandOptions.deleteExternal)
                .append("skipTrash", filesCommandOptions.deleteCommandOptions.skipTrash);

        return openCGAClient.getFileClient().delete(filesCommandOptions.deleteCommandOptions.id, objectMap);
    }

    private QueryResponse<File> link() throws CatalogException, IOException, URISyntaxException {
        logger.debug("Linking the file or folder into catalog.");

        URI uri = UriUtils.createUri(filesCommandOptions.linkCommandOptions.input);
        logger.debug("uri: {}", uri.toString());

        ObjectMap objectMap = new ObjectMap()
                .append(CatalogFileDBAdaptor.QueryParams.DESCRIPTION.key(), filesCommandOptions.linkCommandOptions.description)
                .append("parents", filesCommandOptions.linkCommandOptions.parents);

        CatalogManager catalogManager = null;
        try {
            catalogManager = new CatalogManager(catalogConfiguration);
        } catch (CatalogException e) {
            logger.error("Catalog manager could not be initialized. Is the configuration OK?");
        }
        if (!catalogManager.existsCatalogDB()) {
            logger.error("The database could not be found. Are you running this from the server?");
            return null;
        }
        QueryResult<File> linkQueryResult = catalogManager.link(uri, filesCommandOptions.linkCommandOptions.path,
                filesCommandOptions.linkCommandOptions.studyId, objectMap, sessionId);

        return new QueryResponse<>(new QueryOptions(), Arrays.asList(linkQueryResult));
    }

    private QueryResponse relink() throws CatalogException {
        logger.debug("Change file location. Provided file must be either STAGED or an external file");
        System.out.println("PENDING!!.");
        return null;
        //TODO
    }

    private QueryResponse<File> unlink() throws CatalogException, IOException {
        logger.debug("Unlink an external file from catalog");

        // LOCAL EXECUTION
//        CatalogManager catalogManager = null;
//        try {
//            catalogManager = new CatalogManager(catalogConfiguration);
//        } catch (CatalogException e) {
//            logger.error("Catalog manager could not be initialized. Is the configuration OK?");
//        }
//        if (!catalogManager.existsCatalogDB()) {
//            logger.error("The database could not be found. Are you running this from the server?");
//            return;
//        }
//        QueryResult<File> unlinkQueryResult = catalogManager.unlink(filesCommandOptions.unlinkCommandOptions.id, new QueryOptions(),
//                sessionId);
//
//        QueryResponse<File> unlink = new QueryResponse<>(new QueryOptions(), Arrays.asList(unlinkQueryResult));

        return openCGAClient.getFileClient().unlink(filesCommandOptions.unlinkCommandOptions.id, new ObjectMap());
    }

    private QueryResponse refresh() throws CatalogException {
        logger.debug("Refreshing metadata from the selected file or folder. Print updated files.");
        System.out.println("PENDING!!.");
        return null;
    }


    private QueryResponse groupBy() throws CatalogException {
        logger.debug("Grouping files by several fields");
        System.out.println("PENDING!!.");
        return null;
        //TODO

    }


}
