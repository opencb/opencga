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
import org.opencb.opencga.catalog.utils.FileMetadataReader;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.FileCommandOptions;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.managers.CatalogFileUtils;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.storage.core.exceptions.StorageManagerException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by imedina on 03/06/16.
 */
public class FilesCommandExecutor extends OpencgaCommandExecutor {

    private FileCommandOptions filesCommandOptions;

    public FilesCommandExecutor(FileCommandOptions filesCommandOptions) {
        super(filesCommandOptions.commonCommandOptions);
        this.filesCommandOptions = filesCommandOptions;
    }


    @Override
    public void execute() throws Exception {
        logger.debug("Executing files command line");

        String subCommandString = getParsedSubCommand(filesCommandOptions.jCommander);
        switch (subCommandString) {
            case "copy":
                copy();
                break;
            case "create-folder":
                createFolder();
                break;
            case "info":
                info();
                break;
            case "download":
                download();
                break;
            case "grep":
                grep();
                break;
            case "search":
                search();
                break;
            case "list":
                list();
                break;
            case "index":
                index();
                break;
            case "alignament":
                alignament();
                break;
            case "fetch":
                fetch();
                break;
            case "update":
                update();
                break;
            case "upload":
                upload();
                break;
            case "delete":
                delete();
                break;
            case "link":
                link();
                break;
            case "relink":
                relink();
                break;
            case "unlink":
                unlink();
                break;
            case "refresh":
                refresh();
                break;
            case "group-by":
                groupBy();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void copy() throws CatalogException, IOException, URISyntaxException, StorageManagerException {
        logger.debug("Creating a new file");
        //openCGAClient.getFileClient(). /******************* Falta el create en FileClient.java ?? **//
//        OptionsParser.FileCommands.CreateCommand c = optionsParser.getFileCommands().createCommand;
        FileCommandOptions.CopyCommandOptions copyCommandOptions = filesCommandOptions.copyCommandOptions;
        long studyId = catalogManager.getStudyId(copyCommandOptions.studyId);
        Path inputFile = Paths.get(copyCommandOptions.inputFile);
        URI sourceUri = new URI(null, copyCommandOptions.inputFile, null);
        if (sourceUri.getScheme() == null || sourceUri.getScheme().isEmpty()) {
            sourceUri = inputFile.toUri();
        }
        if (!catalogManager.getCatalogIOManagerFactory().get(sourceUri).exists(sourceUri)) {
            throw new IOException("File " + sourceUri + " does not exist");
        }

        QueryResult<File> file = catalogManager.createFile(studyId, copyCommandOptions.format, copyCommandOptions.bioformat,
                Paths.get(copyCommandOptions.path, inputFile.getFileName().toString()).toString(), copyCommandOptions.description,
                copyCommandOptions.parents, -1, sessionId);
        new CatalogFileUtils(catalogManager).upload(sourceUri, file.first(), null, sessionId, false, false,
                copyCommandOptions.move, copyCommandOptions.calculateChecksum);
        FileMetadataReader.get(catalogManager).setMetadataInformation(file.first(), null, new QueryOptions(), sessionId, false);
    }

    private void createFolder() throws CatalogException {
        logger.debug("Creating a new folder");
        ObjectMap o = new ObjectMap();
       // QueryResponse<File> create = openCGAClient.getFileClient().createFolder(filesCommandOptions.infoCommandOptions.id,
          //      filesCommandOptions.infoCommandOptions.path,o);
        System.out.println("Created.");
        //TODO

    }


    private void info() throws CatalogException, IOException {
        logger.debug("Getting file information");
        QueryResponse<File> info = openCGAClient.getFileClient().get(filesCommandOptions.infoCommandOptions.id, null);
        System.out.println("Files = " + info);
    }

    private void download() throws CatalogException {
        logger.debug("Downloading file");
        //TODO
    }

    private void grep() throws CatalogException {
        logger.debug("Grep command: File content");
        //TODO
    }

    private void search() throws CatalogException {
        logger.debug("Searching files");
        //QueryResponse<File> listfiles = openCGAClient.getFileClient().search(querry, queryoptions);
        //TODO
    }

    private void list() throws CatalogException, IOException {
        logger.debug("Listing files in folder");
        QueryResponse<File> listfiles = openCGAClient.getFileClient().getFiles(filesCommandOptions.listCommandOptions.id, null);
        System.out.println("List files = " + listfiles);

    }

    private void index() throws CatalogException {
        logger.debug("Indexing file in the selected StorageEngine");
        //TODO
    }

    private void alignament() throws CatalogException {
        logger.debug("Fetch alignments from a BAM file");
        //TODO
    }

    private void fetch() throws CatalogException {
        logger.debug("File Fetch");
        //TODO
    }

    private void share() throws CatalogException {
        logger.debug("Sharing a file");
        //TODO
    }

    private void unshare() throws CatalogException {
        logger.debug("Unsharing a file");
        //TODO
    }

    private void update() throws CatalogException {
        logger.debug("updating file");
        //TODO
    }

    private void upload() throws CatalogException, IOException {
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

        QueryResponse<File> upload = openCGAClient.getFileClient().upload(filesCommandOptions.uploadCommandOptions.studyId,
                filesCommandOptions.uploadCommandOptions.inputFile, objectMap);

        if (!upload.getError().isEmpty()) {
            logger.error(upload.getError());
        } else {
            upload.first().getResult().stream().forEach(file -> System.out.println(file.toString()));
        }
    }

    private void delete() throws CatalogException, IOException {
        logger.debug("Deleting file");

        ObjectMap objectMap = new ObjectMap()
                .append("deleteExternal", filesCommandOptions.deleteCommandOptions.deleteExternal)
                .append("skipTrash", filesCommandOptions.deleteCommandOptions.skipTrash);

        QueryResponse<File> delete = openCGAClient.getFileClient().delete(filesCommandOptions.deleteCommandOptions.id, objectMap);

        if (!delete.getError().isEmpty()) {
            logger.error(delete.getError());
        } else {
            delete.first().getResult().stream().forEach(file -> System.out.println(file.toString()));
        }
    }

    private void link() throws CatalogException, IOException, URISyntaxException {
        logger.debug("Linking the file or folder into catalog.");

        String studyStr = filesCommandOptions.linkCommandOptions.studyId;
        URI uri = UriUtils.createUri(filesCommandOptions.linkCommandOptions.input);
        String path = filesCommandOptions.linkCommandOptions.path;
        ObjectMap objectMap = new ObjectMap()
                .append(CatalogFileDBAdaptor.QueryParams.DESCRIPTION.key(), filesCommandOptions.linkCommandOptions.description)
                .append("parents", filesCommandOptions.linkCommandOptions.parents)
                .append("sessionId", sessionId);

        logger.debug("uri: {}", uri.toString());

        QueryResponse<File> link = openCGAClient.getFileClient().link(studyStr, uri.toString(), path, objectMap);
        if (!link.getError().isEmpty()) {
            logger.error(link.getError());
        } else {
            link.first().getResult().stream().forEach(file -> System.out.println(file.toString()));
        }
    }

    private void relink() throws CatalogException {
        logger.debug("Change file location. Provided file must be either STAGED or an external file");
        //TODO
    }

    private void unlink() throws CatalogException, IOException {
        logger.debug("Unlink an external file from catalog");

        QueryResponse<File> unlink = openCGAClient.getFileClient().unlink(filesCommandOptions.unlinkCommandOptions.id, new ObjectMap());

        if (!unlink.getError().isEmpty()) {
            logger.error(unlink.getError());
        } else {
            unlink.first().getResult().stream().forEach(file -> System.out.println(file.toString()));
        }

    }

    private void refresh() throws CatalogException {
        logger.debug("Refreshing metadata from the selected file or folder. Print updated files.");
    }


    private void groupBy() throws CatalogException {
        logger.debug("Grouping files by several fields");
        //TODO

    }


}
