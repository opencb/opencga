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


import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.FileCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.File;

import java.io.IOException;

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
            case "create":
                create();
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
            case "share":
                share();
                break;
            case "unshare":
                unshare();
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

    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new file");
        //openCGAClient.getFileClient(). /******************* Falta el create en FileClient.java ?? **//
        //TODO
    }
    private void createFolder() throws CatalogException {
        logger.debug("Creating a new folder");
        //TODO

    }



    private void info() throws CatalogException, IOException  {
        logger.debug("Getting file information");
        QueryResponse<File> info = openCGAClient.getFileClient().get(filesCommandOptions.infoCommandOptions.id,null);
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

    private void list() throws CatalogException, IOException  {
        logger.debug("Listing files in folder");
        QueryResponse<File> listfiles = openCGAClient.getFileClient().getFiles(filesCommandOptions.listCommandOptions.id,null);
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

    private void upload() throws CatalogException {
        logger.debug("uploading file");
        //TODO
    }

    private void delete() throws CatalogException {
        logger.debug("Deleting file");
        //TODO
    }

    private void link() throws CatalogException, IOException {
        logger.debug("Linking an external file into catalog.");
        String studyId = filesCommandOptions.linkCommandOptions.studyId;
        String uri = filesCommandOptions.linkCommandOptions.uri;
        String path = filesCommandOptions.createCommandOptions.path;
        openCGAClient.getFileClient().link(studyId, uri, path, null);
    }

    private void relink() throws CatalogException {
        logger.debug("Change file location. Provided file must be either STAGED or an external file");
        //TODO
    }

    private void unlink() throws CatalogException {
        logger.debug("Unlink an external file from catalog");
        //TODO

    }

    private void refresh() throws CatalogException {
        logger.debug("Refreshing metadata from the selected file or folder. Print updated files.");
    }



    private void groupBy() throws CatalogException {
        logger.debug("Grouping files by several fields");
        //TODO

    }


}
