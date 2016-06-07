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

package org.opencb.opencga.app.cli.main;


import org.opencb.opencga.catalog.exceptions.CatalogException;

import java.io.IOException;

/**
 * Created by imedina on 03/06/16.
 */
public class FilesCommandExecutor extends OpencgaCommandExecutor {

    private OpencgaCliOptionsParser.UsersCommandOptions filesCommandOptions;

    public FilesCommandExecutor(OpencgaCliOptionsParser.UsersCommandOptions filesCommandOptions) {
        super(filesCommandOptions.commonOptions);
        this.filesCommandOptions = filesCommandOptions;
    }



    @Override
    public void execute() throws Exception {
        logger.debug("Executing files command line");

        String subCommandString = filesCommandOptions.getParsedSubCommand();
        switch (subCommandString) {
            case "create":
                create();
                break;
            case "create-folder":
                createFolder();
                break;
            case "upload":
                upload();
                break;
            case "info":
                info();
                break;
            case "list":
                list();
                break;
            case "link":
                link();
                break;
            case "relink":
                relink();
                break;
            case "refresh":
                refresh();
                break;
            case "search":
                search();
                break;
            case "index":
                index();
                break;
            default:
                logger.error("Subcommand not valid");
                break;
        }

    }

    private void create() throws CatalogException, IOException {
        logger.debug("Creating a new file");
    }
    private void createFolder() throws CatalogException {
        logger.debug("Creating a new folder");
    }

    private void upload() throws CatalogException {
        logger.debug("Attaching a physical file to a catalog entry file");
    }

    private void info() throws CatalogException {
        logger.debug("Getting file information");
    }

    private void list() throws CatalogException {
        logger.debug("Listing files in folder");
    }
    private void link() throws CatalogException {
        logger.debug("Linking an external file into catalog.r");
    }

    private void relink() throws CatalogException {
        logger.debug("Change file location. Provided file must be either STAGED or an external file");
    }

    private void refresh() throws CatalogException {
        logger.debug("Refreshing metadata from the selected file or folder. Print updated files.");
    }

    private void search() throws CatalogException {
        logger.debug("Searching files");
    }

    private void index() throws CatalogException {
        logger.debug("Indexing file in the selected StorageEngine");
    }


}
