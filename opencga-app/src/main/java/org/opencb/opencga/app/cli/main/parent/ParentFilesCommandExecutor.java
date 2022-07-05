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
package org.opencb.opencga.app.cli.main.parent;

import org.opencb.commons.app.cli.GeneralCliOptions;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor;
import org.opencb.opencga.app.cli.main.options.FilesCommandOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.RestResponse;

public abstract class ParentFilesCommandExecutor extends OpencgaCommandExecutor {

    private final FilesCommandOptions filesCommandOptions;

    public ParentFilesCommandExecutor(GeneralCliOptions.CommonCommandOptions options,
                                      FilesCommandOptions filesCommandOptions) throws CatalogAuthenticationException {
        super(options);
        this.filesCommandOptions = filesCommandOptions;
    }

    protected RestResponse<File> upload() throws Exception {
        logger.debug("uploading file");

        FilesCommandOptions.UploadCommandOptions commandOptions = filesCommandOptions.uploadCommandOptions;

        ObjectMap params = new ObjectMap()
                .append("fileFormat", ParamUtils.defaultString(commandOptions.fileFormat, File.Format.UNKNOWN.toString()))
                .append("bioformat", ParamUtils.defaultString(commandOptions.bioformat, File.Bioformat.UNKNOWN.toString()))
                .append("parents", commandOptions.parents);

        params.putIfNotEmpty("study", commandOptions.study);
        params.putIfNotEmpty("relativeFilePath", commandOptions.catalogPath);
        params.putIfNotEmpty("description", commandOptions.description);
        params.putIfNotEmpty("fileName", commandOptions.fileName);
        params.putIfNotEmpty("file", commandOptions.inputFile);
        params.put("uploadServlet", Boolean.TRUE);

        return openCGAClient.getFileClient().upload(params);
    }
}
