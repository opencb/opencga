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
package org.opencb.opencga.app.cli.main.custom;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.core.config.client.ClientConfiguration;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;

public class CustomFilesCommandExecutor extends CustomCommandExecutor {

    public CustomFilesCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                      SessionManager session, String appHome, Logger logger) {
        super(options, token, clientConfiguration, session, appHome, logger);
    }

    public CustomFilesCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                      SessionManager session, String appHome, Logger logger, OpenCGAClient openCGAClient) {
        super(options, token, clientConfiguration, session, appHome, logger, openCGAClient);
    }

    public RestResponse<File> upload(CustomFilesCommandOptions.UploadCommandOptions commandOptions) throws Exception {
//        ObjectMap params = new ObjectMap()
        options.append("fileFormat", options.getString("fileFormat", File.Format.UNKNOWN.toString()))
                .append("bioformat", options.getString("bioformat", File.Bioformat.UNKNOWN.toString()));
//        //If the DEPRECATED parameter fileFormat has set we only override it if the new parameter format is also set
//        params.append("fileFormat", ParamUtils.defaultString(commandOptions.format, params.getString("fileFormat")));
//        params.putIfNotEmpty("study", commandOptions.study);
//        params.putIfNotEmpty("relativeFilePath", commandOptions.catalogPath);
//        params.putIfNotEmpty("relativeFilePath", commandOptions.path);
//        params.putIfNotEmpty("description", commandOptions.description);
//        params.putIfNotEmpty("fileName", commandOptions.fileName);
//        params.putIfNotEmpty("fileName", commandOptions.name);
//        params.putIfNotEmpty("file", commandOptions.inputFile);
        options.put("uploadServlet", Boolean.FALSE);
        return openCGAClient.getFileClient().upload(options);
    }

}
