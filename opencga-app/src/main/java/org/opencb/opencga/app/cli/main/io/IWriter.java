/*
 * Copyright 2015-2016 OpenCB
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

package org.opencb.opencga.app.cli.main.io;

import org.opencb.commons.datastore.core.QueryResponse;

import java.nio.file.Path;

/**
 * Created by pfurio on 28/07/16.
 */
public interface IWriter {

    /**
     * Prints the queryResponse to stdout.
     *
     * @param queryResponse queryResponse object to be printed.
     * @param beauty boolean indicating whether to print with a beauty format.
     */
    void print(QueryResponse queryResponse, boolean beauty);

    /**
     * Writes the output from queryResponse to the file.
     *
     * @param queryResponse queryResponse object to be written.
     * @param filePath file where the output will be written.
     * @param beauty boolean indicating whether to print with a beauty format.
     */
    void writeToFile(QueryResponse queryResponse, Path filePath, boolean beauty);

}
