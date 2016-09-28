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

import org.codehaus.jackson.map.ObjectMapper;
import org.opencb.commons.datastore.core.QueryResponse;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

/**
 * Created by pfurio on 28/07/16.
 */
public class JsonWriter implements IWriter {

    @Override
    public void print(QueryResponse queryResponse, boolean beauty) {
        if (!checkErrors(queryResponse)) {
            generalPrint(queryResponse, beauty);
        }
    }

    @Override
    public void writeToFile(QueryResponse queryResponse, Path filePath, boolean beauty) {
        if (checkErrors(queryResponse)) {
            return;
        }

        // Redirect the output
        try {
            FileOutputStream fos = new FileOutputStream(filePath.toFile());
            PrintStream ps = new PrintStream(fos);
            System.setOut(ps);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        generalPrint(queryResponse, beauty);
    }

    /**
     * Print errors or warnings and return true if any error was found.
     *
     * @param queryResponse queryResponse object
     * @return true if the query gave an error.
     */
    private boolean checkErrors(QueryResponse queryResponse) {
        if (!queryResponse.getError().isEmpty()) {
            System.out.println(queryResponse.getError());
            return true;
        }

        // Print warnings
        if (!queryResponse.getWarning().isEmpty()) {
            System.out.println(queryResponse.getWarning());
        }

        return false;
    }

    private void generalPrint(QueryResponse queryResponse, boolean beauty) {
        if (beauty) {
            try {
                System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().
                        writeValueAsString(queryResponse.getResponse()));
            } catch (IOException e) {
                System.out.println("Error parsing the queryResponse to print as a beautiful JSON");
            }
        } else {
            System.out.println(queryResponse.getResponse());
        }
    }
}
