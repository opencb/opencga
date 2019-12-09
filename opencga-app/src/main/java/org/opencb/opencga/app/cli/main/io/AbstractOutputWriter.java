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

package org.opencb.opencga.app.cli.main.io;

import org.opencb.commons.datastore.core.Event;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.rest.RestResponse;

import java.io.PrintStream;

/**
 * Created by pfurio on 28/07/16.
 */
public abstract class AbstractOutputWriter {

    protected WriterConfiguration writerConfiguration;
    protected PrintStream ps;

    protected static final String ANSI_RESET = "\033[0m";
    protected static final String ANSI_RED = "\033[31m";
    protected static final String ANSI_YELLOW = "\033[33m";

    public AbstractOutputWriter() {
        this(new WriterConfiguration(), System.out);
    }

    public AbstractOutputWriter(WriterConfiguration writerConfiguration) {
        this(writerConfiguration, System.out);
    }

    public AbstractOutputWriter(WriterConfiguration writerConfiguration, PrintStream ps) {
        this.writerConfiguration = writerConfiguration;
        this.ps = ps;
    }

    abstract public void print(RestResponse dataResponse);

    /**
     * Print errors or warnings and return true if any error was found.
     *
     * @param dataResponse dataResponse object
     * @return true if the query gave an error.
     */
    protected <T> boolean checkErrors(RestResponse<T> dataResponse) {
        // Print warnings
        if (ListUtils.isNotEmpty(dataResponse.getEvents())) {
            for (Event event : dataResponse.getEvents()) {
                if (event.getType() == Event.Type.WARNING) {
                    System.err.println(ANSI_YELLOW + "WARNING: " + event.getCode() + ": " + event.getMessage() + ANSI_RESET);
                }
            }
        }

        boolean errors = false;
        if (ListUtils.isNotEmpty(dataResponse.getEvents())) {
            for (Event event : dataResponse.getEvents()) {
                if (event.getType() == Event.Type.ERROR) {
                    System.err.println(ANSI_RED + "ERROR " + event.getCode() + ": " + event.getMessage() + ANSI_RESET);
                    errors = true;
                }
            }
        }

        return errors;
    }

}
