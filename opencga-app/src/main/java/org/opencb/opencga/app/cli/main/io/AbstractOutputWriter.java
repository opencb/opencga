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
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.parent.ParentUsersCommandExecutor;
import org.opencb.opencga.core.response.RestResponse;

import java.io.PrintStream;

/**
 * Created by pfurio on 28/07/16.
 */
public abstract class AbstractOutputWriter {

    protected WriterConfiguration writerConfiguration;
    protected PrintStream ps;

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
                    PrintUtils.printWarn(event.getCode() + ": " + event.getMessage());
                }
            }
        }

        boolean errors = false;
        if (ListUtils.isNotEmpty(dataResponse.getEvents())) {
            for (Event event : dataResponse.getEvents()) {
                if (event.getType() == Event.Type.ERROR) {
                    CommandLineUtils.printError(event.getMessage(), new Exception());
                    errors = true;
                }
            }
        }

        return errors;
    }

    /**
     * Print login message.
     *
     * @param dataResponse dataResponse object
     * @return true if the query gave an error.
     */
    protected <T> boolean checkLogin(RestResponse<T> dataResponse) {
        // Print warnings
        boolean res = false;
        if (ListUtils.isNotEmpty(dataResponse.getEvents())) {
            for (Event event : dataResponse.getEvents()) {
                if (event.getType() == Event.Type.INFO && event.getMessage().equals(ParentUsersCommandExecutor.LOGIN_OK)) {
                    PrintUtils.printInfo(event.getMessage());
                    res = true;
                }
            }
        }

        return res;
    }
}
