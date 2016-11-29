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

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.QueryResponse;

import java.io.PrintStream;

/**
 * Created by pfurio on 28/07/16.
 */
public abstract class AbstractWriter {

    private WriterConfiguration writerConfiguration;
    private PrintStream ps;

    protected static final String ANSI_RESET = "\033[0m";
    protected static final String ANSI_RED = "\033[31m";
    protected static final String ANSI_YELLOW = "\033[33m";

    public AbstractWriter() {
        this(new WriterConfiguration(), System.out);
    }

    public AbstractWriter(WriterConfiguration writerConfiguration) {
        this(writerConfiguration, System.out);
    }

    public AbstractWriter(WriterConfiguration writerConfiguration, PrintStream ps) {
        this.writerConfiguration = writerConfiguration;
        this.ps = ps;
    }

    public void print(QueryResponse queryResponse) {
        print(queryResponse, writerConfiguration, ps);
    }

    public void print(QueryResponse queryResponse, WriterConfiguration writerConfiguration) {
        print(queryResponse, writerConfiguration, ps);
    }

    /**
     * Prints the queryResponse to stdout.
     * @param queryResponse queryResponse object to be printed.
     * @param writerConfiguration basic configuration containing the different parameters accepted to print.
     *
     */
    abstract public void print(QueryResponse queryResponse, WriterConfiguration writerConfiguration, PrintStream ps);

    /**
     * Print errors or warnings and return true if any error was found.
     *
     * @param queryResponse queryResponse object
     * @return true if the query gave an error.
     */
    protected boolean checkErrors(QueryResponse queryResponse) {
        // Print warnings
        if (StringUtils.isNotEmpty(queryResponse.getWarning())) {
            System.err.println(ANSI_YELLOW + "WARNING: " + queryResponse.getWarning() + ANSI_RESET);
        }

        boolean errors = false;
        if (StringUtils.isNotEmpty(queryResponse.getError())) {
            System.err.println(ANSI_RED + "ERROR: " + queryResponse.getError() + ANSI_RESET);
            errors = true;
        }

        return errors;
    }

}
